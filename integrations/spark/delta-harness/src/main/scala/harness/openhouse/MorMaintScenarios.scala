package harness

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.iceberg.exceptions.BadRequestException
import org.apache.iceberg.exceptions.ValidationException
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

trait MorMaintScenarios extends ScenarioKit {
  import Rows._

  // ── MoR delete-file coexistence battery (BUILD-STATUS task #5, the NON-vacuous core) ─────────
  // The appraisal's "core DML → L×M=12" is ~90% vacuous: a read/insert on a DELETE-FREE MoR table
  // is byte-identical to CoW (no delete files to apply; append is mode-independent). The mutation
  // ops ARE crossed with MoR already (the `mor` bucket, 264). The genuinely-new MoR surface is
  // operating on a table that ALREADY carries a live position-delete file — data-file/delete-file
  // COEXISTENCE. `createAndSeedMorDeleted` leaves 2 rows (keys 2,3) with a live delete for key 1;
  // these ops then act on that state.
  val morCoexistCases: List[Plan.Case] =
    morVerifyLayouts
      .map(layout =>
        TablePreparation(
          layout.label,
          createAndSeedMorDeleted(layout, 3)))
      .flatMap { preparation =>
        List(
          preparation.test("coexist.append") { table =>
            table.spark.sql(
              s"INSERT INTO ${table.name} VALUES " +
                "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")

            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0) == 3,
              "append over a live delete file returned the wrong row count")
            assert(
              table.spark
                .sql(
                  s"SELECT count(*) FROM ${table.name} " +
                    s"WHERE ${Core.long0.columnName} = 1")
                .collect()(0)
                .getLong(0) == 0,
              "append resurrected the deleted row")
          },
          preparation.test("coexist.secondDelete") { table =>
            table.spark.sql(
              s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 2")

            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0) == 1,
              "second delete returned the wrong row count")
            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}.all_delete_files")
                .collect()(0)
                .getLong(0) >= 1,
              "delete files are missing after the second delete")
          },
          preparation.test("coexist.update") { table =>
            table.spark.sql(
              s"UPDATE ${table.name} " +
                s"SET ${Core.string0.columnName} = 'cx' " +
                s"WHERE ${Core.long0.columnName} = 3")

            assert(
              table.spark
                .sql(
                  s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
                    s"WHERE ${Core.long0.columnName} = 3")
                .collect()(0)
                .getString(0) == "cx",
              "update over a live delete file failed")
            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0) == 2,
              "update over a live delete file changed the row count")
          },
          preparation.test("coexist.readFilter") { table =>
            val keys = table.spark
              .sql(
                s"SELECT ${Core.long0.columnName} FROM ${table.name} " +
                  s"WHERE ${Core.long0.columnName} <= 2 " +
                  s"ORDER BY ${Core.long0.columnName}")
              .collect()
              .toSeq
              .map(_.getLong(0))

            assert(
              keys == Seq(2L),
              s"filter did not apply the position delete: $keys")
          },
          preparation.test("coexist.compactDeletes") { table =>
            table.spark.sql(
              "CALL openhouse.system.rewrite_position_delete_files(" +
                s"table => '${catalogRelative(table.name)}', " +
                "options => map('rewrite-all', 'true'))")

            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0) == 2,
              "position-delete compaction changed the row set")
          },
          preparation.test("coexist.merge") { table =>
            table.spark.sql(
              s"MERGE INTO ${table.name} target " +
                "USING (SELECT CAST(3 AS BIGINT) key) source " +
                s"ON target.${Core.long0.columnName} = source.key " +
                "WHEN MATCHED THEN UPDATE " +
                s"SET ${Core.string0.columnName} = 'mg'")

            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0) == 2,
              "merge over a live delete file changed the row count")
            assert(
              table.spark
                .sql(
                  s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
                    s"WHERE ${Core.long0.columnName} = 3")
                .collect()(0)
                .getString(0) == "mg",
              "merge over a live delete file failed")
          })
      }

  // ── Maintenance × MoR-with-live-delete (BUILD-STATUS block 8 deepening) ──────────────────────
  // The maintenance.* block runs on plain CoW; the genuinely-distinct surface is maintenance over a
  // table that carries a LIVE position-delete file. `createAndSeedMorDeleted` leaves keys 2,3 live
  // with a live delete for key 1. The hunt: does each maintenance procedure handle the delete file
  // correctly (fold / preserve / not resurrect the deleted row)?

  // rewrite_data_files over a live position delete: it applies the delete to the rewritten data
  // (key 1 physically gone, row set correct) — but it does NOT remove the now-dangling position
  // delete from the CURRENT snapshot. FINDING G14 (characterization): the compacted table still
  // carries a live delete-file reference that points at data already removed; it lingers until
  // rewrite_position_delete_files or expire_snapshots. Reads stay correct throughout. Crossed × 3 MoR
  // formats to confirm the behavior is format-consistent (the delete decode differs per format).
  val maintenanceMorFoldCases: List[Plan.Case] =
    morVerifyLayouts
      .map(layout =>
        TablePreparation(
          layout.label,
          createAndSeedMorDeleted(layout, 3)))
      .flatMap { preparation =>
        List(
          preparation.test("maint.mor.rewriteDataFilesDanglingDelete") { table =>
            table.spark.sql(
              "CALL openhouse.system.rewrite_data_files(" +
                s"table => '${catalogRelative(table.name)}', " +
                "options => map('rewrite-all', 'true'))")

            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0) == 2,
              "rewrite_data_files changed the live row set")
            assert(
              table.spark
                .sql(
                  s"SELECT count(*) FROM ${table.name} " +
                    s"WHERE ${Core.long0.columnName} = 1")
                .collect()(0)
                .getLong(0) == 0,
              "rewrite_data_files resurrected the deleted row")

            val deleteFileCount = table.spark
              .sql(s"SELECT count(*) FROM ${table.name}.delete_files")
              .collect()(0)
              .getLong(0)
            val keys = table.spark
              .sql(
                s"SELECT ${Core.long0.columnName} FROM ${table.name} " +
                  s"WHERE ${Core.long0.columnName} <= 2 " +
                  s"ORDER BY ${Core.long0.columnName}")
              .collect()
              .toSeq
              .map(_.getLong(0))

            assert(
              deleteFileCount == 1,
              "rewrite_data_files should leave one dangling position delete, " +
                s"got $deleteFileCount")
            assert(
              keys == Seq(2L),
              s"read after rewrite_data_files returned incorrect keys: $keys")
          },
          preparation.test("maint.mor.rewritePositionDeleteFolds") { table =>
            table.spark.sql(
              "CALL openhouse.system.rewrite_data_files(" +
                s"table => '${catalogRelative(table.name)}', " +
                "options => map('rewrite-all', 'true'))")
            val deleteFilesBefore = table.spark
              .sql(s"SELECT count(*) FROM ${table.name}.delete_files")
              .collect()(0)
              .getLong(0)

            table.spark.sql(
              "CALL openhouse.system.rewrite_position_delete_files(" +
                s"table => '${catalogRelative(table.name)}', " +
                "options => map('rewrite-all', 'true'))")
            val deleteFilesAfter = table.spark
              .sql(s"SELECT count(*) FROM ${table.name}.delete_files")
              .collect()(0)
              .getLong(0)

            println(
              "DIAG maint.mor.rewritePositionDeleteFolds: " +
                s"delete_files before=$deleteFilesBefore after=$deleteFilesAfter")
            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0) == 2,
              "rewrite_position_delete_files changed the live row set")
            assert(
              table.spark
                .sql(
                  s"SELECT count(*) FROM ${table.name} " +
                    s"WHERE ${Core.long0.columnName} = 1")
                .collect()(0)
                .getLong(0) == 0,
              "rewrite_position_delete_files resurrected the deleted row")
            assert(
              deleteFilesBefore == 1 && deleteFilesAfter == 0,
              "rewrite_position_delete_files should fold the dangling delete: " +
                s"before=$deleteFilesBefore after=$deleteFilesAfter")
          })
      }

  // Metadata-only maintenance over a live delete — format is vacuous (these never decode the delete
  // file), so × 1 MoR layout. Each must PRESERVE the delete (2 live rows, key 1 still gone).
  val maintenanceMorMetaCases: List[Plan.Case] =
    morVerifyLayouts
      .filter(layout =>
        layout.label == "mor-verify/parquet" ||
          layout.label == "mor-verify/orc")
      .map(layout =>
        TablePreparation(
          layout.label,
          createAndSeedMorDeleted(layout, 3)))
      .flatMap { preparation =>
        List(
          preparation.test("maint.mor.expireSnapshots") { table =>
            table.spark.sql(
              "CALL openhouse.system.expire_snapshots(" +
                s"table => '${catalogRelative(table.name)}', " +
                "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
                "retain_last => 1)")

            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0) == 2,
              "expire_snapshots changed the live row set")
            assert(
              table.spark
                .sql(
                  s"SELECT count(*) FROM ${table.name} " +
                    s"WHERE ${Core.long0.columnName} = 1")
                .collect()(0)
                .getLong(0) == 0,
              "expire_snapshots resurrected the deleted row")
          },
          preparation.test("maint.mor.rewriteManifests") { table =>
            table.spark.sql(
              "CALL openhouse.system.rewrite_manifests(" +
                s"table => '${catalogRelative(table.name)}', " +
                "use_caching => false)")

            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0) == 2,
              "rewrite_manifests changed the live row set")
          },
          preparation.test("maint.mor.removeOrphanFiles") { table =>
            table.spark.sql(
              "CALL openhouse.system.remove_orphan_files(" +
                s"table => '${catalogRelative(table.name)}', " +
                "older_than => TIMESTAMP '2020-01-01 00:00:00')")

            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0) == 2,
              "remove_orphan_files changed the live row set")
          },
          preparation.test("maint.mor.compactThenExpire") { table =>
            table.spark.sql(
              "CALL openhouse.system.rewrite_position_delete_files(" +
                s"table => '${catalogRelative(table.name)}', " +
                "options => map('rewrite-all', 'true'))")
            table.spark.sql(
              "CALL openhouse.system.expire_snapshots(" +
                s"table => '${catalogRelative(table.name)}', " +
                "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
                "retain_last => 1)")

            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0) == 2,
              "compact-then-expire changed the live row set")
            assert(
              table.spark
                .sql(
                  s"SELECT count(*) FROM ${table.name} " +
                    s"WHERE ${Core.long0.columnName} = 1")
                .collect()(0)
                .getLong(0) == 0,
              "compact-then-expire resurrected the deleted row")
          })
      }

  // ── MoR delete-file modality hazards (BUILD-STATUS block 10 deepening) ───────────────────────
  // A live position delete is snapshot-scoped state. These hunt for it being mis-resolved across the
  // history/restore axes: a delete must NOT be retroactive (pre-delete snapshots still see the row),
  // rollback must UNDO it, and it must SURVIVE expiration of older snapshots. Time-travel/rollback
  // logic is format-vacuous (it resolves snapshots, not file bytes) → × 1 MoR layout.
  val morHazardCases: List[Plan.Case] =
    morVerifyLayouts
      .filter(layout =>
        layout.label == "mor-verify/parquet" ||
          layout.label == "mor-verify/orc")
      .map(layout =>
        TablePreparation(
          layout.label,
          createAndSeedMorDeleted(layout, 3)))
      .flatMap { preparation =>
        List(
          preparation.test("hazard.mor.timeTravelBeforeDelete") { table =>
            val seedSnapshotId = table.spark
              .sql(
                s"SELECT snapshot_id FROM ${table.name}.snapshots " +
                  "ORDER BY committed_at LIMIT 1")
              .collect()(0)
              .getLong(0)

            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0) == 2,
              "current merge-on-read state should apply the delete")
            assert(
              table.spark
                .sql(
                  s"SELECT count(*) FROM ${table.name} " +
                    s"VERSION AS OF $seedSnapshotId")
                .collect()(0)
                .getLong(0) == 3,
              "the snapshot before the delete should still contain the row")
          },
          preparation.test("hazard.mor.rollbackUndoesDelete") { table =>
            val seedSnapshotId = table.spark
              .sql(
                s"SELECT snapshot_id FROM ${table.name}.snapshots " +
                  "ORDER BY committed_at LIMIT 1")
              .collect()(0)
              .getLong(0)

            table.spark.sql(
              "CALL openhouse.system.rollback_to_snapshot(" +
                s"table => '${catalogRelative(table.name)}', " +
                s"snapshot_id => ${seedSnapshotId}L)")

            assert(
              table.spark
                .sql(s"SELECT count(*) FROM ${table.name}")
                .collect()(0)
                .getLong(0) == 3,
              "rollback did not undo the merge-on-read delete")
            assert(
              table.spark
                .sql(
                  s"SELECT count(*) FROM ${table.name} " +
                    s"WHERE ${Core.long0.columnName} = 1")
                .collect()(0)
                .getLong(0) == 1,
              "rollback did not restore the deleted row")
          },
          preparation.test("hazard.mor.expireThenDeleteHolds") { table =>
            table.spark.sql(
              "CALL openhouse.system.expire_snapshots(" +
                s"table => '${catalogRelative(table.name)}', " +
                "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
                "retain_last => 1)")

            val keys = table.spark
              .sql(
                s"SELECT ${Core.long0.columnName} FROM ${table.name} " +
                  s"WHERE ${Core.long0.columnName} <= 2 " +
                  s"ORDER BY ${Core.long0.columnName}")
              .collect()
              .toSeq
              .map(_.getLong(0))

            assert(
              keys == Seq(2L),
              s"delete did not survive snapshot expiration: $keys")
          })
      }

  // ── MoR × branch MERGE (position deletes carried across fast_forward / cherry_pick / REPLACE BRANCH) ──
  // A DELETE/UPDATE on a branch of a MoR table writes position-delete files ON THE BRANCH; merging the
  // branch back to main must carry those deletes correctly. This is the known-fragile neighborhood of
  // G11 (branch × merge) and the "cherry-pick rejects row-delete snapshots" note — the merge is where
  // MoR-branch breakage hides. Base is a single-file MoR seed (COALESCE(1)) so a strict-subset DELETE
  // is a real position delete, not a file elimination. Merge is a ref/snapshot carry → format-vacuous
  // (× 1 MoR layout). Each hunts for: deletes lost/not-carried, deleted rows resurrecting on main,
  // cherry-pick rejecting row-delete snapshots.
  val morBranchMergeCases: List[Plan.Case] =
    morVerifyLayouts
      .filter(layout =>
        layout.label == "mor-verify/parquet" ||
          layout.label == "mor-verify/orc")
      .map(layout =>
        TablePreparation(
          layout.label,
          createAndSeedSingleFile(layout, 3)))
      .flatMap { preparation =>
        List(
          preparation.test("mbranch.fastForwardDelete") { table =>
            table.spark.sql(
              s"ALTER TABLE ${table.name} CREATE BRANCH mfb")
            table.spark.sql(
              s"DELETE FROM ${table.name}.branch_mfb " +
                s"WHERE ${Core.long0.columnName} = 1")

            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name}") == "3",
              "main advanced before fast-forward")
            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name} VERSION AS OF 'mfb'") == "2",
              "branch delete was not applied")

            table.spark.sql(
              "CALL openhouse.system.fast_forward(" +
                s"'${catalogRelative(table.name)}', 'main', 'mfb')")

            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name}") == "2",
              "fast-forward did not carry the branch position delete")
            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name} " +
                  s"WHERE ${Core.long0.columnName} = 1") == "0",
              "deleted row reappeared after fast-forward")
          },
          preparation.test("mbranch.fastForwardUpdate") { table =>
            table.spark.sql(
              s"ALTER TABLE ${table.name} CREATE BRANCH mub")
            table.spark.sql(
              s"UPDATE ${table.name}.branch_mub " +
                s"SET ${Core.string0.columnName} = 'br-upd' " +
                s"WHERE ${Core.long0.columnName} = 2")
            table.spark.sql(
              "CALL openhouse.system.fast_forward(" +
                s"'${catalogRelative(table.name)}', 'main', 'mub')")

            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name}") == "3",
              "fast-forward of an update changed the main row count")
            assert(
              table.spark
                .sql(
                  s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
                    s"WHERE ${Core.long0.columnName} = 2")
                .collect()(0)
                .getString(0) == "br-upd",
              "fast-forward did not carry the branch update")
          },
          preparation.test("mbranch.cherrypickDelete") { table =>
            table.spark.sql(
              s"ALTER TABLE ${table.name} CREATE BRANCH mcb")
            table.spark.sql(
              s"DELETE FROM ${table.name}.branch_mcb " +
                s"WHERE ${Core.long0.columnName} = 1")
            val deleteSnapshotId = table.spark
              .sql(
                s"SELECT snapshot_id FROM ${table.name}.snapshots " +
                  "ORDER BY committed_at DESC LIMIT 1")
              .collect()(0)
              .getLong(0)
            val outcome =
              try {
                table.spark.sql(
                  "CALL openhouse.system.cherrypick_snapshot(" +
                    s"'${catalogRelative(table.name)}', ${deleteSnapshotId}L)")
                "ok"
              } catch {
                case NonFatal(exception) =>
                  s"rejected:${Exceptions.root(exception).getClass.getSimpleName}"
              }
            val mainCount = countOf(
              table.spark,
              s"SELECT count(*) FROM ${table.name}")

            println(
              s"DIAG mbranch.cherrypickDelete: $outcome, mainCount=$mainCount")
            if (outcome == "ok") {
              assert(
                mainCount == "2",
                "cherry-pick reported success without applying the branch delete")
            } else {
              assert(
                mainCount == "3",
                "cherry-pick was rejected after changing main")
            }
          },
          preparation.test("mbranch.replaceBranchDelete") { table =>
            val seedSnapshotId = table.spark
              .sql(
                s"SELECT snapshot_id FROM ${table.name}.snapshots " +
                  "ORDER BY committed_at DESC LIMIT 1")
              .collect()(0)
              .getLong(0)

            table.spark.sql(
              s"ALTER TABLE ${table.name} CREATE BRANCH mrb")
            table.spark.sql(
              s"DELETE FROM ${table.name}.branch_mrb " +
                s"WHERE ${Core.long0.columnName} = 1")

            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name} VERSION AS OF 'mrb'") == "2",
              "branch delete was not applied")

            table.spark.sql(
              s"ALTER TABLE ${table.name} REPLACE BRANCH mrb " +
                s"AS OF VERSION $seedSnapshotId")

            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name} VERSION AS OF 'mrb'") == "3",
              "replacing the branch target did not undo its position delete")
          })
      }

  // Encryption capability PIN (characterization). OpenHouse delegates table-data encryption to an
  // external KMS plugin (private repo); in OSS the catalog never wires a KeyManagementClient, so
  // customer tables use the default PlaintextEncryptionManager and data is written UNENCRYPTED.
  // Discriminator: a Parquet file's FOOTER magic is "PAR1" when unencrypted and "PARE" under modular
  // encryption — robust regardless of compression. This pins that OSS writes plaintext; it FLIPS to
  // "PARE" the moment table-data encryption is wired (then update BUGS.md and this pin). An off-the-
  // shelf KMS does NOT change this — nothing in the OpenHouse write path invokes the encryption hook.
  val encryptionPinCases: List[Plan.Case] = {
    val preparation = TablePreparation(
      "parquet",
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            "TBLPROPERTIES ('write.format.default'='parquet')")()
        .insert(3)())

    List(
      preparation.test("surface.pin.dataPlaintext") { table =>
        val dataFilePath = table.spark
          .sql(s"SELECT file_path FROM ${table.name}.data_files LIMIT 1")
          .collect()(0)
          .getString(0)
          .stripPrefix("file:")
        val bytes = java.nio.file.Files.readAllBytes(
          java.nio.file.Paths.get(dataFilePath))

        assert(
          bytes.length >= 8,
          s"data file is too small to inspect: ${bytes.length} bytes")
        val footerMagic = new String(bytes.takeRight(4), "US-ASCII")
        assert(
          footerMagic == "PAR1",
          s"expected plaintext Parquet footer PAR1, got $footerMagic")
      })
  }


}
