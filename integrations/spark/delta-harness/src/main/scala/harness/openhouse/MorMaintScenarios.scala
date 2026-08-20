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
  val morCoexistOps: List[(String, TableTest[CoreTable.type])] = List(
    // A new data file must coexist with the existing delete file; the read applies the delete to
    // OLD data only, not the appended rows.
    "coexist.append" -> TableTest(Core).step("coexist.append") { (spark, table) =>
      spark.sql(s"INSERT INTO $table VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "append over live delete file wrong count")
      assert(spark.sql(s"SELECT count(*) FROM $table WHERE ${Core.long0.columnName} = 1").collect()(0).getLong(0) == 0, "deleted row resurrected by append")
    }(),
    // A second delete adds a second position-delete file over the same data file.
    "coexist.secondDelete" -> TableTest(Core).step("coexist.secondDelete") { (spark, table) =>
      spark.sql(s"DELETE FROM $table WHERE ${Core.long0.columnName} = 2")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 1, "second delete over existing delete file wrong count")
      assert(spark.sql(s"SELECT count(*) FROM $table.all_delete_files").collect()(0).getLong(0) >= 1, "delete files missing after second delete")
    }(),
    // Update a surviving row while a delete file is live.
    "coexist.update" -> TableTest(Core).step("coexist.update") { (spark, table) =>
      spark.sql(s"UPDATE $table SET ${Core.string0.columnName} = 'cx' WHERE ${Core.long0.columnName} = 3")
      assert(spark.sql(s"SELECT ${Core.string0.columnName} FROM $table WHERE ${Core.long0.columnName} = 3").collect()(0).getString(0) == "cx", "update over live delete failed")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2, "update over live delete changed count")
    }(),
    // A filtered read must apply the position delete (the deleted key must never appear).
    "coexist.readFilter" -> TableTest(Core).step("coexist.readFilter") { (spark, table) =>
      val keys = spark.sql(s"SELECT ${Core.long0.columnName} FROM $table WHERE ${Core.long0.columnName} <= 2 ORDER BY ${Core.long0.columnName}").collect().toSeq.map(_.getLong(0))
      assert(keys == Seq(2L), s"filter must apply the position delete (key 1 gone): $keys")
    }(),
    // Compacting the position deletes materializes them; the row set is unchanged.
    "coexist.compactDeletes" -> TableTest(Core).step("coexist.compactDeletes") { (spark, table) =>
      spark.sql(s"CALL openhouse.system.rewrite_position_delete_files(table => '${catalogRelative(table)}', options => map('rewrite-all', 'true'))")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2, "compact position deletes changed row set")
    }(),
    // Merge onto a table with a live delete file.
    "coexist.merge" -> TableTest(Core).step("coexist.merge") { (spark, table) =>
      spark.sql(s"MERGE INTO $table t USING (SELECT CAST(3 AS BIGINT) k) s ON t.${Core.long0.columnName} = s.k " +
        s"WHEN MATCHED THEN UPDATE SET ${Core.string0.columnName} = 'mg'")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2, "merge over live delete changed count")
      assert(spark.sql(s"SELECT ${Core.string0.columnName} FROM $table WHERE ${Core.long0.columnName} = 3").collect()(0).getString(0) == "mg", "merge over live delete failed")
    }()
  )

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
  val maintenanceMorFoldOps: List[(String, TableTest[CoreTable.type])] = List(
    "maint.mor.rewriteDataFilesDanglingDelete" -> TableTest(Core).step("maint.mor.rewriteDataFilesDanglingDelete") { (spark, table) =>
      spark.sql(s"CALL openhouse.system.rewrite_data_files(table => '${catalogRelative(table)}', options => map('rewrite-all', 'true'))")
      // the delete IS applied logically — row set is correct
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2, "rewrite_data_files changed the live row set over a MoR delete")
      assert(spark.sql(s"SELECT count(*) FROM $table WHERE ${Core.long0.columnName} = 1").collect()(0).getLong(0) == 0, "rewrite_data_files RESURRECTED the deleted row")
      // G14 PIN: the position delete is NOT removed from the current snapshot — it dangles.
      val delFiles = spark.sql(s"SELECT count(*) FROM $table.delete_files").collect()(0).getLong(0)
      assert(delFiles == 1, s"characterized: rewrite_data_files leaves the position delete dangling in the current snapshot (expected 1), got $delFiles — if this is 0, the build now folds deletes and the pin should flip")
      // despite the dangling delete, reads remain correct (the removed row never reappears)
      val keys = spark.sql(s"SELECT ${Core.long0.columnName} FROM $table WHERE ${Core.long0.columnName} <= 2 ORDER BY ${Core.long0.columnName}").collect().toSeq.map(_.getLong(0))
      assert(keys == Seq(2L), s"read after rewrite_data_files must stay correct despite the dangling delete: $keys")
    }(),
    // D5 DECIDER (owner: G14 is a BUG unless the recovery path works, then a PIN): does
    // `rewrite_position_delete_files` actually FOLD OUT the dangling position delete that
    // rewrite_data_files leaves behind (delete_files 1 -> 0)? If yes, the operator has a working
    // additional-maintenance recovery (G14 = pin); if no, the dangling delete is unrecoverable via the
    // documented procedure (G14 = bug). Reads must stay correct throughout. × 3 MoR formats.
    "maint.mor.rewritePositionDeleteFolds" -> TableTest(Core).step("maint.mor.rewritePositionDeleteFolds") { (spark, table) =>
      // 1) rewrite_data_files leaves a dangling position delete (the G14 state).
      spark.sql(s"CALL openhouse.system.rewrite_data_files(table => '${catalogRelative(table)}', options => map('rewrite-all', 'true'))")
      val danglingBefore = spark.sql(s"SELECT count(*) FROM $table.delete_files").collect()(0).getLong(0)
      // 2) the recovery path: rewrite_position_delete_files — does it fold the dangling delete out?
      spark.sql(s"CALL openhouse.system.rewrite_position_delete_files(table => '${catalogRelative(table)}', options => map('rewrite-all', 'true'))")
      val danglingAfter = spark.sql(s"SELECT count(*) FROM $table.delete_files").collect()(0).getLong(0)
      println(s"DIAG maint.mor.rewritePositionDeleteFolds: delete_files before=$danglingBefore after=$danglingAfter")
      // reads must stay correct regardless (key 1 removed, 2 live rows).
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2, "rewrite_position_delete_files changed the live row set")
      assert(spark.sql(s"SELECT count(*) FROM $table WHERE ${Core.long0.columnName} = 1").collect()(0).getLong(0) == 0, "rewrite_position_delete_files resurrected the deleted row")
      // D5 PIN: the recovery WORKS — rewrite_position_delete_files folds the dangling delete out.
      assert(danglingBefore == 1 && danglingAfter == 0,
        s"D5: expected rewrite_position_delete_files to FOLD the dangling delete (before=1 -> after=0); got before=$danglingBefore after=$danglingAfter — if after>0 the recovery path does NOT work and G14 must be reclassified from pin to BUG")
    }()
  )

  // Metadata-only maintenance over a live delete — format is vacuous (these never decode the delete
  // file), so × 1 MoR layout. Each must PRESERVE the delete (2 live rows, key 1 still gone).
  val maintenanceMorMetaOps: List[(String, TableTest[CoreTable.type])] = List(
    "maint.mor.expireSnapshots" -> TableTest(Core).step("maint.mor.expireSnapshots") { (spark, table) =>
      spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2, "expire_snapshots changed the live row set over a MoR delete")
      assert(spark.sql(s"SELECT count(*) FROM $table WHERE ${Core.long0.columnName} = 1").collect()(0).getLong(0) == 0, "expire_snapshots resurrected the deleted row")
    }(),
    "maint.mor.rewriteManifests" -> TableTest(Core).step("maint.mor.rewriteManifests") { (spark, table) =>
      spark.sql(s"CALL openhouse.system.rewrite_manifests(table => '${catalogRelative(table)}', use_caching => false)")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2, "rewrite_manifests changed the live row set over a MoR delete")
    }(),
    "maint.mor.removeOrphanFiles" -> TableTest(Core).step("maint.mor.removeOrphanFiles") { (spark, table) =>
      spark.sql(s"CALL openhouse.system.remove_orphan_files(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2020-01-01 00:00:00')")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2, "remove_orphan_files changed the live row set over a MoR delete")
    }(),
    // Modality: compact the position deletes, THEN expire the pre-compact snapshot — the folded
    // state must survive (the deleted row must not reappear via the retained/expired lineage).
    "maint.mor.compactThenExpire" -> TableTest(Core).step("maint.mor.compactThenExpire") { (spark, table) =>
      spark.sql(s"CALL openhouse.system.rewrite_position_delete_files(table => '${catalogRelative(table)}', options => map('rewrite-all', 'true'))")
      spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2, "compact-then-expire changed the live row set")
      assert(spark.sql(s"SELECT count(*) FROM $table WHERE ${Core.long0.columnName} = 1").collect()(0).getLong(0) == 0, "compact-then-expire resurrected the deleted row")
    }()
  )

  // ── MoR delete-file modality hazards (BUILD-STATUS block 10 deepening) ───────────────────────
  // A live position delete is snapshot-scoped state. These hunt for it being mis-resolved across the
  // history/restore axes: a delete must NOT be retroactive (pre-delete snapshots still see the row),
  // rollback must UNDO it, and it must SURVIVE expiration of older snapshots. Time-travel/rollback
  // logic is format-vacuous (it resolves snapshots, not file bytes) → × 1 MoR layout.
  val morHazardOps: List[(String, TableTest[CoreTable.type])] = List(
    // The delete is snapshot-scoped: time-travel to the pre-delete snapshot still sees key 1.
    "hazard.mor.timeTravelBeforeDelete" -> TableTest(Core).step("hazard.mor.timeTravelBeforeDelete") { (spark, table) =>
      val seedSnap = spark.sql(s"SELECT snapshot_id FROM $table.snapshots ORDER BY committed_at LIMIT 1").collect()(0).getLong(0)
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2, "current MoR state should have the delete applied")
      assert(spark.sql(s"SELECT count(*) FROM $table VERSION AS OF $seedSnap").collect()(0).getLong(0) == 3,
        "pre-delete snapshot must still see the deleted row (delete must not be retroactive)")
    }(),
    // Rollback to the pre-delete snapshot UNDOES the delete — the row returns and no delete is live.
    "hazard.mor.rollbackUndoesDelete" -> TableTest(Core).step("hazard.mor.rollbackUndoesDelete") { (spark, table) =>
      val seedSnap = spark.sql(s"SELECT snapshot_id FROM $table.snapshots ORDER BY committed_at LIMIT 1").collect()(0).getLong(0)
      spark.sql(s"CALL openhouse.system.rollback_to_snapshot(table => '${catalogRelative(table)}', snapshot_id => ${seedSnap}L)")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "rollback did not undo the MoR delete")
      assert(spark.sql(s"SELECT count(*) FROM $table WHERE ${Core.long0.columnName} = 1").collect()(0).getLong(0) == 1, "rolled-back row not restored")
    }(),
    // The delete must SURVIVE expiration of the older (pre-delete) snapshot — a filtered read still
    // excludes key 1 after expire.
    "hazard.mor.expireThenDeleteHolds" -> TableTest(Core).step("hazard.mor.expireThenDeleteHolds") { (spark, table) =>
      spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
      val keys = spark.sql(s"SELECT ${Core.long0.columnName} FROM $table WHERE ${Core.long0.columnName} <= 2 ORDER BY ${Core.long0.columnName}").collect().toSeq.map(_.getLong(0))
      assert(keys == Seq(2L), s"delete must survive expiration of the pre-delete snapshot (key 1 gone): $keys")
    }()
  )

  // ── MoR × branch MERGE (position deletes carried across fast_forward / cherry_pick / REPLACE BRANCH) ──
  // A DELETE/UPDATE on a branch of a MoR table writes position-delete files ON THE BRANCH; merging the
  // branch back to main must carry those deletes correctly. This is the known-fragile neighborhood of
  // G11 (branch × merge) and the "cherry-pick rejects row-delete snapshots" note — the merge is where
  // MoR-branch breakage hides. Base is a single-file MoR seed (COALESCE(1)) so a strict-subset DELETE
  // is a real position delete, not a file elimination. Merge is a ref/snapshot carry → format-vacuous
  // (× 1 MoR layout). Each hunts for: deletes lost/not-carried, deleted rows resurrecting on main,
  // cherry-pick rejecting row-delete snapshots.
  val morBranchMergeOps: List[(String, TableTest[CoreTable.type])] = List(
    // fast_forward must carry a branch position-delete into main: after merge the deleted row is gone.
    "mbranch.fastForwardDelete" -> TableTest(Core).step("mbranch.fastForwardDelete") { (spark, table) =>
      spark.sql(s"ALTER TABLE $table CREATE BRANCH mfb")
      spark.sql(s"DELETE FROM $table.branch_mfb WHERE ${Core.long0.columnName} = 1")   // position delete on branch
      assert(countOf(spark, s"SELECT count(*) FROM $table") == "3", "main advanced before merge")
      assert(countOf(spark, s"SELECT count(*) FROM $table VERSION AS OF 'mfb'") == "2", "branch delete not applied on the branch")
      spark.sql(s"CALL openhouse.system.fast_forward('${catalogRelative(table)}', 'main', 'mfb')")
      assert(countOf(spark, s"SELECT count(*) FROM $table") == "2", "fast_forward did not carry the branch position-delete to main")
      assert(countOf(spark, s"SELECT count(*) FROM $table WHERE ${Core.long0.columnName} = 1") == "0", "deleted row resurrected on main after fast_forward")
    }(),
    // fast_forward must carry a branch UPDATE (MoR update = position delete + new data file).
    "mbranch.fastForwardUpdate" -> TableTest(Core).step("mbranch.fastForwardUpdate") { (spark, table) =>
      spark.sql(s"ALTER TABLE $table CREATE BRANCH mub")
      spark.sql(s"UPDATE $table.branch_mub SET ${Core.string0.columnName} = 'br-upd' WHERE ${Core.long0.columnName} = 2")
      spark.sql(s"CALL openhouse.system.fast_forward('${catalogRelative(table)}', 'main', 'mub')")
      assert(countOf(spark, s"SELECT count(*) FROM $table") == "3", "fast_forward of a MoR update changed the row count on main")
      assert(spark.sql(s"SELECT ${Core.string0.columnName} FROM $table WHERE ${Core.long0.columnName} = 2").collect()(0).getString(0) == "br-upd",
        "MoR update not carried to main by fast_forward")
    }(),
    // Cherry-pick a branch ROW-DELETE snapshot onto main — CHARACTERIZE (the fragile path): it either
    // applies the delete (main → 2) or is rejected; pin the outcome and assert the row set matches it.
    "mbranch.cherrypickDelete" -> TableTest(Core).step("mbranch.cherrypickDelete") { (spark, table) =>
      spark.sql(s"ALTER TABLE $table CREATE BRANCH mcb")
      spark.sql(s"DELETE FROM $table.branch_mcb WHERE ${Core.long0.columnName} = 1")
      val delSnap = spark.sql(s"SELECT snapshot_id FROM $table.snapshots ORDER BY committed_at DESC LIMIT 1").collect()(0).getLong(0)
      val outcome =
        try { spark.sql(s"CALL openhouse.system.cherrypick_snapshot('${catalogRelative(table)}', ${delSnap}L)"); "ok" }
        catch { case NonFatal(e) => s"rejected:${Exceptions.root(e).getClass.getSimpleName}" }
      val mainCount = countOf(spark, s"SELECT count(*) FROM $table")
      println(s"DIAG mbranch.cherrypickDelete: $outcome, mainCount=$mainCount")
      if (outcome == "ok")
        assert(mainCount == "2", s"cherrypick reported ok but did not apply the branch delete to main (got $mainCount)")
      else
        assert(mainCount == "3", s"cherrypick was rejected but main changed anyway (got $mainCount)")
    }(),
    // REPLACE BRANCH retargets a MoR branch to a pre-delete snapshot — the delete must follow the target.
    "mbranch.replaceBranchDelete" -> TableTest(Core).step("mbranch.replaceBranchDelete") { (spark, table) =>
      val preSnap = spark.sql(s"SELECT snapshot_id FROM $table.snapshots ORDER BY committed_at DESC LIMIT 1").collect()(0).getLong(0) // seed (3 rows)
      spark.sql(s"ALTER TABLE $table CREATE BRANCH mrb")
      spark.sql(s"DELETE FROM $table.branch_mrb WHERE ${Core.long0.columnName} = 1")
      assert(countOf(spark, s"SELECT count(*) FROM $table VERSION AS OF 'mrb'") == "2", "branch delete not applied")
      spark.sql(s"ALTER TABLE $table REPLACE BRANCH mrb AS OF VERSION $preSnap")
      assert(countOf(spark, s"SELECT count(*) FROM $table VERSION AS OF 'mrb'") == "3",
        "REPLACE BRANCH to the pre-delete snapshot did not undo the branch position-delete")
    }()
  )

  // Encryption capability PIN (characterization). OpenHouse delegates table-data encryption to an
  // external KMS plugin (private repo); in OSS the catalog never wires a KeyManagementClient, so
  // customer tables use the default PlaintextEncryptionManager and data is written UNENCRYPTED.
  // Discriminator: a Parquet file's FOOTER magic is "PAR1" when unencrypted and "PARE" under modular
  // encryption — robust regardless of compression. This pins that OSS writes plaintext; it FLIPS to
  // "PARE" the moment table-data encryption is wired (then update BUGS.md and this pin). An off-the-
  // shelf KMS does NOT change this — nothing in the OpenHouse write path invokes the encryption hook.
  val encryptionPlaintextPin: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("surface.pin.dataPlaintext") { (spark, table) =>
        val path = spark.sql(s"SELECT file_path FROM $table.data_files LIMIT 1").collect()(0).getString(0)
        val local = path.stripPrefix("file:")
        val bytes = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(local))
        assert(bytes.length >= 8, s"data file too small to inspect: ${bytes.length} bytes")
        val footerMagic = new String(bytes.takeRight(4), "US-ASCII")
        assert(footerMagic == "PAR1",
          s"expected UNENCRYPTED parquet footer magic PAR1 (OSS encryption is un-wired — capability gap, BUGS.md); " +
          s"got '$footerMagic' — if 'PARE', table-data encryption is now active and this pin should flip to assert ciphertext")
      }()


}
