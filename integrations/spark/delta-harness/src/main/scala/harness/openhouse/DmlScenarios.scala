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

trait DmlScenarios extends ScenarioKit {
  import Rows._

  // ── DDL × consumer battery (BUILD-STATUS task #3) ────────────────────────────────────────────
  // A DDL op is a STATE CHANGE; the battery asserts every consumer still works after it (the
  // modality thesis at the DDL level). DDL preps leave a distinct post-state; consumers are
  // arity-safe (they use SELECT * / metadata tables, never a fixed column list) so they compose
  // over ANY post-DDL schema. NOTE: this is the NON-VACUOUS core — the appraisal's 420 assumed
  // 35 DDL (incl. negatives/one-shots) × 6, but a rejected DDL or a rename has no post-state for a
  // consumer to exercise. State-changing DDL × real consumers is ~54, and that's what's built.
  val ddlPreps: List[(String, Layout => TableTest[CoreTable.type])] = List(
    "addColumn"  -> (l => createAndSeed(l, 3).sql("ddl")(t => s"ALTER TABLE $t ADD COLUMN cc int")()),
    "typeWiden"  -> (l => createAndSeed(l, 3).sql("ddl")(t => s"ALTER TABLE $t ALTER COLUMN ${Core.int0.columnName} TYPE bigint")()),
    "writeOrder" -> (l => createAndSeed(l, 3).sql("ddl")(t => s"ALTER TABLE $t WRITE ORDERED BY ${Core.long0.columnName}")()),
    "distMode"   -> (l => createAndSeed(l, 3).sql("ddl")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.distribution-mode'='range')")())
  )

  private def dupRow(key: Long) = s"SELECT * FROM %s WHERE ${Core.long0.columnName} = $key"  // arity-safe append source

  val ddlConsumers: List[(String, TableTest[CoreTable.type])] = List(
    // C1 the table stays WRITABLE (append) after the DDL — arity-safe self-select append.
    "dmlWrite" -> TableTest(Core).step("consume.dmlWrite") { (spark, table) =>
      spark.sql(s"INSERT INTO $table ${dupRow(1).format(table)}")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 4, "not writable post-DDL")
    }(),
    // C2 the MUTATION path still works after the DDL.
    "dmlMutate" -> TableTest(Core).step("consume.dmlMutate") { (spark, table) =>
      spark.sql(s"DELETE FROM $table WHERE ${Core.long0.columnName} = 2")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2, "mutation broken post-DDL")
    }(),
    // C3 TIME TRAVEL to the pre-DDL/seed snapshot still resolves.
    "timeTravel" -> TableTest(Core).step("consume.timeTravel") { (spark, table) =>
      val s0 = snapshotIds(spark, table).head
      assert(spark.sql(s"SELECT count(*) FROM $table VERSION AS OF $s0").collect()(0).getLong(0) == 3,
        "pre-DDL snapshot not travelable")
    }(),
    // C4 RESTORE across the DDL: write post-DDL, then roll back to the seed snapshot.
    "restore" -> TableTest(Core).step("consume.restore") { (spark, table) =>
      val s0 = snapshotIds(spark, table).head
      spark.sql(s"INSERT INTO $table ${dupRow(1).format(table)}")
      spark.sql(s"CALL openhouse.system.rollback_to_snapshot('${catalogRelative(table)}', $s0)")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "restore across DDL failed")
    }(),
    // C5 EXPIRE after the DDL: history trims, current data survives and reads.
    "expire" -> TableTest(Core).step("consume.expire") { (spark, table) =>
      spark.sql(s"INSERT INTO $table ${dupRow(1).format(table)}")
      spark.sql(s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 4, "unreadable after expire post-DDL")
    }(),
    // C6 BRANCH after the DDL: branchable, write on branch, main isolated.
    "branch" -> TableTest(Core).step("consume.branch") { (spark, table) =>
      spark.sql(s"ALTER TABLE $table CREATE BRANCH cb")
      spark.sql(s"INSERT INTO $table.branch_cb ${dupRow(1).format(table)}")
      assert(spark.sql(s"SELECT count(*) FROM $table VERSION AS OF 'cb'").collect()(0).getLong(0) == 4, "branch write failed post-DDL")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 3, "branch leaked to main post-DDL")
    }(),
    // C7 COMPACTION after the DDL: a second data file, then rewrite_data_files preserves the rows.
    "compact" -> TableTest(Core).step("consume.compact") { (spark, table) =>
      spark.sql(s"INSERT INTO $table ${dupRow(1).format(table)}")   // second data file
      spark.sql(s"CALL openhouse.system.rewrite_data_files(table => '${catalogRelative(table)}', options => map('min-input-files', '2'))")
      assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 4, "compaction changed rows post-DDL")
    }()
  )

  // Closing assertion for the branch axis: after the branch-routed op, MAIN must be untouched
  // (still the 3-row seed) — the isolation half of the branch contract. Uniform across all ops
  // because with spark.wap.branch set every write routes to the branch, never to main.
  val branchMainIsolation: TableTest[CoreTable.type] =
    TableTest(Core).step("branch.mainIsolated") { (spark, table) =>
      spark.conf.unset("spark.wap.branch")
      val mainCount = spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0)
      assert(mainCount == 3, s"branch op leaked to MAIN — expected 3 rows, got $mainCount (isolation broken)")
    }()

  // ── reads ────────────────────────────────────────────────────────────────────────────
  val readProjection: TableTest[CoreTable.type] =
    TableTest(Core).check("read.projection") { view =>
      val expected = view.before.sortBy(_.get(Core.long0)).map(_.get(Core.string0))
      val actual = view.spark
        .sql(s"SELECT ${Core.string0.columnName} FROM ${view.table} ORDER BY ${Core.long0.columnName}")
        .collect().toSeq.map(_.get(Core.string0))
      assert(actual == expected)
    }

  val readFilter: TableTest[CoreTable.type] =
    TableTest(Core).check("read.filter") { view =>
      val expected = view.before.map(_.get(Core.long0)).filter(_ >= 2).sorted
      val actual = view.spark
        .sql(s"SELECT ${Core.long0.columnName} FROM ${view.table} WHERE ${Core.long0.columnName} >= 2 ORDER BY ${Core.long0.columnName}")
        .collect().toSeq.map(_.get(Core.long0))
      assert(actual == expected)
    }

  // The declared write format actually materializes: every data file carries that extension.
  val formatMaterialization: TableTest[CoreTable.type] =
    TableTest(Core).check("format.materialization") { view =>
      val format = view.spark.sql(s"SHOW TBLPROPERTIES ${view.table} ('write.format.default')").collect()(0).getString(1)
      val paths = view.spark.sql(s"SELECT file_path FROM ${view.table}.files").collect().toSeq.map(_.getString(0))
      assert(paths.nonEmpty && paths.forall(_.toLowerCase.endsWith(s".$format")), s"data files are not all .$format: $paths")
    }

  // ── delete ───────────────────────────────────────────────────────────────────────────
  val deleteByPredicate: TableTest[CoreTable.type] =
    TableTest(Core).delete(core => s"${core.long0.columnName} < 2") { view =>
      assert(view.after == view.before.filterNot(_.get(Core.long0) < 2))
    }

  val deleteWhereFalseKeepsSnapshot: TableTest[CoreTable.type] =
    TableTest(Core).delete(_ => "false") { view =>
      assert(view.after == view.before)
      assert(view.snapshotsAfter == view.snapshotsBefore, "DELETE WHERE false must not commit a snapshot")
    }

  val truncate: TableTest[CoreTable.type] =
    TableTest(Core).sql("delete.truncate")(table => s"TRUNCATE TABLE $table") { view =>
      assert(view.after.isEmpty)
    }

  val deleteAtSnapshotRejected: TableTest[CoreTable.type] =
    TableTest(Core).step("delete.atSnapshot.rejected") { (spark, table) =>
      val snapshotId = spark
        .sql(s"SELECT snapshot_id FROM $table.snapshots ORDER BY committed_at DESC LIMIT 1")
        .collect()(0).getLong(0)
      val error = Check.intercept[IllegalArgumentException](
        spark.sql(s"DELETE FROM $table.snapshot_id_$snapshotId WHERE ${Core.long0.columnName} < 4"))
      assert(error.getMessage == s"Cannot delete from table at a specific snapshot: $snapshotId")
    } { view =>
      assert(view.after == view.before) // a rejected delete leaves the table unchanged
    }

  // Removes exactly the keys in the list.
  val deleteByInList: TableTest[CoreTable.type] =
    TableTest(Core).delete(core => s"${core.long0.columnName} IN (1, 3)") { view =>
      assert(keyed(view.after) == view.before.map(_.get(Core.long0)).filterNot(Set(1L, 3L)).sorted)
    }

  // Predicate is an IN-subquery over an explicit source.
  val deleteByInSubquery: TableTest[CoreTable.type] =
    TableTest(Core).delete(core =>
      s"${core.long0.columnName} IN (SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))") { view =>
      assert(keyed(view.after) == view.before.map(_.get(Core.long0)).filterNot(_ == 2L).sorted)
    }

  val deleteByNotInSubquery: TableTest[CoreTable.type] =
    TableTest(Core).delete(core =>
      s"${core.long0.columnName} NOT IN (SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))") { view =>
      assert(keyed(view.after) == view.before.map(_.get(Core.long0)).filter(_ == 2L).sorted)
    }

  val deleteByExistsSubquery: TableTest[CoreTable.type] =
    TableTest(Core).delete(core =>
      s"EXISTS (SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) WHERE s.x = ${core.long0.columnName})") { view =>
      assert(keyed(view.after) == view.before.map(_.get(Core.long0)).filterNot(_ == 2L).sorted)
    }

  val deleteByNotExistsSubquery: TableTest[CoreTable.type] =
    TableTest(Core).delete(core =>
      s"NOT EXISTS (SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) WHERE s.x = ${core.long0.columnName})") { view =>
      assert(keyed(view.after) == view.before.map(_.get(Core.long0)).filter(_ == 2L).sorted)
    }

  val deleteByScalarSubquery: TableTest[CoreTable.type] =
    TableTest(Core).delete(core =>
      s"${core.long0.columnName} = (SELECT max(col1) FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))") { view =>
      assert(keyed(view.after) == view.before.map(_.get(Core.long0)).filterNot(_ == 2L).sorted)
    }

  // Seed a null-string row, then DELETE WHERE string IS NULL must remove exactly it (and nothing
  // else) — a real IS-NULL match, not a vacuous no-op.
  val deleteByNullCondition: TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("delete.byNullCondition.seed")(table =>
        s"INSERT INTO $table VALUES (CAST(99 AS BIGINT), 99, NULL, 99.5, false, '2024-01-01-00')")()
      .delete(core => s"${core.string0.columnName} IS NULL") { view =>
        assert(view.before.exists(_.get(Core.string0) == null), "precondition: a null-string row was seeded")
        val expected = view.before.filterNot(_.get(Core.string0) == null).map(_.get(Core.long0)).sorted
        assert(keyed(view.after) == expected)                 // exactly the non-null rows remain
        assert(!keyed(view.after).contains(99L))              // the null-string row was removed
      }

  // DELETE with no WHERE clause empties the table.
  val deleteAll: TableTest[CoreTable.type] =
    TableTest(Core).sql("delete.all")(table => s"DELETE FROM $table") { view =>
      assert(view.after.isEmpty)
    }

  // A real predicate that matches nothing: rows unchanged, but one (empty) snapshot is still
  // committed — a scanned no-match, unlike the constant-folded `DELETE WHERE false` no-op above.
  val deleteNone: TableTest[CoreTable.type] =
    TableTest(Core).delete(core => s"${core.long0.columnName} = 999") { view =>
      assert(view.after == view.before)
      assert(view.snapshotsAfter == view.snapshotsBefore + 1, "no-match DELETE with a real predicate still commits one snapshot")
    }

  // A partition-column predicate (a metadata-only delete on a partitioned layout).
  val deleteByPartitionPredicate: TableTest[CoreTable.type] =
    TableTest(Core).delete(core => s"${core.datePartition.columnName} = '2024-01-01-00'") { view =>
      val expected = view.before.filterNot(_.get(Core.datePartition) == "2024-01-01-00").map(_.get(Core.long0)).sorted
      assert(keyed(view.after) == expected)
    }

  val deleteWithAlias: TableTest[CoreTable.type] =
    TableTest(Core).sql("delete.withAlias")(table =>
      s"DELETE FROM $table AS x WHERE x.${Core.long0.columnName} < 2") { view =>
      assert(keyed(view.after) == view.before.map(_.get(Core.long0)).filterNot(_ < 2L).sorted)
    }

  // ── update ───────────────────────────────────────────────────────────────────────────
  val updateByPredicate: TableTest[CoreTable.type] =
    TableTest(Core).sql("update.byPredicate")(table =>
      s"UPDATE $table SET ${Core.string0.columnName} = 'X' WHERE ${Core.long0.columnName} = 2") { view =>
      val expected = longToString(view.before).map { case (id, s) => id -> (if (id == 2) "X" else s) }
      assert(longToString(view.after) == expected)
    }

  val updateWithoutCondition: TableTest[CoreTable.type] =
    TableTest(Core).sql("update.withoutCondition")(table =>
      s"UPDATE $table SET ${Core.string0.columnName} = 'Z'") { view =>
      assert(longToString(view.after) == longToString(view.before).map { case (id, _) => id -> "Z" })
    }

  // A real predicate matching nothing still commits an (empty) snapshot — unlike the
  // constant-folded `DELETE WHERE false` no-op (confirmed vs OSS TestUpdate.testUpdateNonExistingRecords).
  val updateNoMatch: TableTest[CoreTable.type] =
    TableTest(Core).sql("update.noMatch")(table =>
      s"UPDATE $table SET ${Core.string0.columnName} = 'Y' WHERE ${Core.long0.columnName} = 99") { view =>
      assert(longToString(view.after) == longToString(view.before))
      assert(view.snapshotsAfter == view.snapshotsBefore + 1, "no-match UPDATE still commits one snapshot")
    }

  private def stringUpdatedWhere(view: StepView[CoreTable.type], matches: Long => Boolean, to: String): Boolean =
    longToString(view.after) == longToString(view.before).map { case (id, s) => id -> (if (matches(id)) to else s) }

  val updateByInSubquery: TableTest[CoreTable.type] =
    TableTest(Core).sql("update.byInSubquery")(table =>
      s"UPDATE $table SET ${Core.string0.columnName} = 'X' " +
        s"WHERE ${Core.long0.columnName} IN (SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))") { view =>
      assert(stringUpdatedWhere(view, _ == 2, "X"))
    }

  val updateByNotInSubquery: TableTest[CoreTable.type] =
    TableTest(Core).sql("update.byNotInSubquery")(table =>
      s"UPDATE $table SET ${Core.string0.columnName} = 'X' " +
        s"WHERE ${Core.long0.columnName} NOT IN (SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))") { view =>
      assert(stringUpdatedWhere(view, _ != 2, "X"))
    }

  val updateByExistsSubquery: TableTest[CoreTable.type] =
    TableTest(Core).sql("update.byExistsSubquery")(table =>
      s"UPDATE $table SET ${Core.string0.columnName} = 'X' " +
        s"WHERE EXISTS (SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) WHERE s.x = ${Core.long0.columnName})") { view =>
      assert(stringUpdatedWhere(view, _ == 2, "X"))
    }

  val updateByNotExistsSubquery: TableTest[CoreTable.type] =
    TableTest(Core).sql("update.byNotExistsSubquery")(table =>
      s"UPDATE $table SET ${Core.string0.columnName} = 'X' " +
        s"WHERE NOT EXISTS (SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) WHERE s.x = ${Core.long0.columnName})") { view =>
      assert(stringUpdatedWhere(view, _ != 2, "X"))
    }

  val updateByScalarSubquery: TableTest[CoreTable.type] =
    TableTest(Core).sql("update.byScalarSubquery")(table =>
      s"UPDATE $table SET ${Core.string0.columnName} = 'X' " +
        s"WHERE ${Core.long0.columnName} = (SELECT max(col1) FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))") { view =>
      assert(stringUpdatedWhere(view, _ == 2, "X"))
    }

  val updateWithAlias: TableTest[CoreTable.type] =
    TableTest(Core).sql("update.withAlias")(table =>
      s"UPDATE $table AS x SET x.${Core.string0.columnName} = 'X' WHERE x.${Core.long0.columnName} = 2") { view =>
      assert(stringUpdatedWhere(view, _ == 2, "X"))
    }

  // Sets two columns in one statement; assert both landed on the matched row.
  val updateMultipleColumns: TableTest[CoreTable.type] =
    TableTest(Core).sql("update.multipleColumns")(table =>
      s"UPDATE $table SET ${Core.string0.columnName} = 'X', ${Core.int0.columnName} = 99 WHERE ${Core.long0.columnName} = 2") { view =>
      assert(stringUpdatedWhere(view, _ == 2, "X"))
      assert(view.after.find(_.get(Core.long0) == 2L).map(_.get(Core.int0)).contains(99))
    }

  // Assign a column by an expression over itself (updates the key column).
  val updateByExpression: TableTest[CoreTable.type] =
    TableTest(Core).sql("update.byExpression")(table =>
      s"UPDATE $table SET ${Core.long0.columnName} = ${Core.long0.columnName} + 10 WHERE ${Core.long0.columnName} = 2") { view =>
      assert(keyed(view.after) == view.before.map(_.get(Core.long0)).map(l => if (l == 2L) 12L else l).sorted)
    }

  // Update the partition column so the row moves partitions.
  val updateMovePartition: TableTest[CoreTable.type] =
    TableTest(Core).sql("update.movePartition")(table =>
      s"UPDATE $table SET ${Core.datePartition.columnName} = '2099-12-31-23' WHERE ${Core.long0.columnName} = 2") { view =>
      val part = (rows: Seq[Row]) => rows.map(r => r.get(Core.long0) -> r.get(Core.datePartition)).toMap
      assert(part(view.after) == part(view.before).map { case (id, d) => id -> (if (id == 2) "2099-12-31-23" else d) })
    }

  val updateNullAssignment: TableTest[CoreTable.type] =
    TableTest(Core).sql("update.nullAssignment")(table =>
      s"UPDATE $table SET ${Core.string0.columnName} = NULL WHERE ${Core.long0.columnName} = 2") { view =>
      assert(longToString(view.after) == longToString(view.before).map { case (id, s) => id -> (if (id == 2) null else s) })
    }

  // ── merge ────────────────────────────────────────────────────────────────────────────
  // Source rows are written as EXPLICIT literals. The generator-sourced alternative for this
  // test would be:
  //   USING (${RowGenerator.valuesClause(Core, ...)} for indices 4,5) ... WHEN NOT MATCHED THEN INSERT *
  // i.e. name the row *indices* and let the column generators fill every column. We prefer the
  // explicit form so the source values are visible in the test.
  val mergeInsertNotMatched: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.insertNotMatched")(table =>
      s"""MERGE INTO $table t USING (
            SELECT * FROM VALUES
              (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
              (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
            AS s($cols)
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN NOT MATCHED THEN INSERT *""") { view =>
      assert(keyed(view.after) == (view.before.map(_.get(Core.long0)) ++ Seq(4L, 5L)).sorted)
      // INSERT * must map the columns correctly, not just land the join key.
      assert(view.after.find(_.get(Core.long0) == 4L).map(_.get(Core.string0)).contains("row-4"))
      assert(view.after.find(_.get(Core.long0) == 5L).map(_.get(Core.string0)).contains("row-5"))
    }

  val mergeUpdateMatched: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.updateMatched")(table =>
      s"""MERGE INTO $table t USING (
            SELECT * FROM VALUES (CAST(2 AS BIGINT), 'M') AS s(${Core.long0.columnName}, ${Core.string0.columnName})
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN MATCHED THEN UPDATE SET t.${Core.string0.columnName} = s.${Core.string0.columnName}""") { view =>
      val expected = longToString(view.before).map { case (id, s) => id -> (if (id == 2) "M" else s) }
      assert(longToString(view.after) == expected)
    }

  val mergeDeleteMatched: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.deleteMatched")(table =>
      s"""MERGE INTO $table t USING (
            SELECT * FROM VALUES (CAST(1 AS BIGINT)), (CAST(3 AS BIGINT)) AS s(${Core.long0.columnName})
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN MATCHED THEN DELETE""") { view =>
      assert(keyed(view.after) == view.before.map(_.get(Core.long0)).filterNot(Set(1L, 3L)).sorted)
    }

  val mergeUpsert: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.upsert")(table =>
      s"""MERGE INTO $table t USING (
            SELECT * FROM VALUES
              (CAST(2 AS BIGINT), 2, 'U', 2.5, true,  '2024-01-02-01'),
              (CAST(7 AS BIGINT), 7, 'g', 7.5, false, '2024-01-07-06')
            AS s($cols)
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN MATCHED THEN UPDATE SET t.${Core.string0.columnName} = s.${Core.string0.columnName}
          WHEN NOT MATCHED THEN INSERT *""") { view =>
      val updated = longToString(view.before).map { case (id, s) => id -> (if (id == 2) "U" else s) }
      val withInsert = if (view.before.exists(_.get(Core.long0) == 7L)) updated else updated + (7L -> "g")
      assert(longToString(view.after) == withInsert)
    }

  // Keep only rows the source knows about: delete every row NOT matched by a source row.
  val mergeDeleteNotMatchedBySource: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.deleteNotMatchedBySource")(table =>
      s"""MERGE INTO $table t USING (
            SELECT * FROM VALUES (CAST(2 AS BIGINT)) AS s(${Core.long0.columnName})
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN NOT MATCHED BY SOURCE THEN DELETE""") { view =>
      assert(keyed(view.after) == view.before.map(_.get(Core.long0)).filter(_ == 2L).sorted)
    }

  // Both keys 2 and 3 match, but the per-clause condition only fires for key 2.
  val mergeConditionalUpdate: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.conditionalUpdate")(table =>
      s"""MERGE INTO $table t USING (
            SELECT * FROM VALUES (CAST(2 AS BIGINT), 'U2'), (CAST(3 AS BIGINT), 'U3')
            AS s(${Core.long0.columnName}, ${Core.string0.columnName})
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN MATCHED AND s.${Core.long0.columnName} = 2 THEN UPDATE SET t.${Core.string0.columnName} = s.${Core.string0.columnName}""") { view =>
      assert(longToString(view.after) == longToString(view.before).map { case (id, s) => id -> (if (id == 2) "U2" else s) })
    }

  // First matched clause wins: key 2 updates (conditional), key 3 falls through to DELETE.
  val mergeMultipleMatchedClauses: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.multipleMatchedClauses")(table =>
      s"""MERGE INTO $table t USING (
            SELECT * FROM VALUES (CAST(2 AS BIGINT), 'U'), (CAST(3 AS BIGINT), 'x')
            AS s(${Core.long0.columnName}, ${Core.string0.columnName})
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN MATCHED AND s.${Core.long0.columnName} = 2 THEN UPDATE SET t.${Core.string0.columnName} = s.${Core.string0.columnName}
          WHEN MATCHED THEN DELETE""") { view =>
      assert(keyed(view.after) == view.before.map(_.get(Core.long0)).filterNot(_ == 3L).sorted)
      assert(view.after.find(_.get(Core.long0) == 2L).map(_.get(Core.string0)).contains("U"))
    }

  // Conditional NOT MATCHED: source keys 4 and 5, but only 4 satisfies the insert condition.
  val mergeConditionalInsert: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.conditionalInsert")(table =>
      s"""MERGE INTO $table t USING (
            SELECT * FROM VALUES
              (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
              (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
            AS s($cols)
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN NOT MATCHED AND s.${Core.long0.columnName} = 4 THEN INSERT *""") { view =>
      assert(keyed(view.after) == (view.before.map(_.get(Core.long0)) :+ 4L).sorted)
    }

  // All three clause kinds in one statement: update key 2, insert key 4, delete-by-source rows 1 & 3.
  val mergeAllClauses: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.allClauses")(table =>
      s"""MERGE INTO $table t USING (
            SELECT * FROM VALUES
              (CAST(2 AS BIGINT), 2, 'M2', 2.5, true,  '2024-01-02-01'),
              (CAST(4 AS BIGINT), 4, 'row-4', 4.5, false, '2024-01-04-03')
            AS s($cols)
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN MATCHED THEN UPDATE SET t.${Core.string0.columnName} = s.${Core.string0.columnName}
          WHEN NOT MATCHED THEN INSERT *
          WHEN NOT MATCHED BY SOURCE THEN DELETE""") { view =>
      assert(keyed(view.after) == Seq(2L, 4L))
      assert(view.after.find(_.get(Core.long0) == 2L).map(_.get(Core.string0)).contains("M2"))
    }

  // UPDATE SET * replaces every column of the matched row from the source.
  val mergeUpdateStar: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.updateStar")(table =>
      s"""MERGE INTO $table t USING (
            SELECT * FROM VALUES (CAST(2 AS BIGINT), 22, 'S2', 22.5, true, '2024-06-06-06') AS s($cols)
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN MATCHED THEN UPDATE SET *""") { view =>
      val row2 = view.after.find(_.get(Core.long0) == 2L)
      assert(row2.map(_.get(Core.string0)).contains("S2"))
      assert(row2.map(_.get(Core.int0)).contains(22))
    }

  // Explicit column-specification INSERT (other columns null-filled).
  val mergeInsertExplicitColumns: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.insertExplicitColumns")(table =>
      s"""MERGE INTO $table t USING (
            SELECT * FROM VALUES (CAST(7 AS BIGINT), 'g') AS s(${Core.long0.columnName}, ${Core.string0.columnName})
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN NOT MATCHED THEN INSERT (${Core.long0.columnName}, ${Core.string0.columnName}) VALUES (s.${Core.long0.columnName}, s.${Core.string0.columnName})""") { view =>
      assert(keyed(view.after) == (view.before.map(_.get(Core.long0)) :+ 7L).sorted)
      assert(view.after.find(_.get(Core.long0) == 7L).map(_.get(Core.string0)).contains("g"))
    }

  // Source is a CTE.
  val mergeSourceCTE: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.sourceCTE")(table =>
      s"""MERGE INTO $table t USING (
            WITH src AS (SELECT CAST(8 AS BIGINT) AS ${Core.long0.columnName}) SELECT * FROM src
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN NOT MATCHED THEN INSERT (${Core.long0.columnName}) VALUES (s.${Core.long0.columnName})""") { view =>
      assert(keyed(view.after) == (view.before.map(_.get(Core.long0)) :+ 8L).sorted)
    }

  // Source is a set operation (UNION ALL).
  val mergeSourceSetOp: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.sourceSetOp")(table =>
      s"""MERGE INTO $table t USING (
            SELECT CAST(8 AS BIGINT) AS ${Core.long0.columnName} UNION ALL SELECT CAST(9 AS BIGINT)
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN NOT MATCHED THEN INSERT (${Core.long0.columnName}) VALUES (s.${Core.long0.columnName})""") { view =>
      assert(keyed(view.after) == (view.before.map(_.get(Core.long0)) ++ Seq(8L, 9L)).sorted)
    }

  // Merge into an empty target inserts all non-matching source rows (empties the seed first).
  val mergeIntoEmptyTarget: TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("merge.intoEmptyTarget.empty")(table => s"DELETE FROM $table")()
      .sql("merge.intoEmptyTarget")(table =>
        s"""MERGE INTO $table t USING (
              SELECT * FROM VALUES
                (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
              AS s($cols)
            ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
            WHEN NOT MATCHED THEN INSERT *""") { view =>
        assert(view.before.isEmpty)
        assert(keyed(view.after) == Seq(4L, 5L))
      }

  // A null join key never matches, so it neither updates nor errors.
  val mergeNullJoinKey: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.nullJoinKey")(table =>
      s"""MERGE INTO $table t USING (
            SELECT * FROM VALUES (CAST(NULL AS BIGINT), 'n'), (CAST(2 AS BIGINT), 'M')
            AS s(${Core.long0.columnName}, ${Core.string0.columnName})
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN MATCHED THEN UPDATE SET t.${Core.string0.columnName} = s.${Core.string0.columnName}""") { view =>
      assert(keyed(view.after) == keyed(view.before))
      assert(longToString(view.after) == longToString(view.before).map { case (id, s) => id -> (if (id == 2) "M" else s) })
    }

  // INSERT * resolves columns by name even when the source lists them in a different order.
  val mergeResolveByName: TableTest[CoreTable.type] =
    TableTest(Core).sql("merge.resolveByName")(table =>
      s"""MERGE INTO $table t USING (
            SELECT * FROM VALUES ('g', CAST(7 AS BIGINT), 7, 7.5, false, '2024-07-07-07')
            AS s(${Core.string0.columnName}, ${Core.long0.columnName}, ${Core.int0.columnName}, ${Core.double0.columnName}, ${Core.boolean0.columnName}, datepartition)
          ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
          WHEN NOT MATCHED THEN INSERT *""") { view =>
      assert(keyed(view.after) == (view.before.map(_.get(Core.long0)) :+ 7L).sorted)
      assert(view.after.find(_.get(Core.long0) == 7L).map(_.get(Core.string0)).contains("g"))
    }

  // ── insert / append / overwrite ────────────────────────────────────────────────────────
  val insertInto: TableTest[CoreTable.type] =
    TableTest(Core).sql("insert.into")(table =>
      s"""INSERT INTO $table VALUES
            (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
            (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')""") { view =>
      assert(keyed(view.after) == (view.before.map(_.get(Core.long0)) ++ Seq(4L, 5L)).sorted)
    }

  val appendDataFrame: TableTest[CoreTable.type] =
    TableTest(Core).step("append.dataFrame") { (spark, table) =>
      val frame = spark.sql(
        s"SELECT * FROM VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05') AS s($cols)")
      frame.writeTo(table).append()
    } { view =>
      assert(keyed(view.after) == (view.before.map(_.get(Core.long0)) :+ 6L).sorted)
    }

  // INSERT OVERWRITE (static mode, the Spark default) replaces the whole table regardless of state.
  val insertOverwrite: TableTest[CoreTable.type] =
    TableTest(Core).sql("insert.overwrite")(table =>
      s"""INSERT OVERWRITE $table VALUES
            (CAST(1 AS BIGINT), 1, 'p', 1.5, false, '2024-01-01-00'),
            (CAST(2 AS BIGINT), 2, 'q', 2.5, true,  '2024-01-02-01')""") { view =>
      assert(keyed(view.after) == Seq(1L, 2L))
    }

  val overwriteDataFrame: TableTest[CoreTable.type] =
    TableTest(Core).step("overwrite.dataFrame") { (spark, table) =>
      val frame = spark.sql(
        s"SELECT * FROM VALUES (CAST(8 AS BIGINT), 8, 'h', 8.5, false, '2024-01-08-07') AS s($cols)")
      frame.writeTo(table).overwrite(org.apache.spark.sql.functions.lit(true))
    } { view =>
      assert(keyed(view.after) == Seq(8L))
    }

  // INSERT INTO with an explicit column list; the unlisted columns are null-filled.
  // NEGATIVE PIN (was SKIP-as-bug; reclassified after code-verified investigation). A partial/named-
  // column INSERT that omits other columns is REJECTED with INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA.
  // This is an ENGINE limitation, not an OpenHouse policy: OpenHouse creates columns nullable-by-default
  // and the server round-trips the schema verbatim (verified) — but Iceberg 1.5's SparkTable does not
  // advertise column defaults (no SupportsColumnDefaultValue), so Spark's byName output resolution never
  // inserts the NULL-fill projection for the omitted (nullable) columns. Pin the rejection; it flips
  // only when the read+write APPLICATION of column defaults is wired (SparkTable implements
  // SupportsColumnDefaultValue + the reader injects initial-default for missing columns). NOTE (fork
  // audit): the com.linkedin.iceberg 1.5.2 fork #251 backported the NestedField initial/write-default
  // APIs + SchemaParser serialization ONLY — no SparkTable, no reader wiring — so the fork does NOT
  // satisfy the flip condition (and persists v3-style defaults on a v2 table with no gate). See
  // ICEBERG-FORK-AUDIT.md.
  val insertExplicitColumns: TableTest[CoreTable.type] =
    TableTest(Core).step("insert.explicitColumns") { (spark, table) =>
      val e = Check.intercept[Exception](
        spark.sql(s"INSERT INTO $table (${Core.long0.columnName}, ${Core.string0.columnName}) " +
          s"VALUES (CAST(4 AS BIGINT), 'd'), (CAST(5 AS BIGINT), 'e')"))
      val msg = Option(e.getMessage).getOrElse("").toUpperCase
      assert(msg.contains("CANNOT_FIND_DATA") || msg.contains("CANNOT FIND DATA") || msg.contains("INCOMPATIBLE_DATA"),
        s"expected a partial-INSERT rejection naming the omitted column (engine limitation), got: ${Option(e.getMessage).getOrElse("").take(200)}")
    }()

  // INSERT INTO … SELECT appends the selected rows.
  val insertIntoSelect: TableTest[CoreTable.type] =
    TableTest(Core).sql("insert.intoSelect")(table =>
      s"INSERT INTO $table SELECT * FROM VALUES " +
        s"(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05') AS s($cols)") { view =>
      assert(keyed(view.after) == (view.before.map(_.get(Core.long0)) :+ 6L).sorted)
    }

  // ── partitioned-only: selective-partition replacement (meaningful only when partitioned) ──
  // Seed rows 1/2/3 live in partitions '2024-01-01-00'/'01'/'02'. Writing one row into partition
  // '…-00' must replace only that partition, leaving rows 2 and 3.
  // Delta-sound: writing row 10 into partition '…-00' replaces ONLY that partition's rows (the
  // seeded row 1), leaving every other partition's rows and adding 10.
  private def onlyFirstPartitionReplaced(view: StepView[CoreTable.type]): Seq[Long] =
    (view.before.filterNot(_.get(Core.datePartition) == "2024-01-01-00").map(_.get(Core.long0)) :+ 10L).sorted

  val insertDynamicOverwrite: TableTest[CoreTable.type] =
    TableTest(Core).step("insert.dynamicOverwrite") { (spark, table) =>
      spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      try spark.sql(s"INSERT OVERWRITE $table VALUES (CAST(10 AS BIGINT), 10, 'p', 10.5, true, '2024-01-01-00')")
      finally spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    } { view =>
      assert(keyed(view.after) == onlyFirstPartitionReplaced(view))
    }

  val overwritePartitions: TableTest[CoreTable.type] =
    TableTest(Core).step("overwrite.partitions") { (spark, table) =>
      val frame = spark.sql(
        s"SELECT * FROM VALUES (CAST(10 AS BIGINT), 10, 'p', 10.5, true, '2024-01-01-00') AS s($cols)")
      frame.writeTo(table).overwritePartitions()
    } { view =>
      assert(keyed(view.after) == onlyFirstPartitionReplaced(view))
    }

  // ── create (a preparation-only test: create under the layout, assert schema + emptiness) ─
  // Also the guard that the literal `columnDefinitions` matches CoreTable's declared columns.
  def createSchema(layout: Layout): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(layout.create) { view =>
      val actual = view.spark.table(view.table).schema.fields.toList.map(field => (field.name, field.dataType.simpleString))
      val expected = Core.tableColumns.toList.map(column => (column.columnName, column.sqlType))
      assert(actual == expected)
      assert(view.after.isEmpty)
    }

  // ── DDL Phase 12: schema evolution — ADD COLUMN family (❓ probes settle B-vs-N) ───────────
  // The added column is not one of CoreTable's typed handles, so these assert on the LIVE schema
  // (name / type / comment / order) and raw SQL, not on typed row handles. Row snapshots
  // (view.before/after) still read only CoreTable's columns, so they stay valid across the ALTER.
  private def liveColumns(view: StepView[CoreTable.type]): Seq[(String, String)] =
    view.spark.table(view.table).schema.fields.toSeq.map(field => (field.name, field.dataType.simpleString))

  val ddlAddColumnSingle: TableTest[CoreTable.type] =
    TableTest(Core).sql("ddl.addColumn.single")(t => s"ALTER TABLE $t ADD COLUMN added_int int") { view =>
      assert(liveColumns(view).map(_._1).contains("added_int"), s"added_int missing: ${liveColumns(view).map(_._1)}")
      val nullCount = view.spark.sql(s"SELECT count(*) FROM ${view.table} WHERE added_int IS NULL").collect()(0).getLong(0)
      assert(nullCount == view.before.size, s"existing rows should read null for added_int: $nullCount != ${view.before.size}")
      assert(view.after.size == view.before.size)                                       // ADD COLUMN keeps rows
    }

  val ddlAddColumnMultiple: TableTest[CoreTable.type] =
    TableTest(Core).sql("ddl.addColumn.multiple")(t => s"ALTER TABLE $t ADD COLUMNS (added_a int, added_b string)") { view =>
      val names = liveColumns(view).map(_._1)
      assert(names.contains("added_a") && names.contains("added_b"), s"added columns missing: $names")
      assert(view.after.size == view.before.size)
    }

  val ddlAddColumnComment: TableTest[CoreTable.type] =
    TableTest(Core).sql("ddl.addColumn.comment")(t => s"ALTER TABLE $t ADD COLUMN added_c int COMMENT 'a note'") { view =>
      val field = view.spark.table(view.table).schema.fields.find(_.name == "added_c")
      assert(field.isDefined, "added_c missing")
      assert(field.get.getComment().contains("a note"), s"comment not stored: ${field.flatMap(_.getComment())}")
    }

  val ddlAddColumnPosition: TableTest[CoreTable.type] =
    TableTest(Core).sql("ddl.addColumn.position")(t => s"ALTER TABLE $t ADD COLUMN added_after int AFTER ${Core.long0.columnName}") { view =>
      val names = liveColumns(view).map(_._1)
      assert(names.indexOf("added_after") == names.indexOf(Core.long0.columnName) + 1, s"added_after not after long0: $names")
    }

  val ddlAlterColumnTypeWiden: TableTest[CoreTable.type] =
    TableTest(Core).sql("ddl.alterColumn.typeWiden")(t => s"ALTER TABLE $t ALTER COLUMN ${Core.int0.columnName} TYPE bigint") { view =>
      assert(liveColumns(view).toMap.get(Core.int0.columnName).contains("bigint"), s"int0 not widened: ${liveColumns(view).toMap.get(Core.int0.columnName)}")
      val vals = view.spark.sql(s"SELECT ${Core.int0.columnName} FROM ${view.table} ORDER BY ${Core.long0.columnName}").collect().toSeq.map(_.getLong(0))
      assert(vals == Seq(1L, 2L, 3L), s"values not preserved after widening: $vals")
    }

  // RENAME COLUMN is a SILENT NO-OP on OpenHouse (tagged bug): the statement neither errors nor renames
  // — verified via REFRESH TABLE + fresh DESCRIBE, the column keeps its old name. The recon predicted a
  // server rejection ("not found in newSchema"), but the client drops the rename before it reaches the
  // server, so nothing happens. This test asserts the CORRECT behavior (rename applies) and is tagged in
  // Plan.knownBugs, so it reports SKIP until fixed. A silent no-op is worse than a clean rejection.
  val ddlRenameColumn: TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("ddl.renameColumn.seed")(t => s"ALTER TABLE $t ADD COLUMN to_rename int")()
      .sql("ddl.renameColumn")(t => s"ALTER TABLE $t RENAME COLUMN to_rename TO renamed_col") { view =>
        val names = liveColumns(view).map(_._1)
        assert(names.contains("renamed_col") && !names.contains("to_rename"), s"RENAME COLUMN silently no-oped: $names")
        assert(view.after.size == view.before.size)
      }

  /** Phase 12 DDL schema-evolution behaviors, crossed with every layout. */
  val ddlSchemaOperations: List[(String, TableTest[CoreTable.type])] = List(
    "ddl.addColumn.single"      -> ddlAddColumnSingle,
    "ddl.addColumn.multiple"    -> ddlAddColumnMultiple,
    "ddl.addColumn.comment"     -> ddlAddColumnComment,
    "ddl.addColumn.position"    -> ddlAddColumnPosition,
    "ddl.alterColumn.typeWiden" -> ddlAlterColumnTypeWiden,
    "ddl.renameColumn"          -> ddlRenameColumn
  )

  /** The operations crossed with every layout, each a headless segment, in report order. */
  val operations: List[(String, TableTest[CoreTable.type])] = List(
    "read.projection"                -> readProjection,
    "read.filter"                    -> readFilter,
    "format.materialization"         -> formatMaterialization,
    "delete.byPredicate"             -> deleteByPredicate,
    "delete.byInList"                -> deleteByInList,
    "delete.byInSubquery"            -> deleteByInSubquery,
    "delete.byNotInSubquery"         -> deleteByNotInSubquery,
    "delete.byExistsSubquery"        -> deleteByExistsSubquery,
    "delete.byNotExistsSubquery"     -> deleteByNotExistsSubquery,
    "delete.byScalarSubquery"        -> deleteByScalarSubquery,
    "delete.byNullCondition"         -> deleteByNullCondition,
    "delete.all"                     -> deleteAll,
    "delete.none"                    -> deleteNone,
    "delete.byPartitionPredicate"    -> deleteByPartitionPredicate,
    "delete.withAlias"               -> deleteWithAlias,
    "delete.whereFalse.noSnapshot"   -> deleteWhereFalseKeepsSnapshot,
    "delete.truncate"                -> truncate,
    "delete.atSnapshot.rejected"     -> deleteAtSnapshotRejected,
    "update.byPredicate"             -> updateByPredicate,
    "update.withoutCondition"        -> updateWithoutCondition,
    "update.noMatch"                 -> updateNoMatch,
    "update.byInSubquery"            -> updateByInSubquery,
    "update.byNotInSubquery"         -> updateByNotInSubquery,
    "update.byExistsSubquery"        -> updateByExistsSubquery,
    "update.byNotExistsSubquery"     -> updateByNotExistsSubquery,
    "update.byScalarSubquery"        -> updateByScalarSubquery,
    "update.withAlias"               -> updateWithAlias,
    "update.multipleColumns"         -> updateMultipleColumns,
    "update.byExpression"            -> updateByExpression,
    "update.movePartition"           -> updateMovePartition,
    "update.nullAssignment"          -> updateNullAssignment,
    "merge.insertNotMatched"         -> mergeInsertNotMatched,
    "merge.updateMatched"            -> mergeUpdateMatched,
    "merge.deleteMatched"            -> mergeDeleteMatched,
    "merge.upsert"                   -> mergeUpsert,
    "merge.deleteNotMatchedBySource" -> mergeDeleteNotMatchedBySource,
    "merge.conditionalUpdate"        -> mergeConditionalUpdate,
    "merge.multipleMatchedClauses"   -> mergeMultipleMatchedClauses,
    "merge.conditionalInsert"        -> mergeConditionalInsert,
    "merge.allClauses"               -> mergeAllClauses,
    "merge.updateStar"               -> mergeUpdateStar,
    "merge.insertExplicitColumns"    -> mergeInsertExplicitColumns,
    "merge.sourceCTE"                -> mergeSourceCTE,
    "merge.sourceSetOp"              -> mergeSourceSetOp,
    "merge.intoEmptyTarget"          -> mergeIntoEmptyTarget,
    "merge.nullJoinKey"              -> mergeNullJoinKey,
    "merge.resolveByName"            -> mergeResolveByName,
    "insert.into"                    -> insertInto,
    "insert.explicitColumns"         -> insertExplicitColumns,
    "insert.intoSelect"              -> insertIntoSelect,
    "append.dataFrame"               -> appendDataFrame,
    "insert.overwrite"               -> insertOverwrite,
    "overwrite.dataFrame"            -> overwriteDataFrame
  )

  /** Operations meaningful only on a partitioned table; crossed with the partitioned layouts only. */
  val partitionedOperations: List[(String, TableTest[CoreTable.type])] = List(
    "insert.dynamicOverwrite"        -> insertDynamicOverwrite,
    "overwrite.partitions"           -> overwritePartitions
  )

  /** The DELETE/UPDATE/MERGE subset — the operations affected by the CoW-vs-MoR mode. */
  val mutationOperations: List[(String, TableTest[CoreTable.type])] =
    operations.filter { case (name, _) =>
      name.startsWith("delete.") || name.startsWith("update.") || name.startsWith("merge.")
    }

  // ── MoR discriminator: prove merge-on-read actually wrote position-delete files ──────────
  // The rest of the MoR axis reuses CoW's row-delta assertions, which pass identically whether the
  // write was copy-on-write or merge-on-read. These two pin the PHYSICAL difference: a MoR delete
  // MUST add a position-delete file; a CoW delete must NOT. Both are prepared with
  // `createAndSeedSingleFile` and delete a strict subset (`long0 < 2` → 1 of 3 rows), so the write
  // cannot be satisfied by whole-file elimination — the outcome is deterministic across formats
  // (verified: parquet/orc/avro all add exactly one position delete under MoR, none under CoW).
  private def deleteFileCount(spark: SparkSession, table: String): Long =
    spark.sql(s"SELECT count(*) FROM $table.delete_files").collect()(0).getLong(0)

  val morWritesDeleteFiles: TableTest[CoreTable.type] =
    TableTest(Core).delete(core => s"${core.long0.columnName} < 2") { view =>
      assert(view.after == view.before.filterNot(_.get(Core.long0) < 2))                 // rows correct
      assert(deleteFileCount(view.spark, view.table) >= 1,
        "merge-on-read DELETE of a strict subset of a data file must write a position-delete file")
    }

  val cowWritesNoDeleteFiles: TableTest[CoreTable.type] =
    TableTest(Core).delete(core => s"${core.long0.columnName} < 2") { view =>
      assert(view.after == view.before.filterNot(_.get(Core.long0) < 2))
      assert(deleteFileCount(view.spark, view.table) == 0, "copy-on-write DELETE must not write delete files")
    }


}
