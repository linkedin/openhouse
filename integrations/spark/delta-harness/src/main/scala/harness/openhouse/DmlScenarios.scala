package harness

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit

// The DML tests are written as two independent lists. A TablePreparation describes a starting
// table state (layout, evolution, restored table). A DmlTestCase describes one operation and asserts
// the rows and the snapshot delta that operation causes. A bucket of cases is the cross of a
// preparation list with the test-case list it is compatible with, so every case reads as "this
// operation, on this starting state". The named test-case lists below are the shared vocabulary a
// feature layer reuses: it crosses them with its own preparations through a self-type on this
// trait.
trait DmlScenarios extends ScenarioKit {
  import Rows._

  // --- the DML test cases ---
  // Each case captures the table state, runs one operation, captures the state again, and asserts
  // the row change and the snapshot delta that operation caused. Deltas are relative, so a case
  // holds on any preparation regardless of how many snapshots the preparation itself committed.

  val readTestCases: List[DmlTestCase[CoreTable.type]] = List(
    DmlTestCase(
      "read.projection",
      s"SELECT of ${Core.string0.columnName} alone returns that column for every prepared row in " +
        "key order and leaves the table state unchanged.",
      table => {
        val before = table.state
        val projected = table.spark
          .sql(
            s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
              s"ORDER BY ${Core.long0.columnName}")
          .collect()
          .toSeq
          .map(_.get(Core.string0))
        val after = table.state

        assert(
          projected == before.rows.sortBy(_.get(Core.long0)).map(_.get(Core.string0)),
          s"projection returned $projected")
        assert(after == before, "a read leaves the rows and the snapshot count unchanged")
      }),
    DmlTestCase(
      "read.filter",
      s"SELECT with a ${Core.long0.columnName} >= 2 predicate returns exactly the prepared rows " +
        "whose key is 2 or greater and leaves the table state unchanged.",
      table => {
        val before = table.state
        val selected = table.spark
          .sql(
            s"SELECT ${Core.long0.columnName} FROM ${table.name} " +
              s"WHERE ${Core.long0.columnName} >= 2 ORDER BY ${Core.long0.columnName}")
          .collect()
          .toSeq
          .map(_.get(Core.long0))
        val after = table.state

        assert(
          selected == before.rows.map(_.get(Core.long0)).filter(_ >= 2).sorted,
          s"filtered read returned $selected")
        assert(after == before, "a read leaves the rows and the snapshot count unchanged")
      }))

  // The DELETE for the preparations that already hold a row with a null string. It is its own list
  // because it only means something against those starting states.
  val nullStringRowTestCases: List[DmlTestCase[CoreTable.type]] = List(
    DmlTestCase(
      "delete.byNullCondition",
      s"DELETE WHERE ${Core.string0.columnName} IS NULL removes exactly the prepared row whose " +
        "string is null, leaves every other row byte for byte as it was, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.string0.columnName} IS NULL")
        val after = table.state

        assert(
          after.rows == before.rows.filter(row => Option(row.get(Core.string0)).nonEmpty),
          s"rows after the null-condition DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by a null condition commits one snapshot")
      }))

  private val deleteByPartitionPredicate: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "delete.byPartitionPredicate",
      s"DELETE WHERE ${Core.datePartition.columnName} = '2024-01-01-00' removes the rows in that " +
        "partition value, keeps the rest, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE " +
            s"${Core.datePartition.columnName} = '2024-01-01-00'")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(_.get(Core.datePartition) == "2024-01-01-00"),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by a partition predicate commits one snapshot")
      })

  private val deleteTestCases: List[DmlTestCase[CoreTable.type]] = List(
    DmlTestCase(
      "delete.byPredicate",
      s"DELETE WHERE ${Core.long0.columnName} < 2 removes the rows below key 2, keeps every other " +
        "row untouched, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} < 2")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(_.get(Core.long0) < 2),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by a predicate commits one snapshot")
      }),
    DmlTestCase(
      "delete.byInList",
      s"DELETE WHERE ${Core.long0.columnName} IN (1, 3) removes keys 1 and 3, leaves every " +
        "other row exactly as prepared, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} IN (1, 3)")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(row => Set(1L, 3L)(row.get(Core.long0))),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by an IN list commits one snapshot")
      }),
    DmlTestCase(
      "delete.byInSubquery",
      s"DELETE WHERE ${Core.long0.columnName} IN (subquery yielding 2) removes key 2, leaves " +
        "every other row exactly as prepared, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} IN (" +
            "SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(_.get(Core.long0) == 2L),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by an IN subquery commits one snapshot")
      }),
    DmlTestCase(
      "delete.byNotInSubquery",
      s"DELETE WHERE ${Core.long0.columnName} NOT IN (subquery yielding 2) removes every key other " +
        "than 2 and leaves the row for key 2 exactly as prepared, in one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} NOT IN (" +
            "SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")
        val after = table.state

        assert(
          after.rows == before.rows.filter(_.get(Core.long0) == 2L),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by a NOT IN subquery commits one snapshot")
      }),
    DmlTestCase(
      "delete.byExistsSubquery",
      s"DELETE WHERE EXISTS (correlated subquery matching ${Core.long0.columnName} = 2) removes " +
        "key 2, leaves every other row exactly as prepared, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE EXISTS (" +
            "SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) " +
            s"WHERE s.x = ${Core.long0.columnName})")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(_.get(Core.long0) == 2L),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by an EXISTS subquery commits one snapshot")
      }),
    DmlTestCase(
      "delete.byNotExistsSubquery",
      s"DELETE WHERE NOT EXISTS (correlated subquery matching ${Core.long0.columnName} = 2) removes " +
        "every key other than 2 and leaves the row for key 2 exactly as prepared, in one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE NOT EXISTS (" +
            "SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) " +
            s"WHERE s.x = ${Core.long0.columnName})")
        val after = table.state

        assert(
          after.rows == before.rows.filter(_.get(Core.long0) == 2L),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by a NOT EXISTS subquery commits one snapshot")
      }),
    DmlTestCase(
      "delete.byScalarSubquery",
      s"DELETE WHERE ${Core.long0.columnName} = (scalar subquery yielding 2) removes key 2, " +
        "leaves every other row exactly as prepared, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = (" +
            "SELECT max(col1) FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(_.get(Core.long0) == 2L),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE by a scalar subquery commits one snapshot")
      }),
    DmlTestCase(
      "delete.all",
      "DELETE FROM without a predicate empties the table and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(s"DELETE FROM ${table.name}")
        val after = table.state

        assert(after.rows.isEmpty, s"rows survived the unconditional DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "an unconditional DELETE commits one snapshot")
      }),
    DmlTestCase(
      "delete.none",
      s"DELETE WHERE ${Core.long0.columnName} = 999 matches no row, keeps every row, and still " +
        "commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 999")
        val after = table.state

        assert(after.rows == before.rows, s"a no-match DELETE changed the rows: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a no-match DELETE with a real predicate still commits one snapshot")
      }),
    deleteByPartitionPredicate,
    DmlTestCase(
      "delete.withAlias",
      s"DELETE FROM <table> AS x WHERE x.${Core.long0.columnName} < 2 resolves the alias, removes " +
        "the rows below key 2, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"DELETE FROM ${table.name} AS x WHERE x.${Core.long0.columnName} < 2")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(_.get(Core.long0) < 2L),
          s"rows after the DELETE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "DELETE through an alias commits one snapshot")
      }),
    DmlTestCase(
      "delete.whereFalse.noSnapshot",
      "DELETE WHERE false is optimized away: the rows stay as they are and no snapshot is committed.",
      table => {
        val before = table.state

        table.spark.sql(s"DELETE FROM ${table.name} WHERE false")
        val after = table.state

        assert(after.rows == before.rows, s"DELETE WHERE false changed the rows: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount,
          "DELETE WHERE false must not commit a snapshot")
      }),
    DmlTestCase(
      "delete.truncate",
      "TRUNCATE TABLE empties the table and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(s"TRUNCATE TABLE ${table.name}")
        val after = table.state

        assert(after.rows.isEmpty, s"rows survived TRUNCATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "TRUNCATE commits one snapshot")
      }),
    DmlTestCase(
      "delete.atSnapshot.rejected",
      "DELETE against a snapshot-pinned identifier is rejected with an IllegalArgumentException " +
        "naming that snapshot, and the table state stays exactly as prepared.",
      table => {
        val before = table.state
        val snapshotId = table.spark
          .sql(
            s"SELECT snapshot_id FROM ${table.name}.snapshots " +
              "ORDER BY committed_at DESC LIMIT 1")
          .collect()(0)
          .getLong(0)

        val exception = Check.intercept[IllegalArgumentException](
          table.spark.sql(
            s"DELETE FROM ${table.name}.snapshot_id_$snapshotId " +
              s"WHERE ${Core.long0.columnName} < 4"))
        val after = table.state

        assert(
          exception.getMessage ==
            s"Cannot delete from table at a specific snapshot: $snapshotId",
          s"unexpected rejection message: ${exception.getMessage}")
        assert(after == before, "a rejected DELETE leaves the rows and the snapshot count unchanged")
      }))

  private val updateTestCases: List[DmlTestCase[CoreTable.type]] = List(
    DmlTestCase(
      "update.byPredicate",
      s"UPDATE SET ${Core.string0.columnName} = 'X' WHERE ${Core.long0.columnName} = 2 rewrites " +
        "that column for key 2 only, leaves every other key's value alone, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            s"WHERE ${Core.long0.columnName} = 2")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "X") else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by a predicate commits one snapshot")
      }),
    DmlTestCase(
      "update.withoutCondition",
      s"UPDATE SET ${Core.string0.columnName} = 'Z' without a WHERE clause rewrites that column " +
        "for every row and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'Z'")
        val after = table.state

        assert(
          after.rows == before.rows.map(row => withColumnValue(row, Core.string0, "Z")),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "an unconditional UPDATE commits one snapshot")
      }),
    DmlTestCase(
      "update.noMatch",
      s"UPDATE ... WHERE ${Core.long0.columnName} = 99 matches no row, leaves every value as it " +
        "was, and still commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'Y' " +
            s"WHERE ${Core.long0.columnName} = 99")
        val after = table.state

        assert(
          after.rows == before.rows,
          s"a no-match UPDATE changed the rows: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a no-match UPDATE still commits one snapshot")
      }),
    DmlTestCase(
      "update.byInSubquery",
      s"UPDATE ... WHERE ${Core.long0.columnName} IN (subquery yielding 2) rewrites key 2 only and " +
        "commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            s"WHERE ${Core.long0.columnName} IN (" +
            "SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "X") else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by an IN subquery commits one snapshot")
      }),
    DmlTestCase(
      "update.byNotInSubquery",
      s"UPDATE ... WHERE ${Core.long0.columnName} NOT IN (subquery yielding 2) rewrites every key " +
        "other than 2 and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            s"WHERE ${Core.long0.columnName} NOT IN (" +
            "SELECT col1 FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) row else withColumnValue(row, Core.string0, "X")),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by a NOT IN subquery commits one snapshot")
      }),
    DmlTestCase(
      "update.byExistsSubquery",
      s"UPDATE ... WHERE EXISTS (correlated subquery matching ${Core.long0.columnName} = 2) " +
        "rewrites key 2 only and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            "WHERE EXISTS (SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) " +
            s"WHERE s.x = ${Core.long0.columnName})")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "X") else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by an EXISTS subquery commits one snapshot")
      }),
    DmlTestCase(
      "update.byNotExistsSubquery",
      s"UPDATE ... WHERE NOT EXISTS (correlated subquery matching ${Core.long0.columnName} = 2) " +
        "rewrites every key other than 2 and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            "WHERE NOT EXISTS (SELECT 1 FROM VALUES (CAST(2 AS BIGINT)) AS s(x) " +
            s"WHERE s.x = ${Core.long0.columnName})")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) row else withColumnValue(row, Core.string0, "X")),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by a NOT EXISTS subquery commits one snapshot")
      }),
    DmlTestCase(
      "update.byScalarSubquery",
      s"UPDATE ... WHERE ${Core.long0.columnName} = (scalar subquery yielding 2) rewrites key 2 " +
        "only and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X' " +
            s"WHERE ${Core.long0.columnName} = (" +
            "SELECT max(col1) FROM VALUES (CAST(2 AS BIGINT)) AS s(col1))")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "X") else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by a scalar subquery commits one snapshot")
      }),
    DmlTestCase(
      "update.withAlias",
      s"UPDATE <table> AS x SET x.${Core.string0.columnName} ... WHERE x.${Core.long0.columnName} " +
        "= 2 resolves the alias on both sides, rewrites key 2 only, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} AS x SET x.${Core.string0.columnName} = 'X' " +
            s"WHERE x.${Core.long0.columnName} = 2")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "X") else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE through an alias commits one snapshot")
      }),
    DmlTestCase(
      "update.multipleColumns",
      s"UPDATE SET ${Core.string0.columnName} = 'X', ${Core.int0.columnName} = 99 WHERE " +
        s"${Core.long0.columnName} = 2 rewrites both columns of key 2 in one statement and commits " +
        "one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'X', " +
            s"${Core.int0.columnName} = 99 WHERE ${Core.long0.columnName} = 2")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) {
              withColumnValue(withColumnValue(row, Core.string0, "X"), Core.int0, 99)
            } else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a multi-column UPDATE commits one snapshot")
      }),
    DmlTestCase(
      "update.byExpression",
      s"UPDATE SET ${Core.long0.columnName} = ${Core.long0.columnName} + 10 WHERE " +
        s"${Core.long0.columnName} = 2 moves key 2 to key 12, leaves the other keys alone, and " +
        "commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET " +
            s"${Core.long0.columnName} = ${Core.long0.columnName} + 10 " +
            s"WHERE ${Core.long0.columnName} = 2")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.long0, 12L) else row)),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "UPDATE by an expression commits one snapshot")
      }),
    DmlTestCase(
      "update.movePartition",
      s"UPDATE SET ${Core.datePartition.columnName} = '2099-12-31-23' WHERE " +
        s"${Core.long0.columnName} = 2 moves key 2 to another partition value, leaves the other " +
        "rows in their partitions, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET " +
            s"${Core.datePartition.columnName} = '2099-12-31-23' " +
            s"WHERE ${Core.long0.columnName} = 2")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) {
              withColumnValue(row, Core.datePartition, "2099-12-31-23")
            } else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a partition-moving UPDATE commits one snapshot")
      }),
    DmlTestCase(
      "update.nullAssignment",
      s"UPDATE SET ${Core.string0.columnName} = NULL WHERE ${Core.long0.columnName} = 2 stores a " +
        "null in that column for key 2 only and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = NULL " +
            s"WHERE ${Core.long0.columnName} = 2")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, null) else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "an UPDATE assigning null commits one snapshot")
      }))

  private val mergeTestCases: List[DmlTestCase[CoreTable.type]] = List(
    DmlTestCase(
      "merge.insertNotMatched",
      "MERGE with only a WHEN NOT MATCHED THEN INSERT * clause appends the two source rows (keys 4 " +
        "and 5) with every source column value, leaves the prepared rows exactly as they were, and " +
        "commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                  (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
                AS s($cols)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN INSERT *""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows ++ Seq(
            Row(4L, 4, "row-4", 4.5, true, "2024-01-04-03"),
            Row(5L, 5, "row-5", 5.5, false, "2024-01-05-04"))),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE that inserts commits one snapshot")
      }),
    DmlTestCase(
      "merge.updateMatched",
      "MERGE with only a WHEN MATCHED THEN UPDATE clause rewrites the matched key 2, leaves the " +
        "unmatched rows alone, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(2 AS BIGINT), 'M')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}""")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "M") else row),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE that updates commits one snapshot")
      }),
    DmlTestCase(
      "merge.deleteMatched",
      "MERGE with only a WHEN MATCHED THEN DELETE clause removes the matched keys 1 and 3, keeps " +
        "the unmatched rows, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(1 AS BIGINT)), (CAST(3 AS BIGINT))
                AS s(${Core.long0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN DELETE""")
        val after = table.state

        assert(
          after.rows == before.rows.filterNot(row => Set(1L, 3L)(row.get(Core.long0))),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE that deletes commits one snapshot")
      }),
    DmlTestCase(
      "merge.upsert",
      "MERGE with both an UPDATE clause and an INSERT clause rewrites the matched key 2 and appends " +
        "the unmatched key 7 in a single statement, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(2 AS BIGINT), 2, 'U', 2.5, true,  '2024-01-02-01'),
                  (CAST(7 AS BIGINT), 7, 'g', 7.5, false, '2024-01-07-06')
                AS s($cols)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}
              WHEN NOT MATCHED THEN INSERT *""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(
            before.rows.map(row =>
              if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "U") else row) :+
              Row(7L, 7, "g", 7.5, false, "2024-01-07-06")),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "an upsert MERGE commits one snapshot")
      }),
    DmlTestCase(
      "merge.deleteNotMatchedBySource",
      "MERGE with a WHEN NOT MATCHED BY SOURCE THEN DELETE clause removes every row the source does " +
        "not carry, keeps the matched key 2, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(2 AS BIGINT))
                AS s(${Core.long0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED BY SOURCE THEN DELETE""")
        val after = table.state

        assert(
          after.rows == before.rows.filter(_.get(Core.long0) == 2L),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a not-matched-by-source MERGE commits one snapshot")
      }),
    DmlTestCase(
      "merge.conditionalUpdate",
      "MERGE with a WHEN MATCHED AND <condition> THEN UPDATE clause rewrites only the matched row " +
        "that also satisfies the condition (key 2), leaves matched key 3 as it was, and commits one " +
        "snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(2 AS BIGINT), 'U2'),
                  (CAST(3 AS BIGINT), 'U3')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED AND s.${Core.long0.columnName} = 2 THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}""")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "U2") else row),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a conditional-update MERGE commits one snapshot")
      }),
    DmlTestCase(
      "merge.multipleMatchedClauses",
      "MERGE with two MATCHED clauses applies the first matching clause per row: key 2 is updated " +
        "by the conditional clause and key 3 falls through to the DELETE clause, in one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(2 AS BIGINT), 'U'),
                  (CAST(3 AS BIGINT), 'x')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED AND s.${Core.long0.columnName} = 2 THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}
              WHEN MATCHED THEN DELETE""")
        val after = table.state

        assert(
          after.rows == before.rows
            .filterNot(_.get(Core.long0) == 3L)
            .map(row =>
              if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "U") else row),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a multi-clause MERGE commits one snapshot")
      }),
    DmlTestCase(
      "merge.conditionalInsert",
      "MERGE with a WHEN NOT MATCHED AND <condition> THEN INSERT clause appends only the source row " +
        "that satisfies the condition (key 4), skips key 5, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                  (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
                AS s($cols)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED AND s.${Core.long0.columnName} = 4 THEN INSERT *""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(4L, 4, "row-4", 4.5, true, "2024-01-04-03")),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a conditional-insert MERGE commits one snapshot")
      }),
    DmlTestCase(
      "merge.allClauses",
      "MERGE carrying UPDATE, INSERT, and NOT MATCHED BY SOURCE DELETE clauses updates key 2, " +
        "inserts key 4, deletes the rows the source omits, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(2 AS BIGINT), 2, 'M2', 2.5, true,  '2024-01-02-01'),
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, false, '2024-01-04-03')
                AS s($cols)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}
              WHEN NOT MATCHED THEN INSERT *
              WHEN NOT MATCHED BY SOURCE THEN DELETE""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(
            before.rows
              .filter(_.get(Core.long0) == 2L)
              .map(row => withColumnValue(row, Core.string0, "M2")) :+
              Row(4L, 4, "row-4", 4.5, false, "2024-01-04-03")),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE with every clause commits one snapshot")
      }),
    DmlTestCase(
      "merge.updateStar",
      "MERGE with WHEN MATCHED THEN UPDATE SET * copies every source column onto the matched key 2, " +
        "leaves the unmatched rows exactly as prepared, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(2 AS BIGINT), 22, 'S2', 22.5, true, '2024-06-06-06')
                AS s($cols)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE SET *""")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) {
              Row(2L, 22, "S2", 22.5, true, "2024-06-06-06")
            } else row),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "an UPDATE SET * MERGE commits one snapshot")
      }),
    DmlTestCase(
      "merge.insertExplicitColumns",
      "MERGE whose INSERT clause names a column subset appends key 7 with the named values, leaves " +
        "the unnamed columns null, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(7 AS BIGINT), 'g')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN
              INSERT (${Core.long0.columnName}, ${Core.string0.columnName})
              VALUES (s.${Core.long0.columnName}, s.${Core.string0.columnName})""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(7L, null, "g", null, null, null)),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "an explicit-column MERGE insert commits one snapshot")
      }),
    DmlTestCase(
      "merge.sourceCTE",
      "MERGE whose source is a common table expression appends the key 8 that CTE yields, with " +
        "null in every column the CTE does not supply, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                WITH src AS (
                  SELECT CAST(8 AS BIGINT) AS ${Core.long0.columnName}
                )
                SELECT * FROM src
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN
              INSERT (${Core.long0.columnName}) VALUES (s.${Core.long0.columnName})""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(8L, null, null, null, null, null)),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE from a CTE source commits one snapshot")
      }),
    DmlTestCase(
      "merge.sourceSetOp",
      "MERGE whose source is a UNION ALL appends both keys the set operation yields (8 and 9), with " +
        "null in every column the source does not supply, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT CAST(8 AS BIGINT) AS ${Core.long0.columnName}
                UNION ALL
                SELECT CAST(9 AS BIGINT)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN
              INSERT (${Core.long0.columnName}) VALUES (s.${Core.long0.columnName})""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows ++ Seq(
            Row(8L, null, null, null, null, null),
            Row(9L, null, null, null, null, null))),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE from a set-operation source commits one snapshot")
      }),
    DmlTestCase(
      "merge.intoEmptyTarget",
      "After the table is emptied, MERGE with a NOT MATCHED INSERT clause inserts both source rows " +
        "(keys 4 and 5) into the empty target and commits one snapshot.",
      table => {
        table.spark.sql(s"DELETE FROM ${table.name}")
        val before = table.state

        assert(before.rows.isEmpty, s"precondition: the target is empty, got ${before.rows}")

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                  (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
                AS s($cols)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN INSERT *""")
        val after = table.state

        assert(
          after.rows == Seq(
            Row(4L, 4, "row-4", 4.5, true, "2024-01-04-03"),
            Row(5L, 5, "row-5", 5.5, false, "2024-01-05-04")),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE into an empty target commits one snapshot")
      }),
    DmlTestCase(
      "merge.nullJoinKey",
      "MERGE whose source carries a null join key matches no target row on that key: only the " +
        "non-null key 2 is updated, no row is added or removed, and one snapshot is committed.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES (CAST(NULL AS BIGINT), 'n'),
                  (CAST(2 AS BIGINT), 'M')
                AS s(${Core.long0.columnName}, ${Core.string0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN MATCHED THEN UPDATE
              SET t.${Core.string0.columnName} = s.${Core.string0.columnName}""")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) withColumnValue(row, Core.string0, "M") else row),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a MERGE with a null join key commits one snapshot")
      }),
    DmlTestCase(
      "merge.resolveByName",
      "MERGE with INSERT * resolves the source columns by name: key 7 lands with every source " +
        "value in its named column when the source lists its columns in another order, and one " +
        "snapshot is committed.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  ('g', CAST(7 AS BIGINT), 7, 7.5, false, '2024-07-07-07')
                AS s(
                  ${Core.string0.columnName},
                  ${Core.long0.columnName},
                  ${Core.int0.columnName},
                  ${Core.double0.columnName},
                  ${Core.boolean0.columnName},
                  datepartition)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN INSERT *""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(7L, 7, "g", 7.5, false, "2024-07-07-07")),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a name-resolved MERGE insert commits one snapshot")
      }))

  private val insertAndOverwriteTestCases: List[DmlTestCase[CoreTable.type]] = List(
    DmlTestCase(
      "insert.into",
      "INSERT INTO ... VALUES appends the two literal rows (keys 4 and 5), keeps the prepared rows, " +
        "and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""INSERT INTO ${table.name} VALUES
                (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows ++ Seq(
            Row(4L, 4, "row-4", 4.5, true, "2024-01-04-03"),
            Row(5L, 5, "row-5", 5.5, false, "2024-01-05-04"))),
          s"rows after the INSERT: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "INSERT INTO commits one snapshot")
      }),
    DmlTestCase(
      "insert.explicitColumns",
      "INSERT INTO naming a subset of the columns is rejected by the engine with a message naming " +
        "the omitted data, and the table state stays exactly as prepared.",
      table => {
        val before = table.state

        val exception = Check.intercept[Exception](
          table.spark.sql(
            s"INSERT INTO ${table.name} " +
              s"(${Core.long0.columnName}, ${Core.string0.columnName}) " +
              "VALUES (CAST(4 AS BIGINT), 'd'), (CAST(5 AS BIGINT), 'e')"))
        val after = table.state
        val exceptionMessage = Option(exception.getMessage).getOrElse("")

        assert(
          exceptionMessage.toUpperCase.contains("CANNOT_FIND_DATA") ||
            exceptionMessage.toUpperCase.contains("CANNOT FIND DATA") ||
            exceptionMessage.toUpperCase.contains("INCOMPATIBLE_DATA"),
          "expected a partial-INSERT rejection naming the omitted column " +
            s"(engine limitation), got: ${exceptionMessage.take(200)}")
        assert(after == before, "a rejected INSERT leaves the rows and the snapshot count unchanged")
      }),
    DmlTestCase(
      "insert.intoSelect",
      "INSERT INTO ... SELECT appends the row the SELECT produces (key 6), keeps the prepared rows, " +
        "and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"INSERT INTO ${table.name} SELECT * FROM VALUES " +
            s"(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05') " +
            s"AS s($cols)")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(6L, 6, "row-6", 6.5, true, "2024-01-06-05")),
          s"rows after the INSERT: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "INSERT INTO ... SELECT commits one snapshot")
      }),
    DmlTestCase(
      "append.dataFrame",
      "The DataFrame writeTo(...).append() path appends the frame's row (key 6), keeps the prepared " +
        "rows, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark
          .sql(
            s"SELECT * FROM VALUES " +
              s"(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05') " +
              s"AS s($cols)")
          .writeTo(table.name)
          .append()
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(6L, 6, "row-6", 6.5, true, "2024-01-06-05")),
          s"rows after the append: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a DataFrame append commits one snapshot")
      }),
    DmlTestCase(
      "insert.overwrite",
      "INSERT OVERWRITE ... VALUES replaces the table contents with the two literal rows (keys 1 " +
        "and 2) and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.sql(
          s"""INSERT OVERWRITE ${table.name} VALUES
                (CAST(1 AS BIGINT), 1, 'p', 1.5, false, '2024-01-01-00'),
                (CAST(2 AS BIGINT), 2, 'q', 2.5, true,  '2024-01-02-01')""")
        val after = table.state

        assert(
          after.rows == Seq(
            Row(1L, 1, "p", 1.5, false, "2024-01-01-00"),
            Row(2L, 2, "q", 2.5, true, "2024-01-02-01")),
          s"rows after the overwrite: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "INSERT OVERWRITE commits one snapshot")
      }),
    DmlTestCase(
      "overwrite.dataFrame",
      "The DataFrame writeTo(...).overwrite(lit(true)) path replaces every row with the frame's row " +
        "(key 8) and commits one snapshot.",
      table => {
        val before = table.state

        table.spark
          .sql(
            s"SELECT * FROM VALUES " +
              s"(CAST(8 AS BIGINT), 8, 'h', 8.5, false, '2024-01-08-07') " +
              s"AS s($cols)")
          .writeTo(table.name)
          .overwrite(lit(true))
        val after = table.state

        assert(
          after.rows == Seq(Row(8L, 8, "h", 8.5, false, "2024-01-08-07")),
          s"rows after the overwrite: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a DataFrame overwrite commits one snapshot")
      }))

  // Partition-scoped writes: they only mean something on a table that is partitioned, so they are
  // crossed with the partitioned preparations alone.
  val partitionedTableTestCases: List[DmlTestCase[CoreTable.type]] = List(
    DmlTestCase(
      "insert.dynamicOverwrite",
      "Under partitionOverwriteMode=dynamic, INSERT OVERWRITE with one row replaces only that row's " +
        "partition (2024-01-01-00), keeps the rows of every other partition, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        try {
          table.spark.sql(
            s"INSERT OVERWRITE ${table.name} VALUES " +
              "(CAST(10 AS BIGINT), 10, 'p', 10.5, true, '2024-01-01-00')")
        } finally {
          table.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        }
        val after = table.state

        assert(
          after.rows == inKeyOrder(
            before.rows.filterNot(_.get(Core.datePartition) == "2024-01-01-00") :+
              Row(10L, 10, "p", 10.5, true, "2024-01-01-00")),
          s"rows after the dynamic overwrite: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a dynamic partition overwrite commits one snapshot")
      }),
    DmlTestCase(
      "overwrite.partitions",
      "The DataFrame writeTo(...).overwritePartitions() path replaces only the partitions the frame " +
        "carries (2024-01-01-00), keeps the rows of every other partition, and commits one snapshot.",
      table => {
        val before = table.state

        table.spark
          .sql(
            s"SELECT * FROM VALUES " +
              "(CAST(10 AS BIGINT), 10, 'p', 10.5, true, '2024-01-01-00') " +
              s"AS s($cols)")
          .writeTo(table.name)
          .overwritePartitions()
        val after = table.state

        assert(
          after.rows == inKeyOrder(
            before.rows.filterNot(_.get(Core.datePartition) == "2024-01-01-00") :+
              Row(10L, 10, "p", 10.5, true, "2024-01-01-00")),
          s"rows after the partition overwrite: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a partition overwrite commits one snapshot")
      }))

  // --- which cases a preparation is compatible with ---
  // Compatibility is a property of the starting state, so each list names the states it fits.

  /** Every DML case. Runs on any preparation that starts from three seed rows of the seed shape. */
  val allDmlTestCases: List[DmlTestCase[CoreTable.type]] =
    readTestCases ++
      deleteTestCases ++
      updateTestCases ++
      mergeTestCases ++
      insertAndOverwriteTestCases

  /** The row-mutating cases: every DELETE, UPDATE and MERGE. */
  val rowMutationTestCases: List[DmlTestCase[CoreTable.type]] =
    deleteTestCases ++ updateTestCases ++ mergeTestCases

  /**
   * The cases that address columns by name and never write a whole seed-shaped row, so they run on
   * a preparation whose column list has grown beyond the seed rows.
   */
  val testCasesCompatibleWithAnAddedColumn: List[DmlTestCase[CoreTable.type]] =
    readTestCases ++ deleteTestCases ++ updateTestCases

  val orderedDmlTestCases: List[DmlTestCase[CoreTable.type]] =
    allDmlTestCases.map {
      case testCase if testCase == deleteByPartitionPredicate =>
        testCase.copy(knownBugReason = Some(
          "DELETE by partition predicate crashes in the Spark and Iceberg rewrite when the " +
            "table has a write order."))
      case testCase =>
        testCase
    }

  // --- standard preparations crossed with the cases they are compatible with ---

  val coreDmlCases: List[Plan.Case] =
    preparedCoreTables.flatMap(preparation => allDmlTestCases.map(_.runOn(preparation))) ++
      preparedNullStringCoreTables.flatMap(preparation =>
        nullStringRowTestCases.map(_.runOn(preparation)))

  val partitionedDmlCases: List[Plan.Case] =
    preparedPartitionedCoreTables.flatMap(preparation =>
      partitionedTableTestCases.map(_.runOn(preparation)))

  val orderedDmlCases: List[Plan.Case] =
    preparedOrderedCoreTables.flatMap(preparation => orderedDmlTestCases.map(_.runOn(preparation))) ++
      preparedNullStringOrderedCoreTables.flatMap(preparation =>
        nullStringRowTestCases.map(_.runOn(preparation)))

  val evolvedDmlCases: List[Plan.Case] =
    preparedEvolvedCoreTables.flatMap(preparation =>
      testCasesCompatibleWithAnAddedColumn.map(_.runOn(preparation)))

  // --- DDL consumers: a DDL evolves the table, then operations are run against it ---

  // Each preparation is one layout evolved by one DDL. A consumer case then runs an operation
  // against the evolved table. Plan walks this list so every consumer family lands on the same
  // preparation before the next preparation starts.
  val ddlConsumerPreparations: List[TablePreparation[CoreTable.type]] =
    parquetAndOrcLayouts.flatMap { layout =>
      List(
        TablePreparation(
          layout.label,
          createAndSeed(layout, 3)
            .sql("ddl")(table => s"ALTER TABLE $table ADD COLUMN cc int")(),
          "ddlConsume:addColumn.",
          description = s"Three seed rows with keys 1, 2 and 3 in ${layout.description}, then " +
            "ADD COLUMN cc int, so the table carries an added column the seed rows read as null."),
        TablePreparation(
          layout.label,
          createAndSeed(layout, 3)
            .sql("ddl")(table =>
              s"ALTER TABLE $table ALTER COLUMN ${Core.int0.columnName} TYPE bigint")(),
          "ddlConsume:typeWiden.",
          description = s"Three seed rows with keys 1, 2 and 3 in ${layout.description}, then " +
            s"${Core.int0.columnName} widened from int to bigint."),
        TablePreparation(
          layout.label,
          createAndSeed(layout, 3)
            .sql("ddl")(table =>
              s"ALTER TABLE $table WRITE ORDERED BY ${Core.long0.columnName}")(),
          "ddlConsume:writeOrder.",
          description = s"Three seed rows with keys 1, 2 and 3 in ${layout.description}, then " +
            s"WRITE ORDERED BY ${Core.long0.columnName}, so the table carries that write sort order."),
        TablePreparation(
          layout.label,
          createAndSeed(layout, 3)
            .sql("ddl")(table =>
              s"ALTER TABLE $table SET TBLPROPERTIES " +
                "('write.distribution-mode'='range')")(),
          "ddlConsume:distMode.",
          description = s"Three seed rows with keys 1, 2 and 3 in ${layout.description}, then " +
            "write.distribution-mode set to range, so writes are range distributed."))
    }

  // The reads and writes a consumer runs against the evolved table.
  def ddlConsumerDataCases(
      preparation: TablePreparation[CoreTable.type]): List[Plan.Case] =
    List(
      preparation.test(
        "dmlWrite",
        "A plain INSERT still lands on the table after the DDL, taking it to four rows.") { table =>
        table.spark.sql(
          s"INSERT INTO ${table.name} SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} = 1")

        assert(
          table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0) == 4,
          "table is not writable after DDL")
      },
      preparation.test(
        "dmlMutate",
        "A row-level DELETE still lands on the table after the DDL, taking it to two rows.") { table =>
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 2")

        assert(
          table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0) == 2,
          "mutation failed after DDL")
      },
      preparation.test(
        "timeTravel",
        "The seed snapshot from before the DDL is still readable through VERSION AS OF and " +
          "returns its three rows.") { table =>
        val seedSnapshotId =
          snapshotIds(table.spark, table.name).head

        assert(
          table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name} " +
                s"VERSION AS OF $seedSnapshotId")
            .collect()(0)
            .getLong(0) == 3,
          "seed snapshot is not readable after DDL")
      },
      preparation.test(
        "restore",
        "rollback_to_snapshot back to the seed snapshot undoes an INSERT made after the DDL " +
          "and returns the table to its three seed rows.") { table =>
        val seedSnapshotId =
          snapshotIds(table.spark, table.name).head

        table.spark.sql(
          s"INSERT INTO ${table.name} SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} = 1")
        table.spark.sql(
          "CALL openhouse.system.rollback_to_snapshot(" +
            s"'${catalogRelative(table.name)}', $seedSnapshotId)")

        assert(
          table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0) == 3,
          "restore across DDL failed")
      },
      preparation.test(
        "expire",
        "expire_snapshots retaining only the newest snapshot leaves the table readable with " +
          "its four current rows.") { table =>
        table.spark.sql(
          s"INSERT INTO ${table.name} SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} = 1")
        table.spark.sql(
          "CALL openhouse.system.expire_snapshots(" +
            s"table => '${catalogRelative(table.name)}', " +
            "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
            "retain_last => 1)")

        assert(
          table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0) == 4,
          "table is unreadable after snapshot expiration")
      })

  // Compaction run against the files written across the DDL.
  def ddlConsumerCompactionCases(
      preparation: TablePreparation[CoreTable.type]): List[Plan.Case] =
    List(
      preparation.test(
        "compact",
        "rewrite_data_files compacts the files written across the DDL and preserves the four " +
          "current rows.") { table =>
        table.spark.sql(
          s"INSERT INTO ${table.name} SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} = 1")
        table.spark.sql(
          "CALL openhouse.system.rewrite_data_files(" +
            s"table => '${catalogRelative(table.name)}', " +
            "options => map('min-input-files', '2'))")

        assert(
          table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0) == 4,
          "compaction changed rows after DDL")
      })

  // --- DDL that changes the schema of a seeded table ---

  val createSchemaCases: List[Plan.Case] = preparedEmptyCoreTables.map { preparation =>
    preparation.test(
      "create.schema",
      "The created table's schema is exactly CoreTable's columns, in declaration order and with " +
        "their declared types, and the table holds no rows.") { table =>
      val actual = table.spark
        .table(table.name)
        .schema
        .fields
        .toList
        .map(field => field.name -> field.dataType.simpleString)
      val expected = Core.tableColumns.toList.map(column => (column.columnName, column.sqlType))

      assert(actual == expected, s"schema is $actual")
      assert(table.rows.isEmpty, "a table that was never seeded holds no rows")
    }
  }

  val ddlSchemaCases: List[Plan.Case] = preparedCoreTables.flatMap { preparation =>
    List(
      preparation.test(
        "ddl.addColumn.single",
        "ADD COLUMN adds the column to the schema, the existing rows read null for it, and the row " +
          "count is unchanged.") { table =>
        table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMN added_int int")

        val columnNames = table.spark.table(table.name).schema.fields.toSeq.map(_.name)
        val nullCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name} WHERE added_int IS NULL")
          .collect()(0)
          .getLong(0)

        assert(columnNames.contains("added_int"), s"added_int missing: $columnNames")
        assert(
          nullCount == table.preparedRows.size,
          s"existing rows should read null for added_int: $nullCount != ${table.preparedRows.size}")
        assert(table.rows.size == table.preparedRows.size, "ADD COLUMN changed the row count")
      },
      preparation.test(
        "ddl.addColumn.multiple",
        "ADD COLUMNS with two columns in one statement adds both to the schema and leaves the row " +
          "count unchanged.") { table =>
        table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMNS (added_a int, added_b string)")

        val columnNames = table.spark.table(table.name).schema.fields.toSeq.map(_.name)

        assert(
          columnNames.contains("added_a") && columnNames.contains("added_b"),
          s"added columns missing: $columnNames")
        assert(table.rows.size == table.preparedRows.size, "ADD COLUMNS changed the row count")
      },
      preparation.test(
        "ddl.addColumn.comment",
        "ADD COLUMN ... COMMENT stores the comment on the added column and the reader sees it.") { table =>
        table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMN added_c int COMMENT 'a note'")

        val addedColumn = table.spark
          .table(table.name)
          .schema
          .fields
          .find(_.name == "added_c")
          .getOrElse(throw new AssertionError("added_c missing"))

        assert(
          addedColumn.getComment().contains("a note"),
          s"comment not stored: ${addedColumn.getComment()}")
      },
      preparation.test(
        "ddl.addColumn.position",
        s"ADD COLUMN ... AFTER ${Core.long0.columnName} places the added column directly after that " +
          "column in the schema.") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} ADD COLUMN added_after int AFTER ${Core.long0.columnName}")

        val columnNames = table.spark.table(table.name).schema.fields.toSeq.map(_.name)

        assert(
          columnNames.indexOf("added_after") == columnNames.indexOf(Core.long0.columnName) + 1,
          s"added_after not after long0: $columnNames")
      },
      preparation.test(
        "ddl.alterColumn.typeWiden",
        s"ALTER COLUMN ${Core.int0.columnName} TYPE bigint widens the column in the schema and the " +
          "already-written values read back unchanged.") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} ALTER COLUMN ${Core.int0.columnName} TYPE bigint")

        val liveColumns = table.spark.table(table.name).schema.fields.toSeq
          .map(field => field.name -> field.dataType.simpleString)
          .toMap
        val values = table.spark
          .sql(
            s"SELECT ${Core.int0.columnName} FROM ${table.name} ORDER BY ${Core.long0.columnName}")
          .collect()
          .toSeq
          .map(_.getLong(0))

        assert(
          liveColumns.get(Core.int0.columnName).contains("bigint"),
          s"int0 not widened: ${liveColumns.get(Core.int0.columnName)}")
        assert(values == Seq(1L, 2L, 3L), s"values not preserved after widening: $values")
      },
      preparation
        .test(
          "ddl.renameColumn",
          "RENAME COLUMN renames the column in the schema: the new name is present, the old name is " +
            "gone, and the row count is unchanged.") { table =>
          table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMN to_rename int")
          table.spark.sql(s"ALTER TABLE ${table.name} RENAME COLUMN to_rename TO renamed_col")

          val columnNames = table.spark.table(table.name).schema.fields.toSeq.map(_.name)

          assert(
            columnNames.contains("renamed_col") && !columnNames.contains("to_rename"),
            s"RENAME COLUMN silently no-oped: $columnNames")
          assert(table.rows.size == table.preparedRows.size, "RENAME COLUMN changed the row count")
        }
        .copy(knownBugReason = Some(
          "RENAME COLUMN is a silent no-op because server-side schema casing normalization " +
            "restores the old name.")))
  }

}
