package harness

import org.apache.spark.sql.Row

/**
 * Merge operations over the core table. Each case verifies the rows selected by its clauses and the single snapshot
 * committed by the statement.
 */
trait ScenarioDmlMerge extends ScenarioKit {
  import Rows._

  /**
   * The MERGE operations. Their INSERT clauses write a whole seed-shaped row, so they run on a preparation whose column
   * list is still the seed shape.
   */
  protected lazy val mergeTestCases: List[DmlTestCase[CoreTable.type]] = List(
    mergeInsertNotMatched,
    mergeUpdateMatched,
    mergeDeleteMatched,
    mergeUpsert,
    mergeDeleteNotMatchedBySource,
    mergeConditionalUpdate,
    mergeMultipleMatchedClauses,
    mergeConditionalInsert,
    mergeAllClauses,
    mergeUpdateStar,
    mergeInsertExplicitColumns,
    mergeSourceCTE,
    mergeSourceSetOp,
    mergeIntoEmptyTarget,
    mergeNullJoinKey,
    mergeResolveByName)

  /**
   * MERGE with only a WHEN NOT MATCHED THEN INSERT * clause appends the two source rows (keys 4 and 5) with every
   * source column value, leaves the prepared rows unchanged, and commits one snapshot.
   */
  private val mergeInsertNotMatched: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.insertNotMatched",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                  (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
                AS s($columnNameList)
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
      })

  /**
   * MERGE with only a WHEN MATCHED THEN UPDATE clause rewrites the matched key 2, leaves the unmatched rows unchanged,
   * and commits one snapshot.
   */
  private val mergeUpdateMatched: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.updateMatched",
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
      })

  /**
   * MERGE with only a WHEN MATCHED THEN DELETE clause removes the matched keys 1 and 3, keeps the unmatched rows, and
   * commits one snapshot.
   */
  private val mergeDeleteMatched: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.deleteMatched",
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
      })

  /**
   * MERGE with both an UPDATE clause and an INSERT clause rewrites the matched key 2 and appends the unmatched key 7 in
   * a single statement, and commits one snapshot.
   */
  private val mergeUpsert: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.upsert",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(2 AS BIGINT), 2, 'U', 2.5, true,  '2024-01-02-01'),
                  (CAST(7 AS BIGINT), 7, 'g', 7.5, false, '2024-01-07-06')
                AS s($columnNameList)
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
      })

  /**
   * MERGE with a WHEN NOT MATCHED BY SOURCE THEN DELETE clause removes every row the source does not carry, keeps the
   * matched key 2, and commits one snapshot.
   */
  private val mergeDeleteNotMatchedBySource: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.deleteNotMatchedBySource",
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
      })

  /**
   * MERGE with a WHEN MATCHED AND <condition> THEN UPDATE clause rewrites only the matched row that also satisfies the
   * condition (key 2), leaves matched key 3 unchanged, and commits one snapshot.
   */
  private val mergeConditionalUpdate: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.conditionalUpdate",
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
      })

  /**
   * MERGE with two MATCHED clauses applies the first matching clause per row: key 2 is updated by the conditional
   * clause and key 3 falls through to the DELETE clause, in one snapshot.
   */
  private val mergeMultipleMatchedClauses: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.multipleMatchedClauses",
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
      })

  /**
   * MERGE with a WHEN NOT MATCHED AND <condition> THEN INSERT clause appends only the source row that satisfies the
   * condition (key 4), skips key 5, and commits one snapshot.
   */
  private val mergeConditionalInsert: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.conditionalInsert",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                  (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
                AS s($columnNameList)
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED AND s.${Core.long0.columnName} = 4 THEN INSERT *""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(4L, 4, "row-4", 4.5, true, "2024-01-04-03")),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a conditional-insert MERGE commits one snapshot")
      })

  /**
   * MERGE carrying UPDATE, INSERT, and NOT MATCHED BY SOURCE DELETE clauses updates key 2, inserts key 4, deletes the
   * rows the source omits, and commits one snapshot.
   */
  private val mergeAllClauses: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.allClauses",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(2 AS BIGINT), 2, 'M2', 2.5, true,  '2024-01-02-01'),
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, false, '2024-01-04-03')
                AS s($columnNameList)
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
      })

  /**
   * MERGE with WHEN MATCHED THEN UPDATE SET * copies every source column onto the matched key 2, leaves the unmatched
   * rows unchanged, and commits one snapshot.
   */
  private val mergeUpdateStar: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.updateStar",
      table => {
        val before = table.state

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(2 AS BIGINT), 22, 'S2', 22.5, true, '2024-06-06-06')
                AS s($columnNameList)
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
      })

  /**
   * MERGE whose INSERT clause names a column subset appends key 7 with the named values, leaves the unnamed columns
   * null, and commits one snapshot.
   */
  private val mergeInsertExplicitColumns: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.insertExplicitColumns",
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
      })

  /**
   * MERGE whose source is a common table expression appends the key 8 that CTE yields, with null in every column the
   * CTE does not supply, and commits one snapshot.
   */
  private val mergeSourceCTE: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.sourceCTE",
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
      })

  /**
   * MERGE whose source is a UNION ALL appends both keys the set operation yields (8 and 9), with null in every column
   * the source does not supply, and commits one snapshot.
   */
  private val mergeSourceSetOp: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.sourceSetOp",
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
      })

  /**
   * After the table is emptied, MERGE with a NOT MATCHED INSERT clause inserts both source rows (keys 4 and 5) into the
   * empty target and commits one snapshot.
   */
  private val mergeIntoEmptyTarget: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.intoEmptyTarget",
      table => {
        table.spark.sql(s"DELETE FROM ${table.name}")
        val before = table.state

        assert(before.rows.isEmpty, s"precondition: the target is empty, got ${before.rows}")

        table.spark.sql(
          s"""MERGE INTO ${table.name} t USING (
                SELECT * FROM VALUES
                  (CAST(4 AS BIGINT), 4, 'row-4', 4.5, true,  '2024-01-04-03'),
                  (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')
                AS s($columnNameList)
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
      })

  /**
   * MERGE whose source carries a null join key matches no target row on that key: only the non-null key 2 is updated,
   * no row is added or removed, and one snapshot is committed.
   */
  private val mergeNullJoinKey: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.nullJoinKey",
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
      })

  /**
   * MERGE with INSERT * resolves the source columns by name: key 7 lands with every source value in its named column
   * when the source lists its columns in another order, and one snapshot is committed.
   */
  private val mergeResolveByName: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "merge.resolveByName",
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
                  ${Core.date0.columnName})
              ) s ON t.${Core.long0.columnName} = s.${Core.long0.columnName}
              WHEN NOT MATCHED THEN INSERT *""")
        val after = table.state

        assert(
          after.rows == inKeyOrder(before.rows :+ Row(7L, 7, "g", 7.5, false, "2024-07-07-07")),
          s"rows after the MERGE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a name-resolved MERGE insert commits one snapshot")
      })
}
