package harness

/**
 * Update operations over the core table. Each case verifies the rewritten column values and the snapshot committed by
 * its SQL form.
 */
trait ScenarioDmlUpdate extends TableTestFixtures {
  import Rows._

  /**
   * The UPDATE operations. They assign columns by name, so they run on any preparation that starts from the three seed
   * rows, including one whose column list has grown past that shape.
   */
  protected lazy val updateTestCases: List[DmlTestCase[CoreTable.type]] = List(
    updateByPredicate,
    updateWithoutCondition,
    updateNoMatch,
    updateByInSubquery,
    updateByNotInSubquery,
    updateByExistsSubquery,
    updateByNotExistsSubquery,
    updateByScalarSubquery,
    updateWithAlias,
    updateMultipleColumns,
    updateByExpression,
    updateMovePartition,
    updateNullAssignment)

  /**
   * UPDATE SET foo_col_string = 'X' WHERE foo_col_long = 2 rewrites that column for key 2 only, leaves every other row
   * unchanged, and commits one snapshot.
   */
  private val updateByPredicate: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byPredicate",
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
      })

  /**
   * UPDATE SET foo_col_string = 'Z' without a WHERE clause rewrites that column for every row and commits one snapshot.
   */
  private val updateWithoutCondition: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.withoutCondition",
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
      })

  /** UPDATE ... WHERE foo_col_long = 99 matches no row, leaves every row unchanged, and still commits one snapshot. */
  private val updateNoMatch: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.noMatch",
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
      })

  /** UPDATE ... WHERE foo_col_long IN (subquery yielding 2) rewrites key 2 only and commits one snapshot. */
  private val updateByInSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byInSubquery",
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
      })

  /**
   * UPDATE ... WHERE foo_col_long NOT IN (subquery yielding 2) rewrites every key other than 2 and commits one
   * snapshot.
   */
  private val updateByNotInSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byNotInSubquery",
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
      })

  /**
   * UPDATE ... WHERE EXISTS (correlated subquery matching foo_col_long = 2) rewrites key 2 only and commits one
   * snapshot.
   */
  private val updateByExistsSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byExistsSubquery",
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
      })

  /**
   * UPDATE ... WHERE NOT EXISTS (correlated subquery matching foo_col_long = 2) rewrites every key other than 2 and
   * commits one snapshot.
   */
  private val updateByNotExistsSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byNotExistsSubquery",
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
      })

  /** UPDATE ... WHERE foo_col_long = (scalar subquery yielding 2) rewrites key 2 only and commits one snapshot. */
  private val updateByScalarSubquery: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byScalarSubquery",
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
      })

  /**
   * UPDATE <table> AS x SET x.foo_col_string ... WHERE x.foo_col_long = 2 resolves the alias on both sides, rewrites
   * key 2 only, and commits one snapshot.
   */
  private val updateWithAlias: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.withAlias",
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
      })

  /**
   * UPDATE SET foo_col_string = 'X', foo_col_int = 99 WHERE foo_col_long = 2 rewrites both columns of key 2 in one
   * statement and commits one snapshot.
   */
  private val updateMultipleColumns: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.multipleColumns",
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
      })

  /**
   * UPDATE SET foo_col_long = foo_col_long + 10 WHERE foo_col_long = 2 moves key 2 to key 12, leaves every other row
   * unchanged, and commits one snapshot.
   */
  private val updateByExpression: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.byExpression",
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
      })

  /**
   * UPDATE SET foo_col_date = '2099-12-31-23' WHERE foo_col_long = 2 moves key 2 to another date partition value,
   * leaves every other row unchanged, and commits one snapshot.
   */
  private val updateMovePartition: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.movePartition",
      table => {
        val before = table.state

        table.spark.sql(
          s"UPDATE ${table.name} SET " +
            s"${Core.date0.columnName} = '2099-12-31-23' " +
            s"WHERE ${Core.long0.columnName} = 2")
        val after = table.state

        assert(
          after.rows == before.rows.map(row =>
            if (row.get(Core.long0) == 2L) {
              withColumnValue(row, Core.date0, "2099-12-31-23")
            } else row),
          s"rows after the UPDATE: ${after.rows}")
        assert(
          after.snapshotCount == before.snapshotCount + 1,
          "a partition-moving UPDATE commits one snapshot")
      })

  /**
   * UPDATE SET foo_col_string = NULL WHERE foo_col_long = 2 stores a null in that column for key 2 only and commits one
   * snapshot.
   */
  private val updateNullAssignment: DmlTestCase[CoreTable.type] =
    DmlTestCase(
      "update.nullAssignment",
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
      })
}
