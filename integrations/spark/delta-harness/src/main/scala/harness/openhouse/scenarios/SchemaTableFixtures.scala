package harness

import org.apache.spark.sql.SparkSession

/**
 * Table fixtures shared by nested-type and schema-evolution scenarios.
 */
trait SchemaTableFixtures extends TableTestFixtures {

  /** Unpartitioned and date-partitioned core layouts in each file format. */
  lazy val schemaCoreLayouts: List[Layout] =
    fileFormats.flatMap(format =>
      List(
        coreLayout(s"unpartitioned/$format", format, ""),
        coreLayout(
          s"partitioned/$format",
          format,
          s"PARTITIONED BY (${Core.date0.columnName})")))

  /** One empty preparation per schema layout. */
  lazy val preparedEmptyCoreTables: List[TablePreparation[CoreTable.type]] =
    schemaCoreLayouts.map(layout =>
      TablePreparation(layout.label, createCoreTable(layout)))

  /** One standard seeded preparation per schema layout. */
  lazy val preparedCoreTables: List[TablePreparation[CoreTable.type]] =
    schemaCoreLayouts.map(layout =>
      TablePreparation(
        layout.label,
        createCoreTable(layout).insert(standardSeedRowCount)()))

  /** Runs a side-table operation and drops the table after the operation completes. */
  protected def withOwnedTable(runStatement: String => Unit, table: String)(
      create: => Unit)(use: => Unit): Unit =
    OwnedTableLifecycle.withOwnership(runStatement(s"DROP TABLE IF EXISTS $table")) {
      markTableCreated =>
        create
        markTableCreated()
        use
    }

  protected def countOf(spark: SparkSession, sql: String): String =
    spark.sql(sql).collect()(0).getLong(0).toString

  protected val extraColumnRowNine =
    "(CAST(9 AS BIGINT), 9, 'row-9', 9.5, true, '2024-01-09-01', 42)"
  protected val extraColumnRowTen =
    "(CAST(10 AS BIGINT), 10, 'row-10', 10.5, true, '2024-01-10-01', 43)"
}
