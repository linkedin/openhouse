package harness

/**
 * Table preparations used by the complete DML operation catalog.
 */
trait DmlTableFixtures extends TableTestFixtures {

  /** One date-partitioned core preparation per file format. */
  lazy val preparedPartitionedCoreTables: List[TablePreparation[CoreTable.type]] =
    fileFormats.map { format =>
      val layout = coreLayout(
        s"partitioned/$format",
        format,
        s"PARTITIONED BY (${Core.date0.columnName})")
      TablePreparation(
        layout.label,
        createCoreTable(layout).insert(standardSeedRowCount)())
    }

  /**
   * Extends a preparation with one row whose string column is null.
   */
  protected def withNullStringRow(
      basePreparation: TablePreparation[CoreTable.type]
  ): TablePreparation[CoreTable.type] =
    basePreparation.copy(
      preparation = basePreparation.preparation.sql("prep.nullStringRow")(table =>
        s"INSERT INTO $table VALUES (CAST(99 AS BIGINT), 99, NULL, 99.5, false, '2024-01-01-00')")())
}
