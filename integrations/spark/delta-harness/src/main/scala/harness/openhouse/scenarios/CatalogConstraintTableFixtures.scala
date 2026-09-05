package harness

import org.apache.spark.sql.SparkSession

/**
 * Table fixtures shared by file-format, partition-evolution, and table-property scenarios.
 */
trait CatalogConstraintTableFixtures extends TableTestFixtures {

  /** Unpartitioned and date-partitioned core layouts in each file format. */
  lazy val catalogConstraintCoreLayouts: List[Layout] =
    fileFormats.flatMap(format =>
      List(
        coreLayout(s"unpartitioned/$format", format, ""),
        coreLayout(
          s"partitioned/$format",
          format,
          s"PARTITIONED BY (${Core.date0.columnName})")))

  /** One standard seeded preparation per catalog-constraint layout. */
  lazy val preparedCoreTables: List[TablePreparation[CoreTable.type]] =
    catalogConstraintCoreLayouts.map(layout =>
      TablePreparation(
        layout.label,
        createCoreTable(layout).insert(standardSeedRowCount)()))

  /** The catalog-constraint preparations with a write sort order on the long key. */
  lazy val preparedOrderedCoreTables: List[TablePreparation[CoreTable.type]] =
    catalogConstraintCoreLayouts.map(layout =>
      TablePreparation(
        layout.label,
        createCoreTable(layout)
          .insert(standardSeedRowCount)()
          .sql("writeOrderedByLongKey")(table =>
            s"ALTER TABLE $table WRITE ORDERED BY ${Core.long0.columnName}")(),
        "prep.ordered:"))

  protected def tableProperties(spark: SparkSession, table: String): Map[String, String] =
    spark.sql(s"SHOW TBLPROPERTIES $table").collect().toSeq
      .map(row => row.getString(0) -> row.getString(1))
      .toMap
}
