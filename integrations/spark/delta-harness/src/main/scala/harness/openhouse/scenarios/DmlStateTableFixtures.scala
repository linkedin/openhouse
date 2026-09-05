package harness

/**
 * Ordered and evolved table preparations used by the DML state matrix.
 */
trait DmlStateTableFixtures extends DmlTableFixtures {

  /** Unpartitioned and date-partitioned core layouts in each file format. */
  private lazy val stateMatrixCoreLayouts: List[Layout] =
    fileFormats.flatMap(format =>
      List(
        coreLayout(s"unpartitioned/$format", format, ""),
        coreLayout(
          s"partitioned/$format",
          format,
          s"PARTITIONED BY (${Core.date0.columnName})")))

  /** Standard seeded preparations carrying a write order on the long key. */
  lazy val preparedOrderedCoreTables: List[TablePreparation[CoreTable.type]] =
    stateMatrixCoreLayouts.map(layout =>
      TablePreparation(
        layout.label,
        createCoreTable(layout)
          .insert(standardSeedRowCount)()
          .sql("writeOrderedByLongKey")(table =>
            s"ALTER TABLE $table WRITE ORDERED BY ${Core.long0.columnName}")(),
        "prep.ordered:"))

  /** Standard seeded preparations carrying one added nullable column. */
  lazy val preparedEvolvedCoreTables: List[TablePreparation[CoreTable.type]] =
    stateMatrixCoreLayouts.map(layout =>
      TablePreparation(
        layout.label,
        createCoreTable(layout)
          .insert(standardSeedRowCount)()
          .sql("addPrepExtraColumn")(table => s"ALTER TABLE $table ADD COLUMN prep_extra int")(),
        "prep.evolved:"))

  /** The write-ordered preparations carrying one row whose string column is null. */
  lazy val preparedNullStringOrderedCoreTables: List[TablePreparation[CoreTable.type]] =
    preparedOrderedCoreTables.map(withNullStringRow)
}
