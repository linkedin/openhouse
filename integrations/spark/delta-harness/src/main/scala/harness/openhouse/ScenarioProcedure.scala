package harness

/**
 * Catalog procedures and catalog-level statements: which of them this catalog implements, and which it rejects.
 *
 * Operations: ancestors_of over a two-snapshot history; register_table onto a new name from an existing metadata file,
 * followed by the rejected system.snapshot and system.add_files import procedures; and the rejected CREATE VIEW and
 * ANALYZE TABLE COMPUTE STATISTICS statements.
 *
 * Preparation axes: in each of the two columnar formats, the two-snapshot core table for the ancestry family and the
 * standard seeded core table for the import and statement families.
 *
 * Case families: three families contributing 6 cases.
 */
trait ScenarioProcedure extends ScenarioKit {

  /** Every catalog-procedure case, one file format at a time. */
  lazy val procedureCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        ancestorsOfCase(preparedTwoSnapshotTable(format)),
        registerTableCase(preparedStandardTable(format)),
        viewAndAnalyzeRejectedCase(preparedStandardTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** ancestors_of lists both snapshots of the table's two-snapshot history. */
  private def ancestorsOfCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("procedure.ancestorsOf") { table =>
      val ancestorCount = table.spark
        .sql(
          "CALL openhouse.system.ancestors_of(" +
            s"table => '${catalogRelative(table.name)}')")
        .collect()
        .length

      assert(
        ancestorCount == 2,
        s"ancestors_of should list two snapshots, got $ancestorCount")
    }

  /**
   * register_table onto a new name makes the source table's snapshot readable there (3 rows) and leaves the source
   * unchanged, and dropping the registered table leaves the source unchanged. The system.snapshot and system.add_files
   * procedures each reject their unsupported inputs with an exception.
   *
   * The drop of the registered table is both its cleanup and the operation the source assertion after the ownership
   * boundary depends on, so it runs as that boundary's cleanup. A drop the catalog refuses fails the case, so the case
   * cannot pass while leaving the registration behind. The snapshot target extends the prepared table's generated
   * name, and its own boundary removes it whether the procedure was rejected as expected, threw something else, or
   * unexpectedly succeeded.
   */
  private def registerTableCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("procedure.registerTable") { table =>
      val registeredTable = s"${table.name}_registered"
      val snapshotTarget = s"${table.name}_snapshotTarget"
      val absentSourceDirectory = s"/tmp/${table.name.split('.').last}_absentSource"
      val metadataFile = table.spark
        .sql(
          s"SELECT file FROM ${table.name}.metadata_log_entries " +
            "ORDER BY timestamp DESC LIMIT 1")
        .collect()(0)
        .getString(0)

      withOwnedTable(table.spark.sql(_), registeredTable)(
        table.spark.sql(
          "CALL openhouse.system.register_table(" +
            s"table => '${catalogRelative(registeredTable)}', " +
            s"metadata_file => '$metadataFile')")) {
        assert(
          countOf(table.spark, s"SELECT count(*) FROM $registeredTable") == "3",
          "register_table should make all source rows readable")
      }
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "3",
        "dropping the registered table should leave the source rows in place")

      withCleanupStatement(table.spark.sql(_), s"DROP TABLE IF EXISTS $snapshotTarget") {
        Check.intercept[Exception](
          table.spark.sql(
            "CALL openhouse.system.snapshot(" +
              s"source_table => '${catalogRelative(table.name)}', " +
              s"table => '${catalogRelative(snapshotTarget)}')"))
      }

      Check.intercept[Exception](
        table.spark.sql(
          "CALL openhouse.system.add_files(" +
            s"table => '${catalogRelative(table.name)}', " +
            s"source_table => '`parquet`.`$absentSourceDirectory`')"))
    }

  /**
   * CREATE VIEW and ANALYZE TABLE COMPUTE STATISTICS are each rejected with an exception. The view name extends the
   * prepared table's generated name, and its boundary removes the view whether the statement was rejected as expected,
   * threw something else, or unexpectedly succeeded.
   */
  private def viewAndAnalyzeRejectedCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("procedure.viewAndAnalyze.rejected") { table =>
      val viewName = s"${table.name}_view"

      withCleanupStatement(table.spark.sql(_), s"DROP VIEW IF EXISTS $viewName") {
        Check.intercept[Exception](
          table.spark.sql(s"CREATE VIEW $viewName AS SELECT 1 AS one"))
      }

      Check.intercept[Exception](
        table.spark.sql(
          s"ANALYZE TABLE ${table.name} COMPUTE STATISTICS"))
    }

}
