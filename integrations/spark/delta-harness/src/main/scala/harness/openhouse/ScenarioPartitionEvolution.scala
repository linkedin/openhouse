package harness

/**
 * Partition evolution: changing the partition specification of an existing table.
 *
 * Operations: ALTER TABLE ADD PARTITION FIELD on an unpartitioned table and ALTER TABLE DROP PARTITION FIELD on a
 * date-partitioned table. The catalog rejects both, so recreating the table is the way to change its partitioning.
 *
 * Preparation axes: in each of the two columnar formats, the standard seeded core table for the add case and a
 * date-partitioned core table seeded with the standard rows for the drop case.
 *
 * Case families: two families contributing 4 cases.
 */
trait ScenarioPartitionEvolution extends ScenarioKit {

  /** The rejected partition-evolution statements, one file format at a time. */
  lazy val partitionEvolutionCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        addPartitionFieldRejectedCase(format),
        dropPartitionFieldRejectedCase(format))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * ALTER TABLE ADD PARTITION FIELD on an unpartitioned table is rejected with an exception stating that evolution of
   * table partitioning is unsupported.
   */
  private def addPartitionFieldRejectedCase(format: String): Plan.Case =
    preparedStandardTable(format).test("partitionEvolution.add.rejected") { table =>
      val exception = Check.intercept[Exception](
        table.spark.sql(
          s"ALTER TABLE ${table.name} ADD PARTITION FIELD ${Core.date0.columnName}"))

      assert(exception.getMessage.contains("Evolution of table partitioning"))
    }

  /**
   * ALTER TABLE DROP PARTITION FIELD on a date-partitioned table is rejected with an exception stating that evolution
   * of table partitioning is unsupported.
   */
  private def dropPartitionFieldRejectedCase(format: String): Plan.Case =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"PARTITIONED BY (${Core.date0.columnName}) " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(standardSeedRowCount)())
      .test("partitionEvolution.drop.rejected") { table =>
        val exception = Check.intercept[Exception](
          table.spark.sql(
            s"ALTER TABLE ${table.name} DROP PARTITION FIELD ${Core.date0.columnName}"))

        assert(exception.getMessage.contains("Evolution of table partitioning"))
      }

}
