package harness

/**
 * Write distribution: the write.distribution-mode a table is configured with is retained, and it decides how a single
 * append is laid out on disk without changing the rows the table holds.
 *
 * Operations: creating a table with an explicit write.distribution-mode of none and of hash and reading the property
 * back; appending one multi-task DataFrame into a four-partition table under each mode and comparing the rows and the
 * data-file counts the two modes produce.
 *
 * Preparation axes: for the two retained-property families, the standard three-row seed in each of the two columnar
 * formats, unpartitioned for none and date-partitioned for hash. The layout family builds its own four-partition
 * tables in each format, because it needs one table per mode inside a single case.
 *
 * Case families: three families contributing 6 cases.
 */
trait WriteDistributionScenarios extends ScenarioKit {

  /** Every write-distribution case, one file format at a time. */
  lazy val writeDistributionCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        Plan.Case(s"writeDistribution.noneVersusHash @ $format", noneVersusHashCase(format)),
        noneRetainedCase(format),
        hashRetainedCase(format))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  // The two tables the layout case compares hold 400 rows spread over 4 table partitions, written from 8 input tasks
  // that each hold rows for every partition.
  private val distributionPartitionCount = 4
  private val distributionInputTaskCount = 8
  private val distributionRowCount = 400

  /**
   * The same multi-task append under an explicit write.distribution-mode of none and of hash keeps the mode each table
   * was configured with and lands the same logical rows in both, while producing the physical layout each mode
   * defines. Under none every input task writes every partition it holds, so one append produces up to (input tasks
   * times partitions) data files. Under hash the writer shuffles rows so one task owns each partition, clustering the
   * append to about one file per partition.
   *
   * The comparison needs both tables live at once, so the case nests one owned-table lifecycle inside the other. Each
   * table carries a generated UUID and counter name, each lifecycle takes ownership the moment its CREATE returns, and
   * each drops the one table it owns. A failure while building or appending to the hash table therefore still drops
   * the none table, and the failure the case reports stays the primary one with any cleanup failure suppressed
   * behind it.
   */
  private def noneVersusHashCase(format: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark

    def createUnder(mode: String, table: String): Unit =
      spark.sql(
        s"CREATE TABLE $table (id bigint, p int) USING $dataSource PARTITIONED BY (p) " +
          "TBLPROPERTIES ('format-version'='2', " +
          s"'write.format.default'='$format', 'write.distribution-mode'='$mode')")

    def appendInputRows(table: String): Unit =
      spark
        .range(0, distributionRowCount.toLong)
        .selectExpr("id", s"cast(id % $distributionPartitionCount as int) as p")
        .repartition(distributionInputTaskCount)
        .writeTo(table)
        .append()

    def rowsOf(table: String): Seq[(Long, Int)] =
      spark
        .sql(s"SELECT id, p FROM $table ORDER BY id")
        .collect()
        .toSeq
        .map(row => (row.getLong(0), row.getInt(1)))

    def dataFileCountOf(table: String): Long =
      spark.sql(s"SELECT count(*) FROM $table.data_files").collect()(0).getLong(0)

    val noneTable = TableTest.nextQualifiedTableName(ctx.namespace)
    val hashTable = TableTest.nextQualifiedTableName(ctx.namespace)

    withOwnedTable(spark.sql(_), noneTable)(createUnder("none", noneTable)) {
      appendInputRows(noneTable)

      withOwnedTable(spark.sql(_), hashTable)(createUnder("hash", hashTable)) {
        appendInputRows(hashTable)

        assert(
          tableProps(spark, noneTable).get("write.distribution-mode").contains("none"),
          s"[$format] the none table should retain write.distribution-mode=none")
        assert(
          tableProps(spark, hashTable).get("write.distribution-mode").contains("hash"),
          s"[$format] the hash table should retain write.distribution-mode=hash")

        val noneRows = rowsOf(noneTable)
        assert(
          noneRows.size == distributionRowCount,
          s"[$format] the none table should hold $distributionRowCount rows, got ${noneRows.size}")
        assert(
          noneRows == rowsOf(hashTable),
          s"[$format] the two distribution modes should land the same logical rows")

        val noneFileCount = dataFileCountOf(noneTable)
        val hashFileCount = dataFileCountOf(hashTable)
        println(
          s"DIAG writeDistribution.noneVersusHash[$format]: noneFiles=$noneFileCount " +
            s"hashFiles=$hashFileCount partitions=$distributionPartitionCount " +
            s"inputTasks=$distributionInputTaskCount")
        assert(
          hashFileCount <= distributionPartitionCount * 2,
          s"[$format] hash should cluster to about $distributionPartitionCount files, " +
            s"got $hashFileCount")
        assert(
          noneFileCount > hashFileCount &&
            noneFileCount <= distributionPartitionCount.toLong * distributionInputTaskCount,
          s"[$format] none should spread the append across more files than hash and at most " +
            s"${distributionPartitionCount * distributionInputTaskCount} " +
            s"(none=$noneFileCount hash=$hashFileCount)")
      }
    }
  }

  /** The write.distribution-mode=none requested at creation is retained and the table holds its 3 seed rows. */
  private def noneRetainedCase(format: String): Plan.Case =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
            s"'write.format.default'='$format', 'write.distribution-mode'='none')")()
        .insert(standardSeedRowCount)())
      .test("writeDistribution.noneRetained") { table =>
        assert(
          tableProps(table.spark, table.name).get("write.distribution-mode").contains("none"),
          "distribution-mode none should be retained")
        assert(
          table.rows.size == standardSeedRowCount,
          "the table should hold its seed rows under distribution-mode none")
      }

  /**
   * The write.distribution-mode=hash requested at creation on a date-partitioned table is retained and the table holds
   * its 3 seed rows.
   */
  private def hashRetainedCase(format: String): Plan.Case =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"PARTITIONED BY (${Core.date0.columnName}) " +
            "TBLPROPERTIES (" +
            s"'write.format.default'='$format', 'write.distribution-mode'='hash')")()
        .insert(standardSeedRowCount)())
      .test("writeDistribution.hashRetained") { table =>
        assert(
          tableProps(table.spark, table.name).get("write.distribution-mode").contains("hash"),
          "distribution-mode hash should be retained")
        assert(
          table.rows.size == standardSeedRowCount,
          "the table should hold its seed rows under distribution-mode hash")
      }

}
