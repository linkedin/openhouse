package harness

import org.apache.spark.sql.SparkSession

/**
 * Write distribution: the write.distribution-mode a table is configured with is persisted in its Iceberg properties and
 * decides how a multi-task append is laid out on disk without changing the rows the table holds.
 *
 * Operations: creating a four-partition table under an explicit write.distribution-mode of none and of hash, reading
 * the persisted property back from the Iceberg table, appending one multi-task DataFrame, and comparing the data-file
 * layout each mode produces against the rows it lands.
 *
 * Preparation axes: each case builds its own four-partition table in each columnar format, because the physical effect
 * only shows on a multi-task append that spans every partition.
 *
 * Case families: three families contributing 6 cases.
 */
trait ScenarioWriteDistribution extends CatalogDdlSupport {

  /** Every write-distribution case, one file format at a time. */
  lazy val writeDistributionCases: List[TestCase] =
    fileFormats.flatMap { format =>
      List(
        TestCase(s"writeDistribution.noneVersusHash @ $format", noneVersusHashCase(format)),
        TestCase(s"writeDistribution.noneRetained @ $format", noneRetainedCase(format)),
        TestCase(s"writeDistribution.hashRetained @ $format", hashRetainedCase(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  // The tables the layout cases build hold 400 rows spread over 4 table partitions, written from 8 input tasks that
  // each hold rows for every partition.
  private val distributionPartitionCount = 4
  private val distributionInputTaskCount = 8
  private val distributionRowCount = 400

  // The exact (id, p) rows a multi-task append lands: id 0 to 399 with p = id mod the partition count.
  private val expectedDistributionRows: Seq[(Long, Int)] =
    (0 until distributionRowCount).map(id => (id.toLong, id % distributionPartitionCount))

  private def createUnder(spark: SparkSession, mode: String, table: String, format: String): Unit =
    spark.sql(
      s"CREATE TABLE $table (id bigint, p int) USING $dataSource PARTITIONED BY (p) " +
        "TBLPROPERTIES ('format-version'='2', " +
        s"'write.format.default'='$format', 'write.distribution-mode'='$mode')")

  private def appendInputRows(spark: SparkSession, table: String): Unit =
    spark
      .range(0, distributionRowCount.toLong)
      .selectExpr("id", s"cast(id % $distributionPartitionCount as int) as p")
      .repartition(distributionInputTaskCount)
      .writeTo(table)
      .append()

  private def rowsOf(spark: SparkSession, table: String): Seq[(Long, Int)] =
    spark
      .sql(s"SELECT id, p FROM $table ORDER BY id")
      .collect()
      .toSeq
      .map(row => (row.getLong(0), row.getInt(1)))

  private def dataFileCountOf(spark: SparkSession, table: String): Long =
    spark.sql(s"SELECT count(*) FROM $table.data_files").collect()(0).getLong(0)

  // The write.distribution-mode read from the Iceberg table's own properties, the source of truth the writer consults.
  private def persistedDistributionMode(spark: SparkSession, table: String): Option[String] =
    Option(icebergTableOf(spark, table).properties().get("write.distribution-mode"))

  /**
   * The same multi-task append under an explicit write.distribution-mode of none and of hash keeps the mode each table
   * was configured with and lands the same logical rows in both, while producing the physical layout each mode defines.
   * Under none every input task writes every partition it holds, so one append produces up to (input tasks times
   * partitions) data files. Under hash the writer shuffles rows so one task owns each partition, clustering the append
   * to about one file per partition.
   *
   * The comparison needs both tables live at once, so the case nests one owned-table lifecycle inside the other. Each
   * table carries a generated UUID and counter name, each lifecycle takes ownership the moment its CREATE returns, and
   * each drops the one table it owns. A failure while building or appending to the hash table therefore still drops
   * the none table, and the failure the case reports stays the primary one with any cleanup failure suppressed behind
   * it.
   */
  private def noneVersusHashCase(format: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val noneTable = TableTest.nextQualifiedTableName(ctx.namespace)
    val hashTable = TableTest.nextQualifiedTableName(ctx.namespace)

    withOwnedTable(spark.sql(_), noneTable)(createUnder(spark, "none", noneTable, format)) {
      appendInputRows(spark, noneTable)

      withOwnedTable(spark.sql(_), hashTable)(createUnder(spark, "hash", hashTable, format)) {
        appendInputRows(spark, hashTable)

        assert(
          persistedDistributionMode(spark, noneTable).contains("none"),
          s"[$format] the none table should persist write.distribution-mode=none")
        assert(
          persistedDistributionMode(spark, hashTable).contains("hash"),
          s"[$format] the hash table should persist write.distribution-mode=hash")

        val noneRows = rowsOf(spark, noneTable)
        assert(
          noneRows == expectedDistributionRows,
          s"[$format] the none table should hold the exact appended rows")
        assert(
          noneRows == rowsOf(spark, hashTable),
          s"[$format] the two distribution modes should land the same logical rows")

        val noneFileCount = dataFileCountOf(spark, noneTable)
        val hashFileCount = dataFileCountOf(spark, hashTable)
        assert(
          hashFileCount <= distributionPartitionCount.toLong * 2,
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

  /**
   * A four-partition table created with write.distribution-mode=none persists that property, and a multi-task append
   * fans out across the input tasks: because the writer runs no shuffle, one append spreads its rows across more data
   * files than the partition count while landing exactly the appended rows.
   */
  private def noneRetainedCase(format: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val noneTable = TableTest.nextQualifiedTableName(ctx.namespace)

    withOwnedTable(spark.sql(_), noneTable)(createUnder(spark, "none", noneTable, format)) {
      appendInputRows(spark, noneTable)

      assert(
        persistedDistributionMode(spark, noneTable).contains("none"),
        s"[$format] distribution-mode none should be persisted")

      val fileCount = dataFileCountOf(spark, noneTable)
      assert(
        fileCount > distributionPartitionCount.toLong &&
          fileCount <= distributionPartitionCount.toLong * distributionInputTaskCount,
        s"[$format] none should fan the append out beyond $distributionPartitionCount files and " +
          s"at most ${distributionPartitionCount * distributionInputTaskCount}, got $fileCount")
      assert(
        rowsOf(spark, noneTable) == expectedDistributionRows,
        s"[$format] the none table should hold the exact appended rows")
    }
  }

  /**
   * A four-partition table created with write.distribution-mode=hash persists that property, and a multi-task append
   * clusters: because the writer shuffles rows so one task owns each partition, one append lands about one data file
   * per partition while holding exactly the appended rows.
   */
  private def hashRetainedCase(format: String)(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val hashTable = TableTest.nextQualifiedTableName(ctx.namespace)

    withOwnedTable(spark.sql(_), hashTable)(createUnder(spark, "hash", hashTable, format)) {
      appendInputRows(spark, hashTable)

      assert(
        persistedDistributionMode(spark, hashTable).contains("hash"),
        s"[$format] distribution-mode hash should be persisted")

      val fileCount = dataFileCountOf(spark, hashTable)
      assert(
        fileCount >= 1 && fileCount <= distributionPartitionCount.toLong * 2,
        s"[$format] hash should cluster the append to about $distributionPartitionCount files, " +
          s"got $fileCount")
      assert(
        rowsOf(spark, hashTable) == expectedDistributionRows,
        s"[$format] the hash table should hold the exact appended rows")
    }
  }

}
