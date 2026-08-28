package harness

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.iceberg.exceptions.BadRequestException
import org.apache.iceberg.exceptions.ValidationException
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

// Shared foundation for every Scenario trait: the standard table/layout/prep "kit". All domain
// traits (DmlScenarios, ForkScenarios, ...) extend this, so mixing them into `object Scenarios` puts
// ScenarioKit first in the linearization, so its vals initialize before any domain's, exactly as
// in the original single object. It holds the copy-on-write layouts and preparations only; each
// feature layer carries its own kit that extends this one. `protected` members are the shared kit;
// `public` ones are also consumed by `object Plan`.
trait ScenarioKit {
  import Rows._

  protected val Core = CoreTable            // brevity in the typed column references below
  protected val cols = Core.columnNames.mkString(", ") // source column list, so renames propagate

  // The rows a case reads back are ordered by the long column and carry exactly the core columns in
  // their declared order, so an expected row set is written as the rows the case started from,
  // filtered, mapped through `withColumnValue`, extended with literal rows, and re-sorted. Both
  // helpers address columns by position so they also work on the literal rows a case writes out.
  private def columnPosition(column: Column[_]): Int = Core.columnNames.indexOf(column.columnName)

  protected def withColumnValue[T](row: Row, column: Column[T], value: T): Row =
    Row.fromSeq(row.toSeq.updated(columnPosition(column), value))

  protected def inKeyOrder(rows: Seq[Row]): Seq[Row] =
    rows.sortBy(_.getLong(columnPosition(Core.long0)))

  // --- layouts: one file format and one partitioning per starting table shape ---
  // A layout is one starting table shape. Each layout is a plain literal CREATE statement: the
  // column list is one shared literal `columnDefinitions`, and format and partitioning are literal
  // fragments. createSchema cross-checks the literal against CoreTable's declared columns, so the
  // two stay in step. A layout belongs to the preparation, so one test case is written once and
  // runs on every layout.
  protected val columnDefinitions =
    "foo_col_long bigint, foo_col_int int, foo_col_string string, foo_col_double double, foo_col_boolean boolean, datepartition string"

  /** One starting table shape: the label that names it in a case ID and the CREATE statement that
    * builds it. */
  final case class Layout(label: String, create: String => String)

  /** One partitioning choice: the label that names it in a case ID and the CREATE clause that
    * applies it. */
  final case class Partitioning(label: String, clause: String)

  /** The empty partitioning clause: the table keeps all its rows in one unpartitioned file set. */
  protected val unpartitioned = Partitioning("unpartitioned", "")

  /** Partitions the table by datepartition, so each distinct date value owns one partition. */
  protected val partitionedByDate =
    Partitioning("partitioned", "PARTITIONED BY (datepartition)")

  protected val partitionings: List[Partitioning] = List(unpartitioned, partitionedByDate)

  protected val fileFormats: List[String] = List("parquet", "orc", "avro")

  /** One copy-on-write table in `format`, shaped by `partitioning`, labelled for its case IDs. */
  private def coreLayout(partitioning: Partitioning, format: String): Layout =
    Layout(
      s"${partitioning.label}/$format",
      table =>
        s"CREATE TABLE $table ($columnDefinitions) USING $dataSource ${partitioning.clause} " +
          s"TBLPROPERTIES ('write.format.default'='$format')")

  /** Every core layout: each file format crossed with each partitioning. */
  val layouts: List[Layout] =
    for {
      format       <- fileFormats
      partitioning <- partitionings
    } yield coreLayout(partitioning, format)

  /** The core layouts partitioned by datepartition, one per file format. */
  val partitionedLayouts: List[Layout] =
    fileFormats.map(format => coreLayout(partitionedByDate, format))

  /**
   * The Parquet and ORC core layouts, each crossed with both partitionings, for the bespoke DDL
   * cases that do not need the full file-format cross.
   */
  val parquetAndOrcLayouts: List[Layout] =
    for {
      format       <- List("parquet", "orc")
      partitioning <- partitionings
    } yield coreLayout(partitioning, format)

  /** Creates the table under `layout`, then seeds `numberOfRows` deterministic rows. */
  def createAndSeed(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(layout.create)().insert(numberOfRows)()

  /** One preparation per core layout: three seed rows with keys 1, 2 and 3. */
  val preparedCoreTables: List[TablePreparation[CoreTable.type]] =
    layouts.map(layout =>
      TablePreparation(
        layout.label,
        createAndSeed(layout, 3)))

  /**
   * One preparation per datepartition-partitioned core layout: three seed rows with keys 1, 2 and
   * 3, one row per datepartition value.
   */
  val preparedPartitionedCoreTables: List[TablePreparation[CoreTable.type]] =
    partitionedLayouts.map(layout =>
      TablePreparation(
        layout.label,
        createAndSeed(layout, 3)))

  /**
   * One preparation per core layout: three seed rows, then ALTER TABLE WRITE ORDERED BY the long
   * key, so the table carries that write sort order.
   */
  val preparedOrderedCoreTables: List[TablePreparation[CoreTable.type]] =
    layouts.map(layout =>
      TablePreparation(
        layout.label,
        createAndSeedOrdered(layout, 3),
        "prep.ordered:"))

  /**
   * One preparation per core layout: three seed rows, then ADD COLUMN prep_extra int, so the table
   * carries one column beyond the seed row shape and the seeded rows read null for it.
   */
  val preparedEvolvedCoreTables: List[TablePreparation[CoreTable.type]] =
    layouts.map(layout =>
      TablePreparation(
        layout.label,
        createAndSeedEvolved(layout, 3),
        "prep.evolved:"))

  /**
   * One preparation per core layout: the table is created and left unseeded, so it holds no rows.
   */
  val preparedEmptyCoreTables: List[TablePreparation[CoreTable.type]] =
    layouts.map(layout =>
      TablePreparation(
        layout.label,
        TableTest(Core).sql("create")(layout.create)()))

  /**
   * One preparation per Parquet and ORC unpartitioned layout: three seed rows with keys 1, 2 and 3.
   */
  val preparedCoreFormats: List[TablePreparation[CoreTable.type]] =
    List("parquet", "orc").map { format =>
      val layout = coreLayout(unpartitioned, format)
      TablePreparation(
        format,
        createAndSeed(layout, 3))
    }

  /**
   * Creates and seeds the table under `layout`, then gives it a write sort order on the long key.
   * The column list stays as seeded, so every DML case runs on the result.
   */
  def createAndSeedOrdered(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    createAndSeed(layout, numberOfRows).sql("prep.ordered")(t => s"ALTER TABLE $t WRITE ORDERED BY ${CoreTable.long0.columnName}")()

  /**
   * Creates and seeds the table under `layout`, then adds the prep_extra column. The column list
   * grows past the seed row shape, so the cases that address columns by name run on the result:
   * the reads, the deletes and the updates.
   */
  def createAndSeedEvolved(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    createAndSeed(layout, numberOfRows).sql("prep.evolved")(t => s"ALTER TABLE $t ADD COLUMN prep_extra int")()

  /**
   * The same starting state with a fourth row whose key is 99 and whose string column is null, so
   * exactly one row of the table reads null for that column.
   */
  protected def withNullStringRow(
      basePreparation: TablePreparation[CoreTable.type]
  ): TablePreparation[CoreTable.type] =
    basePreparation.copy(
      preparation = basePreparation.preparation.sql("prep.nullStringRow")(table =>
        s"INSERT INTO $table VALUES (CAST(99 AS BIGINT), 99, NULL, 99.5, false, '2024-01-01-00')")())

  /** The core preparations, each carrying one row whose string column is null. */
  val preparedNullStringCoreTables: List[TablePreparation[CoreTable.type]] =
    preparedCoreTables.map(withNullStringRow)

  /** The write-ordered preparations, each carrying one row whose string column is null. */
  val preparedNullStringOrderedCoreTables: List[TablePreparation[CoreTable.type]] =
    preparedOrderedCoreTables.map(withNullStringRow)

  /**
   * Every data file the preparation wrote carries the extension of the table's declared
   * write.format.default, and listing the files leaves the table state unchanged.
   */
  private def formatMaterializationCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("format.materialization") { table =>
      val before = table.state
      val declaredFormat = table.spark
        .sql(s"SHOW TBLPROPERTIES ${table.name} ('write.format.default')")
        .collect()(0)
        .getString(1)
      val filePaths = table.spark
        .sql(s"SELECT file_path FROM ${table.name}.files")
        .collect()
        .toSeq
        .map(_.getString(0))
      val after = table.state

      assert(
        filePaths.nonEmpty && filePaths.forall(_.toLowerCase.endsWith(s".$declaredFormat")),
        s"data files are not all .$declaredFormat: $filePaths")
      assert(after == before, "listing files leaves the rows and the snapshot count unchanged")
    }

  /**
   * The format-materialization case for each preparation given. It applies to any preparation that
   * leaves data files behind, so each feature layer passes the list its own preparations produce.
   */
  def layoutFormatCasesFor(
      preparations: List[TablePreparation[CoreTable.type]]
  ): List[Plan.Case] =
    preparations.map { preparation =>
      formatMaterializationCase(preparation)
    }

  /** The standard preparations that leave data files behind: the core and write-ordered ones. */
  val layoutFormatPreparations: List[TablePreparation[CoreTable.type]] =
    preparedCoreTables ++ preparedOrderedCoreTables

  /** The format-materialization case on every standard preparation that writes data files. */
  def layoutFormatCases: List[Plan.Case] = layoutFormatCasesFor(layoutFormatPreparations)

  private def waitForNextSnapshotTimestamp(spark: SparkSession, table: String): Unit = {
    val previousTimestamp = spark
      .sql(
        s"SELECT committed_at FROM $table.snapshots " +
          "ORDER BY committed_at DESC LIMIT 1")
      .collect()(0)
      .getTimestamp(0)
      .getTime
    val deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5)

    while (
      System.currentTimeMillis() <= previousTimestamp &&
      System.nanoTime() < deadline) {
      Thread.sleep(1L)
    }

    assert(
      System.currentTimeMillis() > previousTimestamp,
      s"clock did not advance beyond snapshot timestamp $previousTimestamp")
  }

  // Shared helpers used across domain traits.

  /**
   * Creates a table in the given file format, seeds three rows as the first snapshot, then inserts
   * rows 4 and 5 as a second snapshot committed at a later timestamp.
   */
  protected def coreTwoSnapshots(fmt: String): TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(table => s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES ('write.format.default'='$fmt')")()
      .insert(3)()
      .step("waitForNextSnapshotTimestamp")(waitForNextSnapshotTimestamp)()
      .sql("insertMore")(table => s"INSERT INTO $table VALUES " +
        s"(CAST(4 AS BIGINT), 4, 'row-4', 4.5, true, '2024-01-04-03'), (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')")()

  /** The two-snapshot table in parquet. */
  protected def coreTwoSnapshots: TableTest[CoreTable.type] = coreTwoSnapshots("parquet")

  // Snapshots in ancestry order (root first), following the parent_id chain. This is deterministic even
  // if two commits happen to share a committed_at millisecond (which `ORDER BY committed_at` is not).
  protected def snapshotIds(spark: SparkSession, table: String): Seq[Long] = {
    val rows = spark.sql(s"SELECT snapshot_id, parent_id FROM $table.snapshots").collect().toSeq
    val ids = rows.map(_.getLong(0)).toSet
    val childByParent = rows.collect { case r if !r.isNullAt(1) => r.getLong(1) -> r.getLong(0) }.toMap
    val root = rows.collectFirst { case r if r.isNullAt(1) || !ids.contains(r.getLong(1)) => r.getLong(0) }.get
    val order = scala.collection.mutable.ListBuffer(root)
    var cur = root
    while (childByParent.contains(cur)) { cur = childByParent(cur); order += cur }
    order.toList
  }

  protected def catalogRelative(table: String): String = table.stripPrefix("openhouse.")

  protected def coreRow(long: Long, tag: String): String =
    s"(CAST($long AS BIGINT), ${long.toInt}, '$tag', ${long}.5, false, '2024-01-01-00')"

  protected val L = CoreTable.long0.columnName

  // The Spark data source used by CREATE TABLE statements. The LinkedIn adapter overrides this before
  // building Plan.cases. Catalog procedure calls still use the catalog name "openhouse".
  var dataSource: String = "iceberg"

  protected def coreCreateParquet(table: String): String =
    s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES ('write.format.default'='parquet')"

  protected def tableProps(spark: SparkSession, table: String): Map[String, String] =
    spark.sql(s"SHOW TBLPROPERTIES $table").collect().toSeq.map(r => r.getString(0) -> r.getString(1)).toMap

  protected val extraColInsert9  = "(CAST(9 AS BIGINT), 9, 'row-9', 9.5, true, '2024-01-09-01', 42)"
  protected val extraColInsert10 = "(CAST(10 AS BIGINT), 10, 'row-10', 10.5, true, '2024-01-10-01', 43)"

  protected def countOf(spark: SparkSession, sql: String): String =
    spark.sql(sql).collect()(0).getLong(0).toString

}
