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

  /** One starting table shape: the label that names it in a case id, a human description of the
    * resulting table, and the CREATE statement that builds it. */
  final case class Layout(label: String, description: String, create: String => String)

  object Layout {
    /** A layout whose label already reads as its description. */
    def apply(label: String, create: String => String): Layout = Layout(label, label, create)
  }

  /** One partitioning choice: the label that names it in a case id, a human description, and the
    * CREATE clause that applies it. */
  final case class Partitioning(label: String, description: String, clause: String)

  protected val unpartitioned = Partitioning("unpartitioned", "with no partitioning", "")

  protected val partitionedByDate =
    Partitioning("partitioned", "partitioned by datepartition", "PARTITIONED BY (datepartition)")

  protected val partitionings: List[Partitioning] = List(unpartitioned, partitionedByDate)

  protected val fileFormats: List[String] = List("parquet", "orc", "avro")

  private def coreLayout(partitioning: Partitioning, format: String): Layout =
    Layout(
      s"${partitioning.label}/$format",
      s"a copy-on-write $format table ${partitioning.description}",
      table =>
        s"CREATE TABLE $table ($columnDefinitions) USING $dataSource ${partitioning.clause} " +
          s"TBLPROPERTIES ('write.format.default'='$format')")

  val layouts: List[Layout] =
    for {
      format       <- fileFormats
      partitioning <- partitionings
    } yield coreLayout(partitioning, format)

  val partitionedLayouts: List[Layout] =
    fileFormats.map(format => coreLayout(partitionedByDate, format))

  // Parquet and ORC layouts for bespoke DDL cases that do not need the full format cross.
  val parquetAndOrcLayouts: List[Layout] =
    for {
      format       <- List("parquet", "orc")
      partitioning <- partitionings
    } yield coreLayout(partitioning, format)

  // Create the table under `layout`, then seed deterministic rows as a second visible step.
  def createAndSeed(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(layout.create)().insert(numberOfRows)()

  val preparedCoreTables: List[TablePreparation[CoreTable.type]] =
    layouts.map(layout =>
      TablePreparation(
        layout.label,
        createAndSeed(layout, 3),
        description = s"Three seed rows with keys 1, 2 and 3 in ${layout.description}."))

  val preparedPartitionedCoreTables: List[TablePreparation[CoreTable.type]] =
    partitionedLayouts.map(layout =>
      TablePreparation(
        layout.label,
        createAndSeed(layout, 3),
        description = s"Three seed rows with keys 1, 2 and 3 in ${layout.description}, " +
          "one row per datepartition value."))

  val preparedOrderedCoreTables: List[TablePreparation[CoreTable.type]] =
    layouts.map(layout =>
      TablePreparation(
        layout.label,
        createAndSeedOrdered(layout, 3),
        "prep.ordered:",
        description = s"Three seed rows with keys 1, 2 and 3 in ${layout.description}, then " +
          s"ALTER TABLE WRITE ORDERED BY ${Core.long0.columnName}, so the table carries that write sort order."))

  val preparedEvolvedCoreTables: List[TablePreparation[CoreTable.type]] =
    layouts.map(layout =>
      TablePreparation(
        layout.label,
        createAndSeedEvolved(layout, 3),
        "prep.evolved:",
        description = s"Three seed rows with keys 1, 2 and 3 in ${layout.description}, then " +
          "ADD COLUMN prep_extra int, so the table carries one column beyond the seed row shape " +
          "and the seeded rows read null for it."))

  val preparedEmptyCoreTables: List[TablePreparation[CoreTable.type]] =
    layouts.map(layout =>
      TablePreparation(
        layout.label,
        TableTest(Core).sql("create")(layout.create)(),
        description = s"${layout.description.capitalize} that is created and left unseeded, so it holds no rows."))

  val preparedCoreFormats: List[TablePreparation[CoreTable.type]] =
    List("parquet", "orc").map { format =>
      val layout = coreLayout(unpartitioned, format)
      TablePreparation(
        format,
        createAndSeed(layout, 3),
        description = s"Three seed rows with keys 1, 2 and 3 in ${layout.description}.")
    }

  // A DDL step evolves the starting state, and the test case then runs on the evolved table. The
  // ordered preparation adds a write sort order and leaves the column list intact, so every DML case
  // runs on it. The evolved preparation adds a column, so it runs the cases that address columns by
  // name: reads, deletes, and updates.
  def createAndSeedOrdered(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    createAndSeed(layout, numberOfRows).sql("prep.ordered")(t => s"ALTER TABLE $t WRITE ORDERED BY ${CoreTable.long0.columnName}")()

  def createAndSeedEvolved(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    createAndSeed(layout, numberOfRows).sql("prep.evolved")(t => s"ALTER TABLE $t ADD COLUMN prep_extra int")()

  // The same starting state with one more row appended, whose string column is null. A DELETE that
  // selects rows by IS NULL is then written as one operation against a table that already holds a
  // null string.
  protected def withNullStringRow(
      basePreparation: TablePreparation[CoreTable.type]
  ): TablePreparation[CoreTable.type] =
    basePreparation.copy(
      preparation = basePreparation.preparation.sql("prep.nullStringRow")(table =>
        s"INSERT INTO $table VALUES (CAST(99 AS BIGINT), 99, NULL, 99.5, false, '2024-01-01-00')")(),
      description = s"${basePreparation.description} A fourth row with key 99 is then appended " +
        s"whose ${Core.string0.columnName} is null, so exactly one row of the table reads null for " +
        "that column.")

  val preparedNullStringCoreTables: List[TablePreparation[CoreTable.type]] =
    preparedCoreTables.map(withNullStringRow)

  val preparedNullStringOrderedCoreTables: List[TablePreparation[CoreTable.type]] =
    preparedOrderedCoreTables.map(withNullStringRow)

  // This list validates that each preparation writes data files in its declared format. It runs on
  // every preparation that leaves data files behind. Each feature layer owns the list for its
  // preparations and builds it through this shared case body.
  def layoutFormatCasesFor(
      preparations: List[TablePreparation[CoreTable.type]]
  ): List[Plan.Case] =
    preparations.map { preparation =>
      preparation.test(
        "format.materialization",
        "Every data file the preparation wrote carries the extension of the table's declared " +
          "write.format.default, and listing the files leaves the table state unchanged.") { table =>
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
    }

  val layoutFormatPreparations: List[TablePreparation[CoreTable.type]] =
    preparedCoreTables ++ preparedOrderedCoreTables

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
  protected def coreTwoSnapshots(fmt: String): TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(table => s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES ('write.format.default'='$fmt')")()
      .insert(3)()
      .step("waitForNextSnapshotTimestamp")(waitForNextSnapshotTimestamp)()
      .sql("insertMore")(table => s"INSERT INTO $table VALUES " +
        s"(CAST(4 AS BIGINT), 4, 'row-4', 4.5, true, '2024-01-04-03'), (CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')")()

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
