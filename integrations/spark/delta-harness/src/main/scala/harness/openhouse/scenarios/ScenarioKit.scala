package harness

import org.apache.spark.sql.{Row, SparkSession}
import java.util.concurrent.TimeUnit

/**
 * The shared starting-state kit every capability trait builds on: the core table shape, the layout cross-product, the
 * standard seed, the standard preparations, and the small query helpers a case needs.
 *
 * Every capability trait extends this kit, so mixing them into `object Scenarios` puts ScenarioKit first in the
 * linearization and its vals initialize before any capability's. It holds copy-on-write layouts and preparations only;
 * each feature layer carries its own kit that extends this one. `protected` members are the shared kit; `public` ones
 * are also consumed by `object ScenarioCatalog`, `object Plan`, and downstream runners.
 */
trait ScenarioKit {

  protected val Core = CoreTable            // brevity in the typed column references below
  protected val columnNameList = Core.columnNames.mkString(", ") // source column list, so renames propagate

  // The rows a case reads back are ordered by the long column and carry exactly the core columns in their declared
  // order, so an expected row set is written as the rows the case started from, filtered, mapped through
  // `withColumnValue`, extended with literal rows, and re-sorted. Both helpers address columns by position so they also
  // work on the literal rows a case writes out.
  private def columnPosition(column: Column[_]): Int = Core.columnNames.indexOf(column.columnName)

  protected def withColumnValue[T](row: Row, column: Column[T], value: T): Row =
    Row.fromSeq(row.toSeq.updated(columnPosition(column), value))

  protected def inKeyOrder(rows: Seq[Row]): Seq[Row] =
    rows.sortBy(_.getLong(columnPosition(Core.long0)))

  // --- layouts: one file format and one partitioning per starting table shape ---
  // A layout is one starting table shape. Each layout is a plain literal CREATE statement: the column list is one
  // shared literal `columnDefinitions`, and format and partitioning are literal fragments. The schema-creation case
  // cross-checks the literal against CoreTable's declared columns, so the two stay in step. A layout belongs to the
  // preparation, so one test case is written once and runs on every layout.
  protected val columnDefinitions =
    "foo_col_long bigint, foo_col_int int, foo_col_string string, foo_col_double double, " +
      "foo_col_boolean boolean, foo_col_date string"

  /** One starting table shape: the label that names it in a case ID and the CREATE statement that builds it. */
  final case class Layout(label: String, create: String => String)

  /** One partitioning choice: the label that names it in a case ID and the CREATE clause that applies it. */
  final case class Partitioning(label: String, clause: String)

  /** The empty partitioning clause: the table keeps all its rows in one unpartitioned file set. */
  protected val unpartitioned = Partitioning("unpartitioned", "")

  /** Partitions the table by its date column, so each distinct date value owns one partition. */
  protected val partitionedByDate =
    Partitioning("partitioned", s"PARTITIONED BY (${Core.date0.columnName})")

  protected val partitionings: List[Partitioning] = List(unpartitioned, partitionedByDate)

  /**
   * Every file format the standard matrix runs on. This is the single source for a format list anywhere in the
   * harness, so every format-crossed family covers both columnar formats. A format beyond these two is proven by the
   * file-format extension layer, which supplies its own list.
   */
  val fileFormats: List[String] = List("parquet", "orc")

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

  /** The core layouts partitioned by the date column, one per file format. */
  val partitionedLayouts: List[Layout] =
    fileFormats.map(format => coreLayout(partitionedByDate, format))

  /**
   * The standard seed writes three deterministic rows with keys 1, 2 and 3. Row `n` holds key `n` in the long column,
   * `n` in the int column, `row-n` in the string column, `n.5` in the double column, `n % 2 == 0` in the boolean
   * column, and the date value `n - 1` hours after `2024-01-01-00`. `RowGenerator` builds those literals from
   * `CoreTable`, so the seed follows a column rename. Every preparation that seeds a core table writes exactly these
   * rows, so a case that starts from a seeded table knows its three starting keys.
   */
  val standardSeedRowCount: Int = 3

  /** Creates the table under `layout` and leaves it empty. The caller adds the seed step it wants. */
  def create(layout: Layout): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(layout.create)()

  /** One preparation per core layout: the table is created, then seeded with the standard rows. */
  val preparedCoreTables: List[TablePreparation[CoreTable.type]] =
    layouts.map(layout =>
      TablePreparation(
        layout.label,
        create(layout).insert(standardSeedRowCount)()))

  /**
   * One preparation per date-partitioned core layout: the table is created, then seeded with the standard rows, whose
   * date values put one row in each of three partitions.
   */
  val preparedPartitionedCoreTables: List[TablePreparation[CoreTable.type]] =
    partitionedLayouts.map(layout =>
      TablePreparation(
        layout.label,
        create(layout).insert(standardSeedRowCount)()))

  /**
   * One preparation per core layout: the table is created, seeded, then given a write sort order on the long key by
   * ALTER TABLE WRITE ORDERED BY. The column list stays as seeded, so every DML case runs on the result.
   */
  val preparedOrderedCoreTables: List[TablePreparation[CoreTable.type]] =
    layouts.map(layout =>
      TablePreparation(
        layout.label,
        create(layout)
          .insert(standardSeedRowCount)()
          .sql("writeOrderedByLongKey")(table =>
            s"ALTER TABLE $table WRITE ORDERED BY ${Core.long0.columnName}")(),
        "prep.ordered:"))

  /**
   * One preparation per core layout: the table is created, seeded, then given a prep_extra int column by ALTER TABLE
   * ADD COLUMN. The column list grows past the seed row shape and the seeded rows read null for the new column, so the
   * cases that address columns by name run on the result: the reads, the deletes and the updates.
   */
  val preparedEvolvedCoreTables: List[TablePreparation[CoreTable.type]] =
    layouts.map(layout =>
      TablePreparation(
        layout.label,
        create(layout)
          .insert(standardSeedRowCount)()
          .sql("addPrepExtraColumn")(table => s"ALTER TABLE $table ADD COLUMN prep_extra int")(),
        "prep.evolved:"))

  /** One preparation per core layout: the table is created and left unseeded, so it holds no rows. */
  val preparedEmptyCoreTables: List[TablePreparation[CoreTable.type]] =
    layouts.map(layout => TablePreparation(layout.label, create(layout)))

  /**
   * The CREATE statement for an unpartitioned core table in `format`. This generic substrate contributes zero cases
   * and gives later capability layers a stable shared starting point.
   */
  protected def coreCreate(table: String, format: String): String =
    coreLayout(unpartitioned, format).create(table)

  /** An unseeded, unpartitioned core table in `format`, so the case owns every row the table holds. */
  protected def preparedEmptyStandardTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(format, create(coreLayout(unpartitioned, format)))

  /**
   * An unpartitioned core table in `format`, created and then seeded with the standard rows. This is the plainest
   * starting state in the harness, so most capability families build on it.
   */
  protected def preparedStandardTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      create(coreLayout(unpartitioned, format)).insert(standardSeedRowCount)())

  /** The standard seeded table in each file format. */
  val preparedCoreFormats: List[TablePreparation[CoreTable.type]] =
    fileFormats.map(preparedStandardTable)

  /**
   * An unpartitioned core table in `format` holding five rows across two snapshots: the standard seed, then rows 4 and
   * 5. The step between the two commits holds until the wall clock passes the seed commit's timestamp, so the two
   * snapshots carry distinct commit times and a timestamp-bounded read resolves to exactly one of them.
   *
   * Every family that reads history needs this shape, so the shared kit owns it.
   */
  protected def preparedTwoSnapshotTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      create(coreLayout(unpartitioned, format))
        .insert(standardSeedRowCount)()
        .step("waitForNextSnapshotTimestamp")(waitForNextSnapshotTimestamp)()
        .sql("insertRowsFourAndFive")(table =>
          s"INSERT INTO $table VALUES " +
            "(CAST(4 AS BIGINT), 4, 'row-4', 4.5, true, '2024-01-04-03'), " +
            "(CAST(5 AS BIGINT), 5, 'row-5', 5.5, false, '2024-01-05-04')")())

  /**
   * The same starting state with a fourth row whose key is 99 and whose string column is null, so exactly one row of
   * the table reads null for that column.
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

  // Waits until the wall clock passes the newest snapshot's commit timestamp, so the next commit lands on a strictly
  // later millisecond and a timestamp-bounded read separates the two snapshots.
  private def waitForNextSnapshotTimestamp(spark: SparkSession, table: String): Unit = {
    val previousTimestamp = spark
      .sql(
        s"SELECT committed_at FROM $table.snapshots " +
          "ORDER BY committed_at DESC LIMIT 1")
      .collect()(0)
      .getTimestamp(0)
      .getTime
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)

    while (
      System.currentTimeMillis() <= previousTimestamp &&
      System.nanoTime() < deadline) {
      Thread.sleep(1L)
    }

    assert(
      System.currentTimeMillis() > previousTimestamp,
      s"clock did not advance beyond snapshot timestamp $previousTimestamp")
  }

  // --- table, rename and lock lifecycle boundaries used by cases that build artifacts for themselves ---
  // Each boundary takes the catalog statement executor, so a test drives the same code with a recorder in place of
  // Spark. Call sites pass `spark.sql(_)`.

  /**
   * Runs `use` against `table`, a table the case builds for itself. `create` issues the CREATE; ownership starts the
   * moment it returns, so a name that is already taken leaves the pre-existing table intact and the drop afterwards
   * removes only the table this call created. A failure in `create` or `use` stays the
   * primary failure, and a cleanup failure is attached to it as a suppressed exception. Callers name the table with
   * `TableTest.nextQualifiedTableName` or by extending the generated name of a prepared table, so two runs of the same
   * case can never address the same table.
   */
  private[harness] def withOwnedTable(runStatement: String => Unit, table: String)(
      create: => Unit)(use: => Unit): Unit =
    OwnedTableLifecycle.withOwnership(runStatement(s"DROP TABLE IF EXISTS $table")) {
      markTableCreated =>
        create
        markTableCreated()
        use
    }

  /**
   * Runs `use`, then runs `cleanupStatement` on every outcome. A case uses this for an artifact whose creation is the
   * rejection under test: the statement that would create it is expected to fail, so there is no successful create to
   * take ownership of, yet a rejection that partly applied, threw the wrong type, or unexpectedly succeeded must still
   * leave nothing behind. The failure `use` raises stays primary and a cleanup failure rides along suppressed.
   */
  private[harness] def withCleanupStatement(runStatement: String => Unit, cleanupStatement: String)(
      use: => Unit): Unit =
    OwnedTableLifecycle.withCleanup(runStatement(cleanupStatement))(use)

  /**
   * Runs `use` while tracking which name a table answers to. `use` receives a rename function that issues one ALTER
   * TABLE RENAME TO and records the new name only once the catalog accepted it, so the boundary always knows the live
   * name. If `use` leaves the table under any name other than `originalTable`, the boundary drops that live name, so
   * a failed assertion or a failed rename back still ends with the table gone. A failure in `use` stays primary and a
   * cleanup failure rides along suppressed.
   */
  private[harness] def withTrackedRename(runStatement: String => Unit, originalTable: String)(
      use: (String => Unit) => Unit): Unit = {
    var liveTable = originalTable
    OwnedTableLifecycle.withCleanup(
      if (liveTable != originalTable) runStatement(s"DROP TABLE IF EXISTS $liveTable")) {
      use { newTable =>
        runStatement(s"ALTER TABLE $liveTable RENAME TO $newTable")
        liveTable = newTable
      }
    }
  }

  /**
   * Runs `use` while the case holds a table lock. `lock` is taken first and its response is checked; `use` receives a
   * release function so a case that reads behavior after the lock is gone releases it itself. The boundary releases
   * the lock afterwards only while the case still holds it, so exactly one release is attempted. Every release checks
   * its response, so a rejected release fails the case, and a release failure that follows a failure inside `use`
   * rides along as a suppressed exception.
   */
  private[harness] def withTableLock(
      lock: () => (Int, String),
      unlock: () => (Int, String))(use: (() => Unit) => Unit): Unit = {
    val (lockStatus, lockBody) = lock()
    assert(lockStatus >= 200 && lockStatus < 300, s"lock request failed: $lockStatus $lockBody")

    var lockHeld = true
    def releaseLock(): Unit = {
      val (unlockStatus, unlockBody) = unlock()
      lockHeld = false
      assert(
        unlockStatus >= 200 && unlockStatus < 300,
        s"unlock request failed: $unlockStatus $unlockBody")
    }

    OwnedTableLifecycle.withCleanup(if (lockHeld) releaseLock())(use(() => releaseLock()))
  }

  // --- shared query helpers used across capability traits ---

  // Snapshots in ancestry order (root first), following the parent_id chain. The chain orders commits deterministically
  // even when two of them share a committed_at millisecond.
  protected def snapshotIds(spark: SparkSession, table: String): Seq[Long] = {
    val rows = spark.sql(s"SELECT snapshot_id, parent_id FROM $table.snapshots").collect().toSeq
    val snapshotIdSet = rows.map(_.getLong(0)).toSet
    val childByParent = rows.collect {
      case row if !row.isNullAt(1) => row.getLong(1) -> row.getLong(0)
    }.toMap
    val root = rows.collectFirst {
      case row if row.isNullAt(1) || !snapshotIdSet.contains(row.getLong(1)) => row.getLong(0)
    }.get

    Iterator
      .iterate(Option(root))(parent => parent.flatMap(childByParent.get))
      .takeWhile(_.isDefined)
      .flatten
      .toList
  }

  protected def catalogRelative(table: String): String = table.stripPrefix("openhouse.")

  /** One core row in the seed shape, keyed by `long` and tagged in the string column. */
  protected def coreRow(long: Long, tag: String): String =
    s"(CAST($long AS BIGINT), ${long.toInt}, '$tag', ${long}.5, false, '2024-01-01-00')"

  // The Spark data source used by CREATE TABLE statements. The LinkedIn adapter overrides this before building
  // ScenarioCatalog.cases. Catalog procedure calls still use the catalog name "openhouse".
  var dataSource: String = "iceberg"

  protected def tableProps(spark: SparkSession, table: String): Map[String, String] =
    spark.sql(s"SHOW TBLPROPERTIES $table").collect().toSeq.map(r => r.getString(0) -> r.getString(1)).toMap

  protected val extraColInsert9  = "(CAST(9 AS BIGINT), 9, 'row-9', 9.5, true, '2024-01-09-01', 42)"
  protected val extraColInsert10 = "(CAST(10 AS BIGINT), 10, 'row-10', 10.5, true, '2024-01-10-01', 43)"

  protected def countOf(spark: SparkSession, sql: String): String =
    spark.sql(sql).collect()(0).getLong(0).toString

}
