package harness

import org.apache.spark.sql.{Row, SparkSession}
import java.math.{BigDecimal => JavaBigDecimal}
import java.net.{ConnectException, SocketException, SocketTimeoutException}
import java.sql.{Date, Timestamp}
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

// The harness defines typed, reusable table preparations and self-contained TestCase bodies. Each case gets a fresh table,
// executes its preparation, runs its action and assertions, and drops the table during teardown.

/**
 * One catalog case: the ID that names it, the body that runs it, and its two skip policies.
 * `knownBugReason` marks a case the catalog under test is known to fail; `embeddedSkipReason` marks a case the
 * embedded local catalog cannot run at all. A runner reports either policy as a skip.
 */
final case class TestCase(
  id:                 String,
  run:                Ctx => Unit,
  knownBugReason:     Option[String] = None,
  embeddedSkipReason: Option[String] = None
) {
  /** The skip reason a known bug produces, phrased so a run log explains the skip. */
  def bugReason: Option[String] = knownBugReason.map(reason => s"bug: $reason")
}

final case class Ctx(spark: SparkSession, namespace: String)

sealed trait Outcome { def label: String }
object Outcome {
  case object Passed extends Outcome { val label = "PASS" }
  final case class Failed(cause: Throwable) extends Outcome {
    val label = "FAIL"
    def reason: String = {
      val rootCause = Exceptions.root(cause)
      val rootReason =
        s"${rootCause.getClass.getSimpleName}: ${Option(rootCause.getMessage).getOrElse(rootCause.toString)}"
      if (rootCause eq cause) {
        rootReason
      } else {
        s"${cause.getClass.getSimpleName}: ${Option(cause.getMessage).getOrElse(cause.toString)}; caused by $rootReason"
      }
    }
  }
  final case class Skipped(reason: String) extends Outcome { val label = "SKIP" }
}

object Exceptions {
  def causeChain(throwable: Throwable): List[Throwable] = {
    @tailrec
    def collect(
      current: Option[Throwable],
      seen: Set[Throwable],
      collected: List[Throwable]
    ): List[Throwable] =
      current match {
        case Some(cause) if !seen.contains(cause) =>
          collect(Option(cause.getCause), seen + cause, cause :: collected)
        case _ =>
          collected.reverse
      }

    collect(Some(throwable), Set.empty, Nil)
  }

  def root(throwable: Throwable): Throwable = causeChain(throwable).last

  /**
   * Retries errors positively identified as transient. Other failures remain terminal so data, permission, and
   * assertion failures surface on their first attempt.
   */
  def isTransientConnectionFailure(throwable: Throwable): Boolean = causeChain(throwable).exists {
    case _: SocketTimeoutException => true
    case _: ConnectException       => true
    case socketFailure: SocketException =>
      Option(socketFailure.getMessage).exists(_.toLowerCase.contains("reset"))
    case _ => false
  }
}

// Tests assert with plain `assert`; a failed assertion throws AssertionError, which is NonFatal and so is caught at the
// Runner edge and reported as a (terminal) failure.
object Check {
  /** Requires `operation` to throw `E` and returns the exception for message assertions. */
  def intercept[E <: Throwable: ClassTag](operation: => Unit): E = {
    val expected = classTag[E].runtimeClass
    val caught: Option[Throwable] =
      try {
        operation
        None
      } catch {
        case NonFatal(throwable) => Some(throwable)
      }
    caught match {
      case Some(throwable) if expected.isInstance(throwable) =>
        throwable.asInstanceOf[E]
      case Some(throwable) =>
        throw new AssertionError(
          s"expected ${expected.getName} but got ${throwable.getClass.getName}: " +
            throwable.getMessage,
          throwable)
      case None =>
        throw new AssertionError(
          s"expected ${expected.getName} to be thrown, but nothing was")
    }
  }
}

// `Column[T]` carries the Scala type the column reads back as, so typed row access (`row.get(CoreTable.long0): Long`)
// is compiler-checked. `literalAt(rowIndex)` is a pure function of the row index, so generated data is reproducible.
// Value generation lives on the column, which keeps RowGenerator a plain iteration with no knowledge of types.
final case class Column[T](columnName: String, sqlType: String, literalAt: Int => String)

sealed trait Schema {
  def tableColumns: Seq[Column[_]]
  def columnNames: Seq[String] = tableColumns.map(_.columnName)
}

/** Typed row access, keyed by the column's name: `row.get(CoreTable.long0)` returns a `Long`. */
object Rows {
  implicit class TypedRow(val row: Row) extends AnyVal {
    def get[T](column: Column[T]): T = row.getAs[T](column.columnName)
  }
}

// A representative core table with one column per common data type and a string-encoded date. Tests reference columns
// through these handles, so a column rename propagates to every caller.
object CoreTable extends Schema {
  val long0:    Column[Long]    = Column("foo_col_long",    "bigint",  rowIndex => rowIndex.toString)
  val int0:     Column[Int]     = Column("foo_col_int",     "int",     rowIndex => rowIndex.toString)
  val string0:  Column[String]  = Column("foo_col_string",  "string",  rowIndex => s"'row-$rowIndex'")
  val double0:  Column[Double]  = Column("foo_col_double",  "double",  rowIndex => s"$rowIndex.5")
  val boolean0: Column[Boolean] =
    Column("foo_col_boolean", "boolean", rowIndex => if (rowIndex % 2 == 0) "true" else "false")
  val date0: Column[String] =
    Column("foo_col_date", "string", rowIndex => s"'${CoreTable.dateLiteral(rowIndex)}'")
  def tableColumns: Seq[Column[_]] = Seq(long0, int0, string0, double0, boolean0, date0)

  private val DateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
  private val DateEpoch  = LocalDateTime.of(2024, 1, 1, 0, 0)

  /** Deterministic YYYY-MM-DD-HH date value (one hour per row), formatted via java.time. */
  def dateLiteral(rowIndex: Int): String =
    DateEpoch.plusHours((rowIndex - 1).toLong).format(DateFormat)
}

// A schema containing complex/nested types: a struct, an array, a map, and a struct-in-struct. Struct/array read back
// as Row/Seq; map as a Map. `id` is first so it is the ordering key.
object NestedTable extends Schema {
  val id: Column[Long] =
    Column("id", "bigint", rowIndex => rowIndex.toString)
  val s: Column[Row] =
    Column(
      "s",
      "struct<x:int,y:string>",
      rowIndex => s"named_struct('x', $rowIndex, 'y', 'row-$rowIndex')")
  val arr: Column[Seq[Int]] =
    Column("arr", "array<int>", rowIndex => s"array($rowIndex, ${rowIndex + 1})")
  val m: Column[Map[String, Int]] =
    Column("m", "map<string,int>", rowIndex => s"map('k', $rowIndex)")
  val nested: Column[Row] =
    Column(
      "nested",
      "struct<inner:struct<z:int>>",
      rowIndex => s"named_struct('inner', named_struct('z', $rowIndex))")
  def tableColumns: Seq[Column[_]] = Seq(id, s, arr, m, nested)

  val columnDefinitions: String =
    "id bigint, s struct<x:int,y:string>, arr array<int>, m map<string,int>, nested struct<inner:struct<z:int>>"
}

// A schema for the common scalar types, including literals for null, floating-point, boundary, unicode, and empty
// string cases.
object TypesTable extends Schema {
  val id: Column[Long] =
    Column("id", "bigint", rowIndex => rowIndex.toString)
  val n: Column[Int] =
    Column("n", "int", rowIndex => rowIndex.toString)
  val x: Column[Double] =
    Column("x", "double", rowIndex => s"$rowIndex.5")
  val dec: Column[JavaBigDecimal] =
    Column("dec", "decimal(10,2)", rowIndex => s"CAST($rowIndex.50 AS decimal(10,2))")
  val str: Column[String] =
    Column("str", "string", rowIndex => s"'row-$rowIndex'")
  val bin: Column[Array[Byte]] =
    Column("bin", "binary", rowIndex => s"CAST('bin-$rowIndex' AS binary)")
  val dt: Column[Date] =
    Column(
      "dt",
      "date",
      rowIndex => s"DATE '${DateEpoch.plusDays((rowIndex - 1).toLong)}'")
  val ts: Column[Timestamp] =
    Column(
      "ts",
      "timestamp",
      rowIndex =>
        s"TIMESTAMP '${TimestampEpoch.plusHours((rowIndex - 1).toLong).format(TimestampFormat)}'")
  val tsntz: Column[LocalDateTime] =
    Column(
      "tsntz",
      "timestamp_ntz",
      rowIndex =>
        s"TIMESTAMP_NTZ '${TimestampEpoch.plusHours((rowIndex - 1).toLong).format(TimestampFormat)}'")
  def tableColumns: Seq[Column[_]] = Seq(id, n, x, dec, str, bin, dt, ts, tsntz)

  val columnDefinitions: String =
    "id bigint, n int, x double, dec decimal(10,2), str string, bin binary, dt date, ts timestamp, " +
      "tsntz timestamp_ntz"

  private val DateEpoch = LocalDate.of(2024, 1, 1)
  private val TimestampEpoch = LocalDateTime.of(2024, 1, 1, 0, 0)
  private val TimestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
}

object RowGenerator {
  /** VALUES clause for `numberOfRows` deterministic rows, one literal per column. */
  def valuesClause(schema: Schema, numberOfRows: Int): String =
    (1 to numberOfRows).map { rowIndex =>
      schema.tableColumns.map(column => column.literalAt(rowIndex)).mkString("(", ", ", ")")
    }.mkString("VALUES ", ", ", "")
}

/**
 * What a step's validation thunk sees: the live table, its rows before and after the step, and the table's snapshot
 * (commit) count before and after, so a test can assert the delta in both data and commits (e.g. "a no-match UPDATE
 * still commits exactly one snapshot").
 */
final case class StepView[S <: Schema](
  spark:           SparkSession,
  table:           String,
  schema:          S,
  before:          Seq[Row],
  after:           Seq[Row],
  snapshotsBefore: Long,
  snapshotsAfter:  Long
)

final case class TableState(rows: Seq[Row], snapshotCount: Long)

/** A fresh table after its reusable preparation has completed. */
final case class PreparedTable[S <: Schema](
  spark:                 SparkSession,
  name:                  String,
  schema:                S,
  preparedRows:          Seq[Row],
  preparedSnapshotCount: Long
) {
  def rows: Seq[Row] = PreparedTable.currentRows(spark, name, schema)
  def snapshotCount: Long = PreparedTable.snapshotCount(spark, name)
  def state: TableState = TableState(rows, snapshotCount)
}

object PreparedTable {
  private[harness] def currentRows[S <: Schema](spark: SparkSession, table: String, schema: S): Seq[Row] = {
    val columns = schema.columnNames.mkString(", ")
    spark.sql(s"SELECT $columns FROM $table ORDER BY ${schema.columnNames.head}").collect().toSeq
  }

  private[harness] def snapshotCount(spark: SparkSession, table: String): Long =
    spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0)
}

/** One preparation step and its validation. */
final case class Step[S <: Schema](
  label:    String,
  execute:  (SparkSession, String, S) => Unit,
  validate: StepView[S] => Unit
)

/** An immutable, typed sequence of table-preparation steps. */
final class TableTest[S <: Schema] private (val schema: S, val steps: Vector[Step[S]]) {
  private def add(step: Step[S]): TableTest[S] = new TableTest(schema, steps :+ step)

  // The default validator asserts the seed actually appended `numberOfRows` rows. This defends the case assertions
  // from a vacuous pass on an empty or short baseline.
  def insert(numberOfRows: Int)(
      validate: StepView[S] => Unit = view => assert(
        view.after.size == view.before.size + numberOfRows,
        s"seed insert($numberOfRows) expected ${view.before.size + numberOfRows} rows, got ${view.after.size}")
  ): TableTest[S] =
    add(Step(s"insert($numberOfRows)", (spark, table, schema) =>
      spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(schema, numberOfRows)}"), validate))

  /** Run an arbitrary preparation step, then validate its result. */
  def step(label: String)(mutate: (SparkSession, String) => Unit)
          (validate: StepView[S] => Unit = _ => ()): TableTest[S] =
    add(Step(label, (spark, table, _) => mutate(spark, table), validate))

  /** Run one preparation SQL statement, then validate its result. */
  def sql(label: String)(statement: String => String)
         (validate: StepView[S] => Unit = _ => ()): TableTest[S] =
    step(label)((spark, table) => spark.sql(statement(table)))(validate)

  /**
   * Execute these steps as a reusable preparation, then hand the prepared table to one self-contained test body. The
   * fresh-table lifecycle covers both the preparation and the test body.
   */
  def prepare(ctx: Ctx)(use: PreparedTable[S] => Unit): Unit =
    withTable(ctx) { (table, markTableCreated) =>
    val (preparedRows, preparedSnapshotCount) =
      steps.zipWithIndex.foldLeft((Seq.empty[Row], 0L)) {
        case ((beforeRows, beforeSnapshots), (step, stepIndex)) =>
        step.execute(ctx.spark, table, schema)
        if (stepIndex == 0) {
          markTableCreated()
        }
        val afterRows = PreparedTable.currentRows(ctx.spark, table, schema)
        val afterSnapshots = PreparedTable.snapshotCount(ctx.spark, table)
        step.validate(
          StepView(
            ctx.spark,
            table,
            schema,
            beforeRows,
            afterRows,
            beforeSnapshots,
            afterSnapshots))
        (afterRows, afterSnapshots)
      }
    use(PreparedTable(ctx.spark, table, schema, preparedRows, preparedSnapshotCount))
  }

  // Gives the preparation a unique table name and drops that table after the test. Cleanup starts only after the first
  // preparation step creates the table, so a name conflict preserves the pre-existing table. A test failure stays
  // primary, and a cleanup failure is attached to it as a suppressed exception.
  private def withTable(ctx: Ctx)(use: (String, () => Unit) => Unit): Unit = {
    val table = TableTest.nextQualifiedTableName(ctx.namespace)
    OwnedTableLifecycle.withOwnership(
      ctx.spark.sql(s"DROP TABLE IF EXISTS $table"))(
      markTableCreated => use(table, markTableCreated))
  }

}

private[harness] object OwnedTableLifecycle {
  /**
   * Runs `use`, then runs `cleanUp` on every outcome. A failure from `use` is the failure the caller sees, with a
   * cleanup failure attached to it as a suppressed exception. When `use` returns normally a cleanup failure is the
   * failure the caller sees, so cleanup that silently fails cannot pass for a clean run.
   */
  def withCleanup(cleanUp: => Unit)(use: => Unit): Unit = {
    var primaryFailure: Option[Throwable] = None
    try {
      use
    } catch {
      case failure: Throwable =>
        primaryFailure = Some(failure)
        throw failure
    } finally {
      try {
        cleanUp
      } catch {
        case cleanupFailure: Throwable =>
          primaryFailure match {
            case Some(failure) => failure.addSuppressed(cleanupFailure)
            case None          => throw cleanupFailure
          }
      }
    }
  }

  /**
   * Runs `use` with a mark it calls once the table exists. `dropOwnedTable` runs only when that mark was set, so a
   * create that fails leaves whatever already answered to the name untouched.
   */
  def withOwnership(dropOwnedTable: => Unit)(use: (() => Unit) => Unit): Unit = {
    var tableCreated = false
    withCleanup(if (tableCreated) dropOwnedTable)(use(() => tableCreated = true))
  }
}

object TableTest {
  private val counter = new AtomicInteger(0)

  def apply[S <: Schema](schema: S): TableTest[S] = new TableTest(schema, Vector.empty)
  def seedCounter(value: Int): Unit = counter.set(value)

  private[harness] def nextQualifiedTableName(namespace: String): String =
    s"$namespace.t_${UUID.randomUUID().toString.replace("-", "")}_${counter.incrementAndGet()}"
}

/** An immutable recipe that prepares one fresh table for each test case. */
final case class TablePreparation[S <: Schema](
  label: String,
  preparation: TableTest[S],
  casePrefix: String = "",
  afterTest: PreparedTable[S] => Unit = (_: PreparedTable[S]) => ()
) {
  /**
   * Build the case that runs `body` against one freshly prepared table. The case ID combines the preparation's prefix
   * and label with `caseName`, so one test body yields a separate case on every preparation it runs on.
   */
  def test(caseName: String)(body: PreparedTable[S] => Unit): TestCase =
    TestCase(
      s"$casePrefix$caseName @ $label",
      context => preparation.prepare(context) { table =>
        OwnedTableLifecycle.withCleanup(afterTest(table))(body(table))
      })
}

final case class DmlTestCase[S <: Schema](
  id: String,
  run: PreparedTable[S] => Unit,
  knownBugReason: Option[String] = None
) {
  /** Build the case that runs this operation against a table `preparation` produces. */
  def runOn(preparation: TablePreparation[S]): TestCase =
    preparation
      .test(id)(run)
      .copy(knownBugReason = knownBugReason)
}
