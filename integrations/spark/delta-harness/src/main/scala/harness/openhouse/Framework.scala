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

// =====================================================================================
// Delta-test harness against the real OpenHouse catalog.
//
// A test is a TYPED PIPELINE: `TableTest[S <: Schema]`. The type parameter declares which
// table implementation the test depends on, and every step references that schema's columns
// through typed handles — so the compiler forbids mixing schemas or naming a column the
// schema doesn't declare.
//
// Preparations and operations are BOTH pipeline segments of the same schema, composed with
// `andThen`:
//   * a preparation prefix  (create+seed, and later RTAS / drop+undrop) yields a known state,
//   * an operation suffix   (delete / update / merge / insert ...) runs on that state.
// The test set is `preparations x operations`. RTAS wires into every DML test by joining the
// preparations list; no operation changes. (RTAS is not built yet — only the seam is.)
//
// Catalog wiring is copied from OpenHouseLocalServer + TestSparkSessionUtil (composed, not
// extended); no OpenHouse test is altered.
// =====================================================================================

final case class Ctx(spark: SparkSession, namespace: String, restUri: String = "", restToken: String = "")

// Minimal REST client to the embedded OpenHouse server (control-plane ops with no SQL surface:
// lock/unlock). Uses JDK 17's java.net.http; auth is the same Bearer token the Spark catalog uses.
object Rest {
  import java.net.http.{HttpClient, HttpRequest, HttpResponse}
  import java.net.URI
  private lazy val client = HttpClient.newHttpClient()
  private def base(ctx: Ctx, path: String): HttpRequest.Builder =
    HttpRequest.newBuilder(URI.create(ctx.restUri + path))
      .header("Authorization", s"Bearer ${ctx.restToken}")
      .header("Content-Type", "application/json")
  def post(ctx: Ctx, path: String, body: String): (Int, String) = {
    val r = client.send(base(ctx, path).POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString())
    (r.statusCode(), r.body())
  }
  def delete(ctx: Ctx, path: String): (Int, String) = {
    val r = client.send(base(ctx, path).DELETE().build(), HttpResponse.BodyHandlers.ofString())
    (r.statusCode(), r.body())
  }
  def put(ctx: Ctx, path: String, body: String): (Int, String) = {
    val r = client.send(base(ctx, path).PUT(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString())
    (r.statusCode(), r.body())
  }
  def get(ctx: Ctx, path: String): (Int, String) = {
    val r = client.send(base(ctx, path).GET().build(), HttpResponse.BodyHandlers.ofString())
    (r.statusCode(), r.body())
  }
}

// Drives the soft-delete / list / restore lifecycle for the UNDROP preparation axis (Phase 4).
// The customer DROP hard-codes purge=true (a hard delete), so soft-delete is unreachable via the
// Tables API — we trigger it directly on the EMBEDDED real HTS (only available under HARNESS_REAL_HTS=1),
// then restore via the customer-facing Tables API. Endpoints are process-global (one HTS, one tables
// server for the whole run) so they are held here and set once at startup; TableTest steps see only
// (spark, table) and reach the endpoints through this holder.
object HtsAdmin {
  import java.net.http.{HttpClient, HttpRequest, HttpResponse}
  import java.net.URI
  @volatile var htsUri: String = ""      // embedded HTS base (soft-delete + querySoftDeleted)
  @volatile var tablesUri: String = ""   // tables server base (restore, customer-facing)
  @volatile var token: String = ""       // Bearer token for the tables server
  def enabled: Boolean = htsUri.nonEmpty

  private lazy val client = HttpClient.newHttpClient()
  private def send(b: HttpRequest.Builder): (Int, String) = {
    val r = client.send(b.header("Content-Type", "application/json").build(), HttpResponse.BodyHandlers.ofString())
    (r.statusCode(), r.body())
  }

  /** Soft-delete on the embedded HTS (V1 endpoint carries the isSoftDelete flag). No auth (HTS security excluded). */
  def softDelete(db: String, tbl: String): (Int, String) =
    send(HttpRequest.newBuilder(URI.create(s"$htsUri/v1/hts/tables?databaseId=$db&tableId=$tbl&isSoftDelete=true")).DELETE())

  /** Recover the deletedAtMs of a soft-deleted table (needed to restore) from the HTS querySoftDeleted view. */
  def softDeletedAtMs(db: String, tbl: String): Option[Long] = {
    val (code, body) = send(HttpRequest.newBuilder(URI.create(s"$htsUri/hts/tables/querySoftDeleted?databaseId=$db&tableId=$tbl")).GET())
    if (code < 200 || code >= 300) None
    else "\"deletedAtMs\"\\s*:\\s*(\\d+)".r.findFirstMatchIn(body).map(_.group(1).toLong)
  }

  /** Restore via the customer-facing Tables API (PUT .../restore?deletedAtMs=). Requires the Bearer token. */
  def restore(db: String, tbl: String, deletedAtMs: Long): (Int, String) =
    send(HttpRequest.newBuilder(URI.create(s"$tablesUri/v1/databases/$db/tables/$tbl/restore?deletedAtMs=$deletedAtMs"))
      .header("Authorization", s"Bearer $token")
      .PUT(HttpRequest.BodyPublishers.ofString("")))
}

sealed trait Outcome { def label: String }
object Outcome {
  case object Passed extends Outcome { val label = "PASS" }
  final case class Failed(cause: Throwable) extends Outcome {
    val label = "FAIL"
    def retryable: Boolean = Exceptions.isTransient(cause)
    def reason: String = s"${Exceptions.root(cause).getClass.getSimpleName}: ${cause.getMessage}"
  }
  final case class Skipped(reason: String) extends Outcome { val label = "SKIP" }
}

object Exceptions {
  def causeChain(t: Throwable): List[Throwable] = {
    val chain = scala.collection.mutable.ListBuffer[Throwable]()
    var current = t
    while (current != null && !chain.contains(current)) { chain += current; current = current.getCause }
    chain.toList
  }
  def root(t: Throwable): Throwable = causeChain(t).last

  /**
   * Retry ONLY errors we positively recognize as transient. A bare IOException is NOT assumed
   * transient — a FileNotFoundException, an EOFException on a corrupt file, or a permission error
   * is an IOException too, and those are real failures that must surface rather than be retried
   * away. When in doubt, an error is terminal.
   */
  def isTransient(t: Throwable): Boolean = causeChain(t).exists {
    case _: java.net.SocketTimeoutException => true
    case _: java.net.ConnectException       => true
    case e: java.net.SocketException        => Option(e.getMessage).exists(_.toLowerCase.contains("reset"))
    case _                                  => false
  }
}

// Tests assert with plain `assert`; a failed assertion throws AssertionError, which is NonFatal
// and so is caught at the Runner edge and reported as a (terminal) failure.
object Check {
  /**
   * Require `op` to throw exactly `E` — the ACTUAL thrown type is asserted, not merely that
   * *something* threw — and return it so the caller can assert on its message. NonFatal only; a
   * wrong type, or no throw at all, is itself an assertion failure.
   */
  def intercept[E <: Throwable: ClassTag](op: => Unit): E = {
    val expected = classTag[E].runtimeClass
    val caught: Option[Throwable] = try { op; None } catch { case NonFatal(t) => Some(t) }
    caught match {
      case Some(t) if expected.isInstance(t) => t.asInstanceOf[E]
      case Some(t) => throw new AssertionError(s"expected ${expected.getName} but got ${t.getClass.getName}: ${t.getMessage}", t)
      case None    => throw new AssertionError(s"expected ${expected.getName} to be thrown, but nothing was")
    }
  }
}

// ── Schema: columns only. A column owns its deterministic value generator; no stored seed. ──
//
// `Column[T]` carries a phantom type `T` — the Scala type the column reads back as — so typed
// row access (`row.get(CoreTable.long0): Long`) is compiler-checked. `literalAt(rowIndex)` is a
// pure function of the row index, so generated data is reproducible. Value generation lives on
// the column, which keeps RowGenerator a plain iteration with no knowledge of types.
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

// A representative "core" table: one column per common data type. Column NAMES are arbitrary
// literals (decoupled from the Scala handle) — tests reference columns through the handle, so a
// rename here propagates everywhere. Plus an explicit string date-partition field in the widely
// used YYYY-MM-DD-HH form. Columns only; each carries a deterministic generator.
object CoreTable extends Schema {
  val long0:         Column[Long]    = Column("foo_col_long",    "bigint",  rowIndex => rowIndex.toString)
  val int0:          Column[Int]     = Column("foo_col_int",     "int",     rowIndex => rowIndex.toString)
  val string0:       Column[String]  = Column("foo_col_string",  "string",  rowIndex => s"'row-$rowIndex'")
  val double0:       Column[Double]  = Column("foo_col_double",  "double",  rowIndex => s"$rowIndex.5")
  val boolean0:      Column[Boolean] = Column("foo_col_boolean", "boolean", rowIndex => if (rowIndex % 2 == 0) "true" else "false")
  val datePartition: Column[String]  = Column("datepartition",   "string",  rowIndex => s"'${CoreTable.datePartitionLiteral(rowIndex)}'")
  def tableColumns: Seq[Column[_]] = Seq(long0, int0, string0, double0, boolean0, datePartition)

  private val DatePartitionFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
  private val DatePartitionEpoch  = LocalDateTime.of(2024, 1, 1, 0, 0)

  /** Deterministic YYYY-MM-DD-HH partition value (one hour per row), formatted via java.time. */
  def datePartitionLiteral(rowIndex: Int): String =
    DatePartitionEpoch.plusHours((rowIndex - 1).toLong).format(DatePartitionFormat)
}

// A schema exercising complex/nested types: a struct, an array, a map, and a struct-in-struct.
// Struct/array read back as Row/Seq; map as a Map. `id` is first so it is the ordering key.
object NestedTable extends Schema {
  val id:     Column[Long]            = Column("id",     "bigint",                      rowIndex => rowIndex.toString)
  val s:      Column[Row]             = Column("s",      "struct<x:int,y:string>",      rowIndex => s"named_struct('x', $rowIndex, 'y', 'row-$rowIndex')")
  val arr:    Column[Seq[Int]]        = Column("arr",    "array<int>",                  rowIndex => s"array($rowIndex, ${rowIndex + 1})")
  val m:      Column[Map[String, Int]] = Column("m",     "map<string,int>",             rowIndex => s"map('k', $rowIndex)")
  val nested: Column[Row]             = Column("nested", "struct<inner:struct<z:int>>", rowIndex => s"named_struct('inner', named_struct('z', $rowIndex))")
  def tableColumns: Seq[Column[_]] = Seq(id, s, arr, m, nested)

  val columnDefinitions: String =
    "id bigint, s struct<x:int,y:string>, arr array<int>, m map<string,int>, nested struct<inner:struct<z:int>>"
}

// A schema for type-edge coverage: the common scalar types, exercised with nulls, special float
// values, boundary values, and unicode/empty strings.
object TypesTable extends Schema {
  val id:    Column[Long]   = Column("id",    "bigint",        rowIndex => rowIndex.toString)
  val n:     Column[Int]    = Column("n",     "int",           rowIndex => rowIndex.toString)
  val x:     Column[Double] = Column("x",     "double",        rowIndex => s"$rowIndex.5")
  val dec:   Column[java.math.BigDecimal] = Column("dec", "decimal(10,2)", rowIndex => s"CAST($rowIndex.50 AS decimal(10,2))")
  val str:   Column[String] = Column("str",   "string",        rowIndex => s"'row-$rowIndex'")
  val bin:   Column[Array[Byte]] = Column("bin", "binary",     rowIndex => s"CAST('bin-$rowIndex' AS binary)")
  val dt:    Column[java.sql.Date] = Column("dt", "date",      rowIndex => s"DATE '2024-01-0$rowIndex'")
  val ts:    Column[java.sql.Timestamp] = Column("ts", "timestamp", rowIndex => s"TIMESTAMP '2024-01-01 0$rowIndex:00:00'")
  val tsntz: Column[java.time.LocalDateTime] = Column("tsntz", "timestamp_ntz", rowIndex => s"TIMESTAMP_NTZ '2024-01-01 0$rowIndex:00:00'")
  def tableColumns: Seq[Column[_]] = Seq(id, n, x, dec, str, bin, dt, ts, tsntz)

  val columnDefinitions: String =
    "id bigint, n int, x double, dec decimal(10,2), str string, bin binary, dt date, ts timestamp, tsntz timestamp_ntz"
}

object RowGenerator {
  /** VALUES clause for `numberOfRows` deterministic rows, one literal per column. */
  def valuesClause(schema: Schema, numberOfRows: Int): String =
    (1 to numberOfRows).map { rowIndex =>
      schema.tableColumns.map(column => column.literalAt(rowIndex)).mkString("(", ", ", ")")
    }.mkString("VALUES ", ", ", "")
}

/**
 * What a step's validation thunk sees: the live table, its rows before and after the step, and
 * the table's snapshot (commit) count before and after — so a test can assert the delta in both
 * data and commits (e.g. "a no-match UPDATE still commits exactly one snapshot").
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

/** One pipeline step: mutate the live table, then validate it against before/after. */
final case class Step[S <: Schema](
  label:    String,
  execute:  (SparkSession, String, S) => Unit,
  validate: StepView[S] => Unit
)

/**
 * An immutable, typed pipeline. Build a preparation prefix and an operation suffix, then
 * `run` executes the steps in order on one fresh, always-dropped table, validating each step.
 */
final class TableTest[S <: Schema] private (val schema: S, val steps: Vector[Step[S]]) {
  private def add(step: Step[S]): TableTest[S] = new TableTest(schema, steps :+ step)

  /** Append another same-schema pipeline (this is how prep prefixes join operation suffixes). */
  def andThen(next: TableTest[S]): TableTest[S] = new TableTest(schema, steps ++ next.steps)

  // The default validator asserts the seed actually appended `numberOfRows` rows. This defends the
  // relative-delta operation assertions from a vacuous pass on an empty/short baseline.
  def insert(numberOfRows: Int)(
      validate: StepView[S] => Unit = view => assert(
        view.after.size == view.before.size + numberOfRows,
        s"seed insert($numberOfRows) expected ${view.before.size + numberOfRows} rows, got ${view.after.size}")
  ): TableTest[S] =
    add(Step(s"insert($numberOfRows)", (spark, table, schema) =>
      spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(schema, numberOfRows)}"), validate))

  def delete(predicate: S => String)(validate: StepView[S] => Unit = _ => ()): TableTest[S] =
    add(Step("delete", (spark, table, schema) =>
      spark.sql(s"DELETE FROM $table WHERE ${predicate(schema)}"), validate))

  /** General operation step: run an arbitrary mutation on the table, then validate the delta. */
  def step(label: String)(mutate: (SparkSession, String) => Unit)
          (validate: StepView[S] => Unit = _ => ()): TableTest[S] =
    add(Step(label, (spark, table, _) => mutate(spark, table), validate))

  /** Operation step whose mutation is a single SQL statement (the table name is supplied). */
  def sql(label: String)(statement: String => String)
         (validate: StepView[S] => Unit = _ => ()): TableTest[S] =
    step(label)((spark, table) => spark.sql(statement(table)))(validate)

  /** Read/assert-only step: no mutation, so before == after; used for the read paths. */
  def check(label: String)(validate: StepView[S] => Unit): TableTest[S] =
    step(label)((_, _) => ())(validate)

  // Execute the pipeline on a fresh, always-dropped table. Each step's `before` is the previous
  // step's `after` (an empty/zero baseline for the first step), so rows and commits are only ever
  // read AFTER a step has run — on a table a prior step created. There is no existence guard: a
  // query against a missing table loudly fails, which is the correct behavior.
  def run(ctx: Ctx): Unit = withTable(ctx) { table =>
    steps.foldLeft((Seq.empty[Row], 0L)) { case ((beforeRows, beforeSnapshots), step) =>
      step.execute(ctx.spark, table, schema)
      val afterRows = currentRows(ctx.spark, table)
      val afterSnapshots = snapshotCount(ctx.spark, table)
      step.validate(StepView(ctx.spark, table, schema, beforeRows, afterRows, beforeSnapshots, afterSnapshots))
      (afterRows, afterSnapshots)
    }
  }

  // The one table-lifecycle primitive: hand `use` a fresh table name and always drop it afterward.
  // The teardown drop is guarded so a drop failure can't mask the real failure from `use`.
  private def withTable(ctx: Ctx)(use: String => Unit): Unit = {
    val table = s"${ctx.namespace}.t_${TableTest.counter.incrementAndGet()}"
    ctx.spark.sql(s"DROP TABLE IF EXISTS $table") // ensure absent
    try use(table)
    finally try ctx.spark.sql(s"DROP TABLE IF EXISTS $table") catch { case NonFatal(_) => () }
  }

  // Rows selected by the schema's columns, ordered by the key (first) column for deterministic
  // comparison. Ordering by the key (not all columns) keeps this valid for schemas with columns
  // that aren't orderable, e.g. a map.
  private def currentRows(spark: SparkSession, table: String): Seq[Row] = {
    val columns = schema.columnNames.mkString(", ")
    spark.sql(s"SELECT $columns FROM $table ORDER BY ${schema.columnNames.head}").collect().toSeq
  }

  private def snapshotCount(spark: SparkSession, table: String): Long =
    spark.sql(s"SELECT count(*) FROM $table.snapshots").collect()(0).getLong(0)
}

object TableTest {
  private val counter = new java.util.concurrent.atomic.AtomicInteger(0)
  def apply[S <: Schema](schema: S): TableTest[S] = new TableTest(schema, Vector.empty)
}
