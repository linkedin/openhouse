package harness

import org.apache.spark.sql.Row

/**
 * Table fixtures used by the foundation scenarios: the core table shape, row helpers, file formats, standard seed,
 * standard unpartitioned preparations, and late-bound Spark data source.
 *
 * Capability layers build their specialized preparations from these table primitives in the layer that first uses
 * them.
 */
trait TableTestFixtures {

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

  // A layout belongs to its preparation, so one test body can run on each table shape the scenario declares.
  protected val columnDefinitions =
    "foo_col_long bigint, foo_col_int int, foo_col_string string, foo_col_double double, " +
      "foo_col_boolean boolean, foo_col_date string"

  /** One starting table shape: the label that names it in a case ID and the CREATE statement that builds it. */
  final case class Layout(label: String, create: String => String)

  /**
   * Every file format the harness runs on. This is the single source for a format list, so every format-crossed family
   * covers both columnar formats.
   */
  val fileFormats: List[String] = List("parquet", "orc")

  /** One core table in `format`, shaped by `partitionClause` and labelled for its case IDs. */
  protected def coreLayout(label: String, format: String, partitionClause: String): Layout =
    Layout(
      label,
      table =>
        s"CREATE TABLE $table ($columnDefinitions) USING $dataSource $partitionClause " +
          s"TBLPROPERTIES ('write.format.default'='$format')")

  /** The foundation's unpartitioned core layout in each file format. */
  val coreLayouts: List[Layout] =
    fileFormats.map(format => coreLayout(format, format, ""))

  /**
   * The standard seed writes three deterministic rows with keys 1, 2 and 3. Row `n` holds key `n` in the long column,
   * `n` in the int column, `row-n` in the string column, `n.5` in the double column, `n % 2 == 0` in the boolean
   * column, and the date value `n - 1` hours after `2024-01-01-00`. `RowGenerator` builds those literals from
   * `CoreTable`, so the seed follows a column rename. Every preparation that seeds a core table writes exactly these
   * rows, so a case that starts from a seeded table knows its three starting keys.
   */
  val standardSeedRowCount: Int = 3

  /** Creates a core table under `layout` and leaves it empty. */
  protected def createCoreTable(layout: Layout): TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(layout.create)()

  /** An unpartitioned core table in `format`, created and seeded with the standard rows. */
  protected def preparedStandardTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      createCoreTable(coreLayout(format, format, "")).insert(standardSeedRowCount)())

  /** The standard seeded table in each file format. */
  val preparedCoreFormats: List[TablePreparation[CoreTable.type]] =
    fileFormats.map(preparedStandardTable)

  // The Spark data source used by CREATE TABLE statements. A remote adapter sets this before reading Catalog.cases.
  var dataSource: String = "iceberg"

}
