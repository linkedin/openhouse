package com.linkedin.openhouse.spark.lineage

/**
 * Data model describing table- and column-level lineage extracted from a Spark logical plan.
 *
 * The model is intentionally free of any transport concern: [[LineageSink]] implementations decide
 * whether a [[SqlLineage]] is logged, emitted onto Kafka, or shipped elsewhere.
 */

/** Fully qualified reference to a table, e.g. `openhouse.db.tbl`. */
case class TableRef(catalog: Option[String], namespace: Seq[String], name: String) {

  def qualifiedName: String = (catalog.toSeq ++ namespace :+ name).mkString(".")

  /** Name without the catalog prefix, e.g. `db.tbl`. */
  def namespacedName: String = (namespace :+ name).mkString(".")

  override def toString: String = qualifiedName
}

object TableRef {

  /** Builds a [[TableRef]] from a multipart identifier such as `Seq("openhouse", "db", "tbl")`. */
  def fromMultipart(parts: Seq[String], catalog: Option[String] = None): TableRef = {
    val cleaned = parts.filter(_ != null).filter(_.nonEmpty)
    cleaned match {
      case Seq() => TableRef(catalog, Nil, LineageConstants.UnknownTable)
      case _ => TableRef(catalog, cleaned.dropRight(1), cleaned.last)
    }
  }

  def unknown: TableRef = TableRef(None, Nil, LineageConstants.UnknownTable)
}

/** Fully qualified reference to a single column of a table. */
case class ColumnRef(table: TableRef, column: String) {

  def qualifiedName: String = s"${table.qualifiedName}.$column"

  override def toString: String = qualifiedName
}

/**
 * Lineage of a single output column.
 *
 * @param column             name of the produced column
 * @param sources            upstream columns this column is derived from; empty for literals
 * @param transformation     SQL rendering of the expression producing the column
 * @param transformationType coarse classification, see [[TransformationType]]
 */
case class ColumnLineage(
    column: String,
    sources: Seq[ColumnRef],
    transformation: String,
    transformationType: String) {

  def isIdentity: Boolean = transformationType == TransformationType.Identity
}

/**
 * Columns that influence which rows are produced without being projected themselves, e.g. the
 * columns of a `WHERE`, `JOIN ... ON`, `GROUP BY` or `MERGE ... ON` clause. This is commonly
 * referred to as indirect (or "influence") lineage.
 *
 * @param kind       see [[ConditionKind]]
 * @param expression SQL rendering of the condition
 * @param columns    upstream columns referenced by the condition
 */
case class ConditionLineage(kind: String, expression: String, columns: Seq[ColumnRef])

/** Complete lineage of one SQL statement. */
case class SqlLineage(
    operation: String,
    sql: Option[String],
    outputTable: Option[TableRef],
    inputTables: Seq[TableRef],
    columnLineage: Seq[ColumnLineage],
    conditions: Seq[ConditionLineage]) {

  /** All upstream columns referenced anywhere in the statement, projected or not. */
  def allSourceColumns: Seq[ColumnRef] =
    (columnLineage.flatMap(_.sources) ++ conditions.flatMap(_.columns)).distinct

  def columnLineageFor(column: String): Option[ColumnLineage] =
    columnLineage.find(_.column.equalsIgnoreCase(column))

  def sourcesOf(column: String): Seq[ColumnRef] =
    columnLineageFor(column).map(_.sources).getOrElse(Nil)

  /** Compact single-line JSON, suitable for structured logging or a Kafka payload. */
  def toJson: String = {
    val sb = new StringBuilder
    sb.append('{')
    sb.append(LineageJson.field("operation", operation)).append(',')
    sql.foreach(s => sb.append(LineageJson.field("sql", LineageJson.singleLine(s))).append(','))
    outputTable.foreach(t =>
      sb.append(LineageJson.field("outputTable", t.qualifiedName)).append(','))
    sb.append(LineageJson.arrayField("inputTables", inputTables.map(_.qualifiedName))).append(',')
    sb.append("\"columnLineage\":[")
    sb.append(
      columnLineage
        .map { cl =>
          "{" + LineageJson.field("column", cl.column) + "," +
            LineageJson.field("transformationType", cl.transformationType) + "," +
            LineageJson.field("transformation", cl.transformation) + "," +
            LineageJson.arrayField("sources", cl.sources.map(_.qualifiedName)) + "}"
        }
        .mkString(","))
    sb.append("],")
    sb.append("\"conditions\":[")
    sb.append(
      conditions
        .map { c =>
          "{" + LineageJson.field("kind", c.kind) + "," +
            LineageJson.field("expression", LineageJson.singleLine(c.expression)) + "," +
            LineageJson.arrayField("columns", c.columns.map(_.qualifiedName)) + "}"
        }
        .mkString(","))
    sb.append("]}")
    sb.toString
  }

  /** Multi-line human readable rendering, used by [[LogLineageSink]]. */
  def toPrettyString: String = {
    val sb = new StringBuilder
    sb.append("OpenHouse lineage\n")
    sb.append(s"  operation   : $operation\n")
    sql.foreach(s => sb.append(s"  sql         : ${LineageJson.singleLine(s)}\n"))
    sb.append(s"  outputTable : ${outputTable.map(_.qualifiedName).getOrElse("-")}\n")
    sb.append(
      s"  inputTables : ${if (inputTables.isEmpty) "-" else inputTables.map(_.qualifiedName).mkString(", ")}\n")
    sb.append("  columns     :\n")
    if (columnLineage.isEmpty) {
      sb.append("      -\n")
    } else {
      columnLineage.foreach { cl =>
        val target = outputTable.map(t => s"${t.qualifiedName}.${cl.column}").getOrElse(cl.column)
        val sources = if (cl.sources.isEmpty) "<none>" else cl.sources.map(_.qualifiedName).mkString(", ")
        sb.append(s"      $target <- $sources  [${cl.transformationType}] ${cl.transformation}\n")
      }
    }
    if (conditions.nonEmpty) {
      sb.append("  conditions  :\n")
      conditions.foreach { c =>
        val cols = if (c.columns.isEmpty) "<none>" else c.columns.map(_.qualifiedName).mkString(", ")
        sb.append(s"      ${c.kind}: ${LineageJson.singleLine(c.expression)} -> $cols\n")
      }
    }
    sb.toString
  }
}

/** Coarse classification of how an output column is computed. */
object TransformationType {

  /** Column is a straight copy (possibly renamed) of a single upstream column. */
  val Identity = "IDENTITY"

  /** Column is a constant; it has no upstream columns. */
  val Literal = "LITERAL"

  /** Column is produced by an aggregate function such as `SUM` or `COUNT`. */
  val Aggregation = "AGGREGATION"

  /** Column is produced by a window function. */
  val Window = "WINDOW"

  /** Column is produced by a generator such as `EXPLODE`. */
  val Generator = "GENERATOR"

  /** Any other scalar expression, e.g. `a * b`, `CASE WHEN ...`, `UPPER(x)`. */
  val Expression = "EXPRESSION"
}

/** Kind of an indirect-lineage condition. */
object ConditionKind {
  val Filter = "FILTER"
  val Join = "JOIN"
  val GroupBy = "GROUP_BY"
  val MergeOn = "MERGE_ON"
  val Sort = "SORT"
}

/** Operation names reported on [[SqlLineage.operation]]. */
object LineageOperation {
  val Select = "SELECT"
  val CreateTableAsSelect = "CREATE_TABLE_AS_SELECT"
  val ReplaceTableAsSelect = "REPLACE_TABLE_AS_SELECT"
  val CreateViewAsSelect = "CREATE_VIEW_AS_SELECT"
  val InsertInto = "INSERT_INTO"
  val InsertOverwrite = "INSERT_OVERWRITE"
  val InsertOverwritePartitions = "INSERT_OVERWRITE_PARTITIONS"
  val Merge = "MERGE_INTO"
  val Update = "UPDATE"
  val Delete = "DELETE"
}

private[lineage] object LineageConstants {
  val UnknownTable = "<unknown>"
}

private[lineage] object LineageJson {

  def field(name: String, value: String): String = s""""$name":"${escape(value)}""""

  def arrayField(name: String, values: Seq[String]): String =
    s""""$name":[${values.map(v => "\"" + escape(v) + "\"").mkString(",")}]"""

  def singleLine(s: String): String = s.replaceAll("\\s+", " ").trim

  def escape(s: String): String = {
    val sb = new StringBuilder
    s.foreach {
      case '"' => sb.append("\\\"")
      case '\\' => sb.append("\\\\")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case c if c < 0x20 => sb.append("\\u%04x".format(c.toInt))
      case c => sb.append(c)
    }
    sb.toString
  }
}
