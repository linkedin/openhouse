package com.linkedin.openhouse.spark.lineage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, Window}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}

import scala.util.Try

/**
 * Turns a Spark logical plan into a [[SqlLineage]] describing which tables were read, which table
 * was written and, for every produced column, which upstream columns it came from and through which
 * expression.
 *
 * Usage:
 * {{{
 *   // from an executed query (see OpenhouseLineageListener)
 *   val lineage = SqlLineageExtractor.extract(queryExecution.analyzed, Some(sqlText))
 *
 *   // or statically, without running the statement
 *   val lineage = SqlLineageExtractor.extractFromSql(spark, "INSERT INTO db.t2 SELECT a, b FROM db.t1")
 * }}}
 *
 * Implementation note: command nodes are inspected through accessor methods rather than case-class
 * pattern matching, see [[PlanAccessors]] for why.
 */
object SqlLineageExtractor {

  private val MaxInlineDepth = 32

  /**
   * Parses and analyses `sqlText` without executing it, then extracts lineage. Safe to call for
   * DML/DDL: only the analyzer runs, so no table is created or written.
   */
  def extractFromSql(spark: SparkSession, sqlText: String): Option[SqlLineage] =
    extract(analyze(spark, sqlText), Some(sqlText))

  /** Analyses `sqlText` against the session catalog without executing it. */
  def analyze(spark: SparkSession, sqlText: String): LogicalPlan = {
    val parsed = spark.sessionState.sqlParser.parsePlan(sqlText)
    spark.sessionState.analyzer.execute(parsed)
  }

  /** Extracts lineage from an analyzed logical plan. Returns [[None]] for plans without lineage. */
  def extract(plan: LogicalPlan, sql: Option[String] = None): Option[SqlLineage] = {
    // `WITH` definitions live on the root `WithCTE` node while the references live in the command's
    // query subtree, so they are collected before the root is unwrapped and handed to the resolver.
    val resolver = new ColumnOriginResolver(ColumnOriginResolver.cteDefinitions(plan))
    val lineage = Try(extractInternal(unwrapWithCte(plan), sql, resolver)).toOption.flatten
    lineage.filter(l => l.outputTable.isDefined || l.inputTables.nonEmpty)
  }

  // scalastyle:off cyclomatic.complexity method.length
  private def extractInternal(
      plan: LogicalPlan,
      sql: Option[String],
      resolver: ColumnOriginResolver): Option[SqlLineage] = {
    val simpleName = plan.getClass.getSimpleName

    if (isV2Write(plan)) {
      val query = PlanAccessors.plan(plan, "query").getOrElse(plan.children.head)
      val target = PlanAccessors.plan(plan, "table").flatMap(resolver.targetRelationRef)
      val targetColumns = PlanAccessors.plan(plan, "table").map(_.output.map(_.name)).getOrElse(Nil)
      val operation = v2WriteOperation(plan, simpleName)
      allNodes(query, resolver).find(_.getClass.getSimpleName == "MergeRows") match {
        case Some(mergeRows) =>
          Some(
            rewrittenMergeLineage(operation, sql, target, targetColumns, query, mergeRows, resolver))
        case None =>
          Some(queryLineage(operation, sql, target, targetColumns, query, resolver))
      }

    } else if (simpleName == "CreateTableAsSelect" || simpleName == "ReplaceTableAsSelect") {
      val query = PlanAccessors.plan(plan, "query").getOrElse(plan.children.head)
      val operation =
        if (simpleName == "CreateTableAsSelect") LineageOperation.CreateTableAsSelect
        else LineageOperation.ReplaceTableAsSelect
      Some(queryLineage(operation, sql, createTableTarget(plan), Nil, query, resolver))

    } else if (simpleName == "InsertIntoStatement") {
      val query = PlanAccessors.plan(plan, "query").getOrElse(plan.children.head)
      val targetPlan = PlanAccessors.plan(plan, "table")
      val overwrite = PlanAccessors.boolean(plan, "overwrite").getOrElse(false)
      val operation =
        if (overwrite) LineageOperation.InsertOverwrite else LineageOperation.InsertInto
      val userCols = PlanAccessors.seq[String](plan, "userSpecifiedCols")
      val targetColumns =
        if (userCols.nonEmpty) userCols else targetPlan.map(_.output.map(_.name)).getOrElse(Nil)
      Some(
        queryLineage(
          operation,
          sql,
          targetPlan.flatMap(resolver.targetRelationRef),
          targetColumns,
          query,
          resolver))

    } else if (isMerge(plan)) {
      Some(mergeLineage(plan, sql, resolver))

    } else if (simpleName.startsWith("UpdateTable") || simpleName.startsWith("UpdateIcebergTable")) {
      Some(updateLineage(plan, sql, resolver))

    } else if (simpleName.startsWith("DeleteFrom")) {
      Some(deleteLineage(plan, sql, resolver))

    } else if (simpleName == "CreateViewCommand") {
      val query = PlanAccessors
        .plan(plan, "plan")
        .orElse(PlanAccessors.plan(plan, "child"))
        .orElse(firstInnerPlan(plan))
      query.map { q =>
        queryLineage(
          LineageOperation.CreateViewAsSelect,
          sql,
          viewTarget(plan),
          Nil,
          q,
          resolver)
      }

    } else if (isV1Write(simpleName)) {
      val query = PlanAccessors
        .plan(plan, "query")
        .orElse(firstInnerPlan(plan))
      query.map { q =>
        val target = v1WriteTarget(plan)
        val overwrite = PlanAccessors.boolean(plan, "overwrite").getOrElse(false)
        val operation =
          if (simpleName.startsWith("Create")) LineageOperation.CreateTableAsSelect
          else if (overwrite) LineageOperation.InsertOverwrite
          else LineageOperation.InsertInto
        queryLineage(operation, sql, target, Nil, q, resolver)
      }

    } else {
      Some(queryLineage(LineageOperation.Select, sql, None, Nil, plan, resolver))
    }
  }
  // scalastyle:on cyclomatic.complexity method.length

  /** Lineage of a plan whose produced rows come from a single query subtree. */
  private def queryLineage(
      operation: String,
      sql: Option[String],
      target: Option[TableRef],
      targetColumns: Seq[String],
      query: LogicalPlan,
      resolver: ColumnOriginResolver): SqlLineage = {
    val origins = resolver.resolve(query)
    val definitions = collectDefinitions(query, resolver)
    // A row-level write appends internal row-tracking columns (`_file`, `_pos`, ...) to its query so
    // Spark can locate the rows it rewrites. The target schema defines the real output columns.
    val outputAttributes =
      if (targetColumns.nonEmpty) query.output.take(targetColumns.size) else query.output
    val columns = outputAttributes.zipWithIndex.map { case (attr, idx) =>
      val expression = inline(attr, definitions)
      ColumnLineage(
        column = targetColumns.lift(idx).getOrElse(attr.name),
        sources = origins.getOrElse(attr.exprId, Nil),
        transformation = resolver.render(expression),
        transformationType = resolver.classify(expression))
    }
    SqlLineage(
      operation = operation,
      sql = sql,
      outputTable = target,
      inputTables = inputTables(query, resolver),
      columnLineage = columns,
      conditions = conditions(query, origins, definitions, resolver))
  }

  /**
   * Lineage of a `MERGE INTO` that the analyser already rewrote into a `ReplaceData`/`WriteDelta`
   * over a `MergeRows` node (Spark 3.4+).
   *
   * The per-branch assignment lists of the original statement survive as one output expression row
   * per branch, so each target column is folded back together from every branch that can produce it
   * - the same shape [[mergeLineage]] reports for the un-rewritten node.
   */
  private def rewrittenMergeLineage(
      operation: String,
      sql: Option[String],
      target: Option[TableRef],
      targetColumns: Seq[String],
      query: LogicalPlan,
      mergeRows: LogicalPlan,
      resolver: ColumnOriginResolver): SqlLineage = {
    val origins = resolver.resolve(query)
    val definitions = collectDefinitions(query, resolver)
    val perBranch = ColumnOriginResolver.mergeBranches(mergeRows).flatMap {
      case (_, outputs) =>
        outputs.zipWithIndex.flatMap { case (value, idx) =>
          targetColumns.lift(idx).map { name =>
            val expression = inline(value, definitions)
            ColumnLineage(
              column = name,
              sources = resolver.sourcesOf(value, origins),
              transformation = resolver.render(expression),
              transformationType = resolver.classify(expression))
          }
        }
    }
    SqlLineage(
      operation = operation,
      sql = sql,
      outputTable = target,
      inputTables = inputTables(query, resolver),
      columnLineage = mergeColumns(perBranch),
      conditions = conditions(query, origins, definitions, resolver))
  }

  private def mergeLineage(
      plan: LogicalPlan,
      sql: Option[String],
      resolver: ColumnOriginResolver): SqlLineage = {    val target = PlanAccessors.plan(plan, "targetTable")
    val source = PlanAccessors.plan(plan, "sourceTable")
    val origins = resolver.resolve(plan)
    val definitions = collectDefinitions(plan, resolver)

    val actions = PlanAccessors.seq[AnyRef](plan, "matchedActions") ++
      PlanAccessors.seq[AnyRef](plan, "notMatchedActions") ++
      PlanAccessors.seq[AnyRef](plan, "notMatchedBySourceActions")

    val perColumn = actions
      .flatMap(action => PlanAccessors.seq[AnyRef](action, "assignments"))
      .flatMap { assignment =>
        for {
          key <- PlanAccessors.expression(assignment, "key")
          value <- PlanAccessors.expression(assignment, "value")
        } yield {
          val expression = inline(value, definitions)
          val name = key match {
            case named: NamedExpression => named.name
            case other => resolver.render(other)
          }
          ColumnLineage(
            column = name,
            sources = resolver.sourcesOf(value, origins),
            transformation = resolver.render(expression),
            transformationType = resolver.classify(expression))
        }
      }

    val mergeCondition = PlanAccessors.expression(plan, "mergeCondition").map { condition =>
      ConditionLineage(
        ConditionKind.MergeOn,
        resolver.render(condition),
        resolver.sourcesOf(condition, origins))
    }
    val inputs =
      (target.toSeq ++ source.toSeq).flatMap(inputTables(_, resolver)).distinct

    SqlLineage(
      operation = LineageOperation.Merge,
      sql = sql,
      outputTable = target.flatMap(resolver.targetRelationRef),
      inputTables = inputs,
      columnLineage = mergeColumns(perColumn),
      conditions = mergeCondition.toSeq ++
        source.toSeq.flatMap(conditions(_, origins, definitions, resolver)))
  }

  private def updateLineage(
      plan: LogicalPlan,
      sql: Option[String],
      resolver: ColumnOriginResolver): SqlLineage = {
    val target = PlanAccessors.plan(plan, "table")
    val origins = resolver.resolve(plan)
    val definitions = collectDefinitions(plan, resolver)
    val columns = PlanAccessors
      .seq[AnyRef](plan, "assignments")
      .flatMap { assignment =>
        for {
          key <- PlanAccessors.expression(assignment, "key")
          value <- PlanAccessors.expression(assignment, "value")
        } yield {
          val expression = inline(value, definitions)
          ColumnLineage(
            column = key match {
              case named: NamedExpression => named.name
              case other => resolver.render(other)
            },
            sources = resolver.sourcesOf(value, origins),
            transformation = resolver.render(expression),
            transformationType = resolver.classify(expression))
        }
      }
    SqlLineage(
      operation = LineageOperation.Update,
      sql = sql,
      outputTable = target.flatMap(resolver.targetRelationRef),
      inputTables = target.toSeq.flatMap(inputTables(_, resolver)),
      columnLineage = columns,
      conditions = conditionOf(plan, origins, resolver).toSeq)
  }

  private def deleteLineage(
      plan: LogicalPlan,
      sql: Option[String],
      resolver: ColumnOriginResolver): SqlLineage = {
    val target = PlanAccessors.plan(plan, "table").orElse(plan.children.headOption)
    val origins = resolver.resolve(plan)
    SqlLineage(
      operation = LineageOperation.Delete,
      sql = sql,
      outputTable = target.flatMap(resolver.targetRelationRef),
      inputTables = target.toSeq.flatMap(inputTables(_, resolver)),
      columnLineage = Nil,
      conditions = conditionOf(plan, origins, resolver).toSeq)
  }

  private def conditionOf(
      plan: LogicalPlan,
      origins: ColumnOriginResolver#OriginMap,
      resolver: ColumnOriginResolver): Option[ConditionLineage] = {
    val expression = PlanAccessors
      .expression(plan, "condition")
      .orElse(PlanAccessors.option[Expression](plan, "condition"))
    expression.map { c =>
      ConditionLineage(ConditionKind.Filter, resolver.render(c), resolver.sourcesOf(c, origins))
    }
  }

  /** A target column can be assigned by several MERGE branches; fold them into one entry. */
  private def mergeColumns(columns: Seq[ColumnLineage]): Seq[ColumnLineage] =
    columns
      .groupBy(_.column)
      .toSeq
      .sortBy { case (name, _) => columns.indexWhere(_.column == name) }
      .map { case (name, entries) =>
        ColumnLineage(
          column = name,
          sources = entries.flatMap(_.sources).distinct,
          transformation = entries.map(_.transformation).distinct.mkString(" | "),
          transformationType = entries.map(_.transformationType).distinct match {
            case Seq(single) => single
            case _ => TransformationType.Expression
          })
      }

  /** Indirect lineage: columns that filter or group rows without being projected. */
  private def conditions(
      plan: LogicalPlan,
      origins: ColumnOriginResolver#OriginMap,
      definitions: Map[ExprId, Expression],
      resolver: ColumnOriginResolver): Seq[ConditionLineage] =
    allNodes(plan, resolver).flatMap {
      case filter: Filter =>
        Seq(condition(ConditionKind.Filter, filter.condition, origins, definitions, resolver))
      case join: Join =>
        join.condition
          .map(c => condition(ConditionKind.Join, c, origins, definitions, resolver))
          .toSeq
      case aggregate: Aggregate if aggregate.groupingExpressions.nonEmpty =>
        aggregate.groupingExpressions.map(g =>
          condition(ConditionKind.GroupBy, g, origins, definitions, resolver))
      case _ => Nil
    }.filter(_.columns.nonEmpty).distinct

  private def condition(
      kind: String,
      expression: Expression,
      origins: ColumnOriginResolver#OriginMap,
      definitions: Map[ExprId, Expression],
      resolver: ColumnOriginResolver): ConditionLineage =
    ConditionLineage(
      kind,
      resolver.render(inline(expression, definitions)),
      resolver.sourcesOf(expression, origins))

  /** Distinct tables read anywhere under `plan`, including inside subqueries. */
  private def inputTables(plan: LogicalPlan, resolver: ColumnOriginResolver): Seq[TableRef] =
    allNodes(plan, resolver).flatMap(resolver.relationRef).distinct

  /**
   * Every node of the plan, descending into subquery expressions as well as into children, and
   * following `CTERelationRef` placeholders into the subtree they stand for.
   */
  private def allNodes(plan: LogicalPlan, resolver: ColumnOriginResolver): Seq[LogicalPlan] =
    allNodes(plan, resolver.rootCteDefinitions ++ ColumnOriginResolver.cteDefinitions(plan), Set.empty)

  private def allNodes(
      plan: LogicalPlan,
      cteDefs: Map[Long, LogicalPlan],
      visitedCtes: Set[Long]): Seq[LogicalPlan] = {
    val direct = plan.collect { case node => node }
    val nested = direct.flatMap(_.subqueries).flatMap(allNodes(_, cteDefs, visitedCtes))
    val expandedCtes = direct.flatMap { node =>
      ColumnOriginResolver.cteReference(node, cteDefs) match {
        case Some((id, definition)) if !visitedCtes.contains(id) =>
          allNodes(definition, cteDefs, visitedCtes + id)
        case _ => Nil
      }
    }
    (direct ++ nested ++ expandedCtes).distinct
  }

  /**
   * Collects the defining expression of every alias in the plan, so an output attribute can be
   * expanded back into the full expression that produced it.
   */
  private def collectDefinitions(
      plan: LogicalPlan,
      resolver: ColumnOriginResolver): Map[ExprId, Expression] =
    allNodes(plan, resolver).flatMap {
      case project: Project => namedDefinitions(project.projectList)
      case aggregate: Aggregate => namedDefinitions(aggregate.aggregateExpressions)
      case window: Window => namedDefinitions(window.windowExpressions)
      case _ => Nil
    }.toMap

  private def namedDefinitions(expressions: Seq[NamedExpression]): Seq[(ExprId, Expression)] =
    expressions.collect {
      case named if !named.isInstanceOf[AttributeReference] => named.exprId -> named
    }

  /**
   * Substitutes attribute references by the expressions that defined them, so that
   * `INSERT INTO t SELECT price * qty AS total` reports `(price * qty)` rather than the opaque
   * `total` attribute Spark leaves on the write node.
   *
   * The cast the analyzer adds on top of a write projection to match the target schema is dropped:
   * it is Spark plumbing rather than user intent, and keeping it would misreport a plain column copy
   * as a computed column. Casts written by the user further down the expression tree are preserved.
   */
  private def inline(expression: Expression, definitions: Map[ExprId, Expression]): Expression =
    stripCasts(inlineDefinitions(expression, definitions, 0))

  private def inlineDefinitions(
      expression: Expression,
      definitions: Map[ExprId, Expression],
      depth: Int): Expression = {
    if (depth > MaxInlineDepth) {
      return expression
    }
    val unaliased = expression match {
      case named: NamedExpression if !named.isInstanceOf[AttributeReference] =>
        named.children.headOption.getOrElse(named)
      case other => other
    }
    unaliased match {
      case attr: AttributeReference =>
        definitions.get(attr.exprId) match {
          case Some(definition) => inlineDefinitions(definition, definitions, depth + 1)
          case None => attr
        }
      case other =>
        other.mapChildren(child => inlineDefinitions(child, definitions, depth + 1))
    }
  }

  private def stripCasts(expression: Expression): Expression = expression match {
    case cast: Cast => stripCasts(cast.child)
    case other if other.getClass.getSimpleName == "AnsiCast" =>
      other.children.headOption.map(stripCasts).getOrElse(other)
    case other => other
  }

  /**
   * Commands hide their query subtree under `innerChildren` rather than `children`; the intermediate
   * `Any` keeps scalac from choking on the existential `Seq[QueryPlan[_]]`.
   */
  private def firstInnerPlan(plan: LogicalPlan): Option[LogicalPlan] =
    plan.innerChildren.toList.map(_.asInstanceOf[Any]).collectFirst { case p: LogicalPlan => p }

  private def unwrapWithCte(plan: LogicalPlan): LogicalPlan =
    if (plan.getClass.getSimpleName == "WithCTE") {
      PlanAccessors.plan(plan, "plan").getOrElse(plan)
    } else {
      plan
    }

  private def isV2Write(plan: LogicalPlan): Boolean =
    PlanAccessors.hasMethod(plan, "query") && PlanAccessors.hasMethod(plan, "isByName") &&
      PlanAccessors.hasMethod(plan, "table")

  private def isMerge(plan: LogicalPlan): Boolean =
    PlanAccessors.hasMethod(plan, "targetTable") && PlanAccessors.hasMethod(plan, "sourceTable") &&
      PlanAccessors.hasMethod(plan, "mergeCondition")

  private def isV1Write(simpleName: String): Boolean =
    simpleName == "InsertIntoHadoopFsRelationCommand" ||
      simpleName == "InsertIntoHiveTable" ||
      simpleName == "CreateDataSourceTableAsSelectCommand" ||
      simpleName == "CreateHiveTableAsSelectCommand" ||
      simpleName == "OptimizedCreateHiveTableAsSelectCommand"

  /**
   * Names a V2 write.
   *
   * Spark 3.4+ rewrites `UPDATE` / `DELETE` / `MERGE INTO` into a generic `ReplaceData` or
   * `WriteDelta` while analysing, which would otherwise erase the statement the user actually wrote.
   * Those nodes carry the connector's [[org.apache.spark.sql.connector.write.RowLevelOperation]],
   * whose `command` still names the original operation, so it is preferred when present.
   */
  private def v2WriteOperation(plan: LogicalPlan, simpleName: String): String =
    rowLevelCommand(plan).getOrElse(simpleName match {
      case "AppendData" => LineageOperation.InsertInto
      case "OverwriteByExpression" => LineageOperation.InsertOverwrite
      case "OverwritePartitionsDynamic" => LineageOperation.InsertOverwritePartitions
      case other => other.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase
    })

  private def rowLevelCommand(plan: LogicalPlan): Option[String] =
    PlanAccessors
      .invoke[AnyRef](plan, "operation")
      .flatMap(operation => PlanAccessors.invoke[AnyRef](operation, "command"))
      .map(_.toString)
      .collect {
        case "DELETE" => LineageOperation.Delete
        case "UPDATE" => LineageOperation.Update
        case "MERGE" => LineageOperation.Merge
      }

  private def createTableTarget(plan: LogicalPlan): Option[TableRef] = {
    val catalogName = PlanAccessors
      .invoke[CatalogPlugin](plan, "catalog")
      .orElse(PlanAccessors.plan(plan, "name").flatMap(PlanAccessors.invoke[CatalogPlugin](_, "catalog")))
      .flatMap(c => Try(c.name()).toOption)
    PlanAccessors
      .invoke[Identifier](plan, "tableName")
      .map(id => TableRef(catalogName, id.namespace().toSeq, id.name()))
  }

  private def viewTarget(plan: LogicalPlan): Option[TableRef] =
    PlanAccessors
      .invoke[org.apache.spark.sql.catalyst.TableIdentifier](plan, "name")
      .map(id => TableRef(None, id.database.toSeq, id.table))

  private def v1WriteTarget(plan: LogicalPlan): Option[TableRef] = {
    val catalogTable = PlanAccessors
      .option[CatalogTable](plan, "catalogTable")
      .orElse(PlanAccessors.invoke[CatalogTable](plan, "table"))
      .orElse(PlanAccessors.invoke[CatalogTable](plan, "tableDesc"))
    catalogTable.map(t => TableRef(None, t.identifier.database.toSeq, t.identifier.table))
  }

  /** Exposed for callers that only need the produced attributes of a plan. */
  private[lineage] def outputNames(output: Seq[Attribute]): Seq[String] = output.map(_.name)
}
