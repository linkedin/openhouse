package com.linkedin.openhouse.spark.lineage

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, ExprId, Expression, Generator, LeafExpression, Literal, NamedExpression, SubqueryExpression, Unevaluable, WindowExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Generate, LogicalPlan, Project, Union, Window}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{DataType, StringType}

import scala.util.Try

/**
 * Walks a logical plan bottom-up and answers "which upstream table columns does this expression id
 * come from?".
 *
 * The resolver builds a map from every [[ExprId]] visible in the plan to the set of base-table
 * [[ColumnRef]]s that feed it. Leaf relations seed the map with their own columns; every operator
 * that introduces new expression ids ([[Project]], [[Aggregate]], [[Window]], [[Generate]],
 * [[Union]], `Expand`, CTE references, ...) extends it by resolving its expressions against the map
 * produced by its children. Because the map is keyed by expression id rather than by name, aliasing,
 * self joins and repeated column names all resolve correctly.
 */
private[lineage] class ColumnOriginResolver(
    val rootCteDefinitions: Map[Long, LogicalPlan] = Map.empty) {

  type OriginMap = Map[ExprId, Seq[ColumnRef]]

  private val EmptyOrigins: OriginMap = Map.empty

  /**
   * Builds the expression-id to source-column map for the whole plan.
   *
   * `rootCteDefinitions` carries the `WITH` clauses of the enclosing statement: a command's query
   * subtree references them but does not contain them, so they have to be supplied from the root.
   */
  def resolve(plan: LogicalPlan): OriginMap =
    build(plan, rootCteDefinitions ++ ColumnOriginResolver.cteDefinitions(plan))

  /** Source columns feeding an arbitrary expression, deduplicated and stably ordered. */
  def sourcesOf(expression: Expression, origins: OriginMap): Seq[ColumnRef] =
    expression
      .collect { case a: Attribute => a }
      .flatMap(a => origins.getOrElse(a.exprId, Nil))
      .distinct

  /** Source columns feeding any of the given expressions. */
  def sourcesOfAll(expressions: Seq[Expression], origins: OriginMap): Seq[ColumnRef] =
    expressions.flatMap(sourcesOf(_, origins)).distinct

  // scalastyle:off cyclomatic.complexity
  private def build(plan: LogicalPlan, cteDefs: Map[Long, LogicalPlan]): OriginMap = {
    // Correlated and scalar subqueries hang off expressions rather than off `children`, so they have
    // to be folded in explicitly or lineage through `IN (SELECT ...)` would be lost.
    val subqueryOrigins = plan.subqueries.map(build(_, cteDefs)).foldLeft(EmptyOrigins)(_ ++ _)

    val ownOrigins = plan match {
      case relation if relationRef(relation).isDefined =>
        val table = relationRef(relation).get
        relation.output.map(attr => attr.exprId -> Seq(ColumnRef(table, attr.name))).toMap

      case project: Project =>
        val childOrigins = build(project.child, cteDefs)
        childOrigins ++ aliasOrigins(project.projectList, childOrigins)

      case aggregate: Aggregate =>
        val childOrigins = build(aggregate.child, cteDefs)
        childOrigins ++ aliasOrigins(aggregate.aggregateExpressions, childOrigins)

      case window: Window =>
        val childOrigins = build(window.child, cteDefs)
        childOrigins ++ aliasOrigins(window.windowExpressions, childOrigins)

      case generate: Generate =>
        val childOrigins = build(generate.child, cteDefs)
        val generatorSources = sourcesOf(generate.generator, childOrigins)
        childOrigins ++ generate.generatorOutput.map(_.exprId -> generatorSources).toMap

      case union: Union =>
        val childOrigins = union.children.map(build(_, cteDefs))
        val merged = childOrigins.foldLeft(EmptyOrigins)(_ ++ _)
        merged ++ positionalOrigins(union.output, union.children.map(_.output), childOrigins)

      case mergeRows if mergeRows.getClass.getSimpleName == "MergeRows" =>
        val childOrigins = build(mergeRows.children.head, cteDefs)
        val branches = ColumnOriginResolver.mergeBranches(mergeRows)
        childOrigins ++ mergeRows.output.zipWithIndex.map { case (attr, idx) =>
          attr.exprId -> branches.flatMap {
            case (_, outputs) => outputs.lift(idx).toSeq.flatMap(sourcesOf(_, childOrigins))
          }.distinct
        }.toMap

      case other =>
        val childOrigins = other.children.map(build(_, cteDefs))
        val merged = childOrigins.foldLeft(EmptyOrigins)(_ ++ _)
        merged ++ expandOrigins(other, merged) ++ cteRefOrigins(other, cteDefs) ++
          passthroughOrigins(other, merged)
    }

    subqueryOrigins ++ ownOrigins
  }
  // scalastyle:on cyclomatic.complexity

  private def aliasOrigins(expressions: Seq[NamedExpression], childOrigins: OriginMap): OriginMap =
    expressions.map(expr => expr.exprId -> sourcesOf(expr, childOrigins)).toMap

  /**
   * Maps each output attribute to the union of the sources of the i-th output attribute of every
   * branch. Used for `UNION`, where the output attributes are new expression ids covering all
   * branches.
   */
  private def positionalOrigins(
      output: Seq[Attribute],
      branchOutputs: Seq[Seq[Attribute]],
      branchOrigins: Seq[OriginMap]): OriginMap =
    output.zipWithIndex.map { case (attr, idx) =>
      val sources = branchOutputs
        .zip(branchOrigins)
        .flatMap { case (branch, origins) =>
          branch.lift(idx).toSeq.flatMap(a => origins.getOrElse(a.exprId, Nil))
        }
        .distinct
      attr.exprId -> sources
    }.toMap

  /** `Expand` (GROUPING SETS / CUBE / ROLLUP) rewrites output attributes over several projections. */
  private def expandOrigins(plan: LogicalPlan, childOrigins: OriginMap): OriginMap = {
    if (plan.getClass.getSimpleName != "Expand") {
      return EmptyOrigins
    }
    val projections = PlanAccessors
      .seq[scala.collection.Seq[Expression]](plan, "projections")
      .map(_.toList)
    if (projections.isEmpty) EmptyOrigins
    else {
      plan.output.zipWithIndex.map { case (attr, idx) =>
        attr.exprId -> projections.flatMap(p => p.lift(idx).toSeq.flatMap(sourcesOf(_, childOrigins))).distinct
      }.toMap
    }
  }

  /**
   * Spark 3.4+ keeps CTEs as `CTERelationDef` / `CTERelationRef` pairs in the analyzed plan instead
   * of inlining them, so a reference has to be linked back to its definition to keep lineage intact.
   */
  private def cteRefOrigins(plan: LogicalPlan, cteDefs: Map[Long, LogicalPlan]): OriginMap =
    ColumnOriginResolver.cteReference(plan, cteDefs) match {
      case Some((_, defPlan)) =>
        val defOrigins = build(defPlan, cteDefs)
        defOrigins ++ positionalOrigins(plan.output, Seq(defPlan.output), Seq(defOrigins))
      case None => EmptyOrigins
    }

  /**
   * Generic safety net for single-child nodes that re-project their child's output under fresh
   * expression ids (Spark 3.1's `View`, Iceberg's rewritten row-level nodes, ...). Only attributes
   * that could not be resolved otherwise are filled in, so this never overrides a precise mapping.
   */
  private def passthroughOrigins(plan: LogicalPlan, origins: OriginMap): OriginMap = {
    val children = plan.children
    if (children.size != 1 || children.head.output.size != plan.output.size) {
      return EmptyOrigins
    }
    plan.output
      .zip(children.head.output)
      .collect {
        case (out, in) if out.exprId != in.exprId && !origins.contains(out.exprId) &&
          origins.contains(in.exprId) =>
          out.exprId -> origins(in.exprId)
      }
      .toMap
  }

  /** Recognises the leaf nodes that read from a persisted table. */
  def relationRef(plan: LogicalPlan): Option[TableRef] = plan match {    case relation: LogicalRelation => relation.catalogTable.map(catalogTableRef)
    case _ if plan.getClass.getSimpleName == "DataSourceV2Relation" => v2RelationRef(plan)
    case _ if plan.getClass.getSimpleName == "DataSourceV2ScanRelation" =>
      PlanAccessors.invoke[LogicalPlan](plan, "relation").flatMap(v2RelationRef)
    case _ if plan.getClass.getSimpleName == "HiveTableRelation" =>
      PlanAccessors.invoke[CatalogTable](plan, "tableMeta").map(catalogTableRef)
    case _ if plan.getClass.getSimpleName == "UnresolvedRelation" =>
      Some(TableRef.fromMultipart(PlanAccessors.seq[String](plan, "multipartIdentifier")))
    case _ => None
  }

  private def v2RelationRef(plan: AnyRef): Option[TableRef] = {    val catalogName = PlanAccessors
      .option[CatalogPlugin](plan, "catalog")
      .flatMap(c => Try(c.name()).toOption)
    val fromIdentifier = PlanAccessors.option[Identifier](plan, "identifier").map { id =>
      TableRef(catalogName, id.namespace().toSeq, id.name())
    }
    fromIdentifier.orElse {
      // Fall back to the connector-reported name, which for Iceberg is already fully qualified.
      PlanAccessors
        .invoke[AnyRef](plan, "table")
        .flatMap(t => Try(t.getClass.getMethod("name").invoke(t).asInstanceOf[String]).toOption)
        .map(name => TableRef.fromMultipart(name.split('.').toSeq, catalogName))
    }
  }

  private def catalogTableRef(table: CatalogTable): TableRef =
    TableRef(None, table.identifier.database.toSeq, table.identifier.table)

  /**
   * Resolves the table a write command targets. The target is rarely the top node itself: an alias
   * (`MERGE INTO db.t AS t`) or an engine-specific wrapper usually sits in between, so the subtree
   * is searched for the first readable relation.
   */
  def targetRelationRef(plan: LogicalPlan): Option[TableRef] =
    relationRef(plan).orElse(plan.children.iterator.flatMap(targetRelationRef).toStream.headOption)

  /** Classifies how an output column is computed. */
  def classify(expression: Expression): String = {
    val unwrapped = expression match {
      case alias: Alias => alias.child
      case other => other
    }
    if (unwrapped.find(_.isInstanceOf[WindowExpression]).isDefined) {
      TransformationType.Window
    } else if (unwrapped.find(_.isInstanceOf[AggregateExpression]).isDefined) {
      TransformationType.Aggregation
    } else if (unwrapped.find(_.isInstanceOf[Generator]).isDefined) {
      TransformationType.Generator
    } else {
      unwrapped match {
        case _: AttributeReference => TransformationType.Identity
        case _: Literal => TransformationType.Literal
        case other if other.find(_.isInstanceOf[Attribute]).isEmpty => TransformationType.Literal
        case _ => TransformationType.Expression
      }
    }
  }

  /**
   * Best-effort SQL rendering of an expression.
   *
   * Attribute references are rewritten to their bare column name first: Spark renders them as
   * fully-qualified backquoted identifiers, which makes the formula unreadable. The fully-qualified
   * provenance is already carried by [[ColumnLineage.sources]].
   */
  def render(expression: Expression): String = {
    val unwrapped = expression match {
      case alias: Alias => alias.child
      case other => other
    }
    val readable = Try(unwrapped.transformUp { case attr: Attribute => PlainColumnName(attr.name) })
      .getOrElse(unwrapped)
    Try(readable.sql).getOrElse(readable.toString)
  }

  /** Source plans referenced by subquery expressions in the given expression. */
  def subqueryPlans(expression: Expression): Seq[LogicalPlan] =
    expression.collect { case sub: SubqueryExpression => sub.plan }
}

private[lineage] object ColumnOriginResolver {

  /**
   * Maps every `CTERelationDef` id in the plan to the subtree it defines.
   *
   * Spark 3.4+ stops inlining `WITH` clauses during analysis and instead leaves a `WithCTE` node
   * holding the definitions plus `CTERelationRef` placeholders where they are used, so the two have
   * to be reconnected for lineage to reach through a CTE.
   */
  def cteDefinitions(plan: LogicalPlan): Map[Long, LogicalPlan] = {
    val defs = plan.collect {
      case p if p.getClass.getSimpleName == "CTERelationDef" => p
    } ++ plan.subqueries.flatMap(sub => sub.collect {
      case p if p.getClass.getSimpleName == "CTERelationDef" => p
    })
    defs.flatMap { d =>
      for {
        id <- PlanAccessors.invoke[java.lang.Long](d, "id").map(_.longValue())
        child <- PlanAccessors.plan(d, "child")
      } yield id -> child
    }.toMap
  }

  /** The definition a `CTERelationRef` points at, if it is known. */
  def cteReference(plan: LogicalPlan, cteDefs: Map[Long, LogicalPlan]): Option[(Long, LogicalPlan)] =
    if (plan.getClass.getSimpleName != "CTERelationRef") {
      None
    } else {
      PlanAccessors
        .invoke[java.lang.Long](plan, "cteId")
        .map(_.longValue())
        .flatMap(id => cteDefs.get(id).map(id -> _))
    }

  /**
   * The `(condition, output expressions)` pairs of a `MergeRows` node.
   *
   * Spark 3.4+ rewrites `MERGE INTO` during analysis into a `ReplaceData`/`WriteDelta` over a
   * `MergeRows` node, which replaces the original per-branch assignment lists with one output
   * expression row per branch, positionally aligned with the node's output attributes.
   */
  def mergeBranches(plan: LogicalPlan): Seq[(Option[Expression], Seq[Expression])] = {
    val instructions = PlanAccessors.seq[AnyRef](plan, "matchedInstructions") ++
      PlanAccessors.seq[AnyRef](plan, "notMatchedInstructions") ++
      PlanAccessors.seq[AnyRef](plan, "notMatchedBySourceInstructions")
    instructions.flatMap { instruction =>
      val condition = PlanAccessors.expression(instruction, "condition")
      PlanAccessors
        .seq[scala.collection.Seq[Expression]](instruction, "outputs")
        .map(row => condition -> row.toList)
    }
  }
}

/** Renders as a bare column name; used only to pretty-print expressions, never evaluated. */
private[lineage] case class PlainColumnName(name: String) extends LeafExpression with Unevaluable {
  override def dataType: DataType = StringType
  override def nullable: Boolean = true
  override def sql: String = name
  override def toString: String = name
}
