package com.linkedin.openhouse.spark.acl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, ExprId, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Masks the columns of an OpenHouse table that the current principal is not entitled to read.
 *
 * Every restricted column is replaced by a typed `NULL` directly above the relation that produces
 * it, which is the only position that closes the whole surface at once: filters, joins, aggregates
 * and `CREATE TABLE AS SELECT` all consume the masked values, so a restricted column cannot leak
 * through `WHERE ssn = '...'` or by being copied into another table. Masking the final projection
 * instead would leave every one of those paths open.
 *
 * The masks deliberately carry fresh expression ids, and every reference above the relation is
 * repointed at them. Reusing the original ids would make the masking projection indistinguishable
 * from its child, and the optimizer discards such projections as redundant.
 *
 * Because masking happens at the relation, a restricted column referenced explicitly and one merely
 * covered by `SELECT *` are indistinguishable here; both yield `NULL` rather than an error.
 */
case class ColumnAclRule(spark: SparkSession) extends Rule[LogicalPlan] {

  private lazy val resolver: ColumnEntitlementsResolver =
    new ColumnEntitlementsResolver(
      spark.conf
        .get(ColumnAclRule.CacheTtlSecondsConf, ColumnAclRule.CacheTtlSecondsDefault)
        .toLong * 1000L)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!enabled) plan else rewrite(plan)._1
  }

  private def enabled: Boolean =
    spark.conf.get(ColumnAclRule.EnabledConf, ColumnAclRule.EnabledDefault).toBoolean

  /**
   * Rewrites `plan`, returning it alongside the masks introduced underneath it, keyed by the
   * expression id each one replaces so that ancestors can be repointed at them.
   */
  private def rewrite(plan: LogicalPlan): (LogicalPlan, Map[ExprId, Attribute]) = plan match {
    // The write target denotes the destination schema rather than data being read, and it has to
    // remain a NamedRelation, so only the query feeding the write is rewritten. Row level commands
    // that genuinely read the target do so through relations inside that query.
    case write: V2WriteCommand =>
      val (query, masked) = rewrite(write.query)
      (repoint(write.withNewQuery(query), masked), masked)

    case relation: DataSourceV2Relation =>
      mask(relation)

    case other =>
      val rewritten = other.children.map(rewrite)
      val masked = rewritten.flatMap(_._2).toMap
      (repoint(other.withNewChildren(rewritten.map(_._1)), masked), masked)
  }

  private def mask(relation: DataSourceV2Relation): (LogicalPlan, Map[ExprId, Attribute]) = {
    val unmasked = (relation, Map.empty[ExprId, Attribute])
    if (relation.getTagValue(ColumnAclRule.MaskedTag).isDefined) {
      return unmasked
    }
    val identifier = relation.identifier.orNull
    val catalog = relation.catalog.orNull
    if (identifier == null || catalog == null || relation.table == null) {
      return unmasked
    }

    val restricted = resolver.restrictedColumns(catalog, identifier, relation.table)
    if (restricted.isEmpty) {
      return unmasked
    }

    val projectList: Seq[NamedExpression] = relation.output.map { attr =>
      if (restricted.exists(_.equalsIgnoreCase(attr.name))) {
        Alias(Literal(null, attr.dataType), attr.name)(qualifier = attr.qualifier)
      } else {
        attr
      }
    }
    val masked = projectList
      .zip(relation.output)
      .collect { case (alias: Alias, attr) => attr.exprId -> alias.toAttribute }
      .toMap

    relation.setTagValue(ColumnAclRule.MaskedTag, true)
    (Project(projectList, relation), masked)
  }

  /** Points this node's own references at the masks that replaced them further down the plan. */
  private def repoint(plan: LogicalPlan, masked: Map[ExprId, Attribute]): LogicalPlan = {
    if (masked.isEmpty) {
      plan
    } else {
      plan.transformExpressions {
        case attr: AttributeReference if masked.contains(attr.exprId) => masked(attr.exprId)
      }
    }
  }
}

object ColumnAclRule {

  /** Set to false to stop masking restricted columns, e.g. while debugging a policy. */
  val EnabledConf = "spark.openhouse.columnAcl.enabled"

  val EnabledDefault = "true"

  /** How long a resolved entitlement is reused before the catalog is asked again. */
  val CacheTtlSecondsConf = "spark.openhouse.columnAcl.cacheTtlSeconds"

  val CacheTtlSecondsDefault = "30"

  /** Marks relations already wrapped in a mask so repeated analysis does not stack projections. */
  private val MaskedTag = TreeNodeTag[Boolean]("openhouse.columnAcl.masked")
}
