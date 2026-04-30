package com.linkedin.openhouse.spark.extensions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Post-hoc resolution rule that replicates the column-name and type normalization that Spark's
 * {@code ResolveOutputRelation} would have applied to OpenHouse write commands, compensating for
 * the fact that OH tables advertise {@link org.apache.spark.sql.connector.catalog.TableCapability#ACCEPT_ANY_SCHEMA}
 * (via {@link OHSparkCatalog}) which causes {@code ResolveOutputRelation} to skip them entirely.
 *
 * <p>Why {@code ACCEPT_ANY_SCHEMA}? Spark's {@code ResolveOutputRelation} throws at analysis time
 * when {@code caseSensitive=true} and a client DataFrame column (e.g. {@code "id"}) does not match
 * the OH table column name exactly ({@code "ID"}). Advertising {@code ACCEPT_ANY_SCHEMA} prevents
 * the throw. This rule then runs as a {@code Post-Hoc Resolution} rule and does the work that
 * {@code ResolveOutputRelation} would have done: wrapping the source query in a {@code Project}
 * that renames (and if necessary casts) each source column to the stored OH casing and type.
 *
 * <p>The rule handles both write modes:
 * <ul>
 *   <li><b>By-name writes</b> ({@code isByName=true}, e.g. {@code df.writeTo().append()}): each
 *       source column is matched to the target column whose name it equals case-insensitively.
 *       Tables with case-duplicate columns are skipped (ambiguous target).</li>
 *   <li><b>By-position writes</b> ({@code isByName=false}, e.g. {@code INSERT INTO … VALUES …}):
 *       source and target columns are zipped positionally and each source column is renamed (and
 *       if the types differ, cast) to match the target. This replicates the {@code Alias} +
 *       {@code Cast} that {@code ResolveOutputRelation} would have inserted.</li>
 * </ul>
 *
 * <p>In both modes, if source and target already match in name and type the rule returns the plan
 * unchanged. If the column count differs the rule is a no-op (the mismatch is left for Iceberg or
 * the OH server to report).
 */
class OHWriteSchemaNormalizationRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown {
      case write: V2WriteCommand
          if write.table.resolved && write.query.resolved && isOHWrite(write) =>
        normalizeColumnNames(write).getOrElse(write)
    }
  }

  private def isOHWrite(write: V2WriteCommand): Boolean = {
    write.table match {
      case rel: DataSourceV2Relation => isOHRelation(rel)
      case _ => false
    }
  }

  private def normalizeColumnNames(write: V2WriteCommand): Option[V2WriteCommand] = {
    val ohRelation = write.table match {
      case rel: DataSourceV2Relation => rel
      case _ => return None
    }

    val targetCols = ohRelation.output
    val sourceCols = write.query.output

    // If column counts differ, leave it to Iceberg / the OH server to report the mismatch.
    if (sourceCols.size != targetCols.size) return None

    val projections =
      if (write.isByName) projectByName(sourceCols, targetCols)
      else projectByPosition(sourceCols, targetCols)

    projections match {
      case None => None
      case Some(exprs) => Some(write.withNewQuery(Project(exprs, write.query)))
    }
  }

  /**
   * By-name mode: replicate what {@code ResolveOutputRelation} does for by-name writes — produce a
   * projection in <em>target column order</em> that renames (and if necessary casts) each source
   * column to the stored OH casing. This also handles the case where the source DataFrame has
   * columns in a different order than the stored schema (e.g. when the source is built from a bean
   * whose fields are introspected alphabetically).
   *
   * <p>Tables with case-duplicate columns are skipped (the target is ambiguous).
   */
  private def projectByName(
      sourceCols: Seq[org.apache.spark.sql.catalyst.expressions.Attribute],
      targetCols: Seq[org.apache.spark.sql.catalyst.expressions.Attribute])
      : Option[Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression]] = {

    // Case-duplicate target: skip normalization to avoid silently misdirecting the write.
    val targetGrouped = targetCols.groupBy(_.name.toLowerCase)
    if (targetGrouped.values.exists(_.size > 1)) return None

    // Case-duplicate source: skip to avoid ambiguous lookup.
    val srcGrouped = sourceCols.groupBy(_.name.toLowerCase)
    if (srcGrouped.values.exists(_.size > 1)) return None
    val srcByLower: Map[String, org.apache.spark.sql.catalyst.expressions.Attribute] =
      srcGrouped.map { case (lower, attrs) => lower -> attrs.head }

    // Produce expressions in TARGET column order (replicating ResolveOutputRelation).
    // For each target column find the matching source column by case-insensitive name.
    val exprs: Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression] =
      targetCols.map { tgt =>
        srcByLower.get(tgt.name.toLowerCase) match {
          case Some(src) if src.name == tgt.name => src               // correct casing, keep as-is
          case Some(src)                          => Alias(src, tgt.name)() // rename to stored casing
          case None                               => return None       // unmatched column
        }
      }

    // No-op if the result is identical to the source (same column order, same names).
    val unchanged = exprs.zip(sourceCols).forall {
      case (expr: org.apache.spark.sql.catalyst.expressions.Attribute, src) =>
        expr.exprId == src.exprId
      case _ => false
    }
    if (unchanged) None else Some(exprs)
  }

  /**
   * By-position mode (e.g. {@code INSERT INTO … VALUES …}): zip source and target by position.
   * For each pair, replicate what {@code ResolveOutputRelation} would have done:
   * <ul>
   *   <li>If names and types already match, keep the source attribute as-is.</li>
   *   <li>Otherwise, wrap the source in {@code Alias(Cast(src, targetType), targetName)} to
   *       rename the column and coerce the type to the stored schema.</li>
   * </ul>
   */
  private def projectByPosition(
      sourceCols: Seq[org.apache.spark.sql.catalyst.expressions.Attribute],
      targetCols: Seq[org.apache.spark.sql.catalyst.expressions.Attribute])
      : Option[Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression]] = {

    val pairsNeedingChange = sourceCols.zip(targetCols).filter {
      case (src, tgt) =>
        src.name != tgt.name ||
          src.dataType != tgt.dataType ||
          src.metadata != tgt.metadata
    }
    if (pairsNeedingChange.isEmpty) return None

    val exprs = sourceCols.zip(targetCols).map {
      case (src, tgt)
          if src.name == tgt.name && src.dataType == tgt.dataType && src.metadata == tgt.metadata =>
        src
      case (src, tgt) =>
        val castExpr = if (src.dataType == tgt.dataType) src
                       else Cast(src, tgt.dataType, Option(spark.conf.get("spark.sql.session.timeZone")))
        Alias(castExpr, tgt.name)(explicitMetadata = Some(tgt.metadata))
    }
    Some(exprs)
  }

  private def isOHRelation(rel: DataSourceV2Relation): Boolean = {
    rel.catalog match {
      case Some(c) =>
        val key = s"spark.sql.catalog.${c.name()}.catalog-impl"
        spark.conf.getOption(key).exists(_.toLowerCase.contains("openhouse"))
      case None =>
        false
    }
  }
}
