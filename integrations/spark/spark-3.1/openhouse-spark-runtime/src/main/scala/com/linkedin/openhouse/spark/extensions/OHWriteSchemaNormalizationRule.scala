package com.linkedin.openhouse.spark.extensions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{
  Alias, Attribute, Cast, CreateNamedStruct, Expression, GetStructField, If, IsNull, Literal, NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{DataType, StructField, StructType}

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
   * projection in <em>target column order</em> that renames and, if necessary, casts each source
   * column to the stored OH casing and type. This also handles the case where the source DataFrame
   * has columns in a different order than the stored schema (e.g. when the source is built from a
   * bean whose fields are introspected alphabetically).
   *
   * <p>Type coercion is included here because {@code ACCEPT_ANY_SCHEMA} bypasses {@code
   * ResolveOutputRelation} entirely — including its {@code Cast} insertion for type-widening cases
   * like {@code INT → LONG}. Without the cast, the raw source type would be passed to Iceberg and
   * fail at write time.
   *
   * <p>Tables with case-duplicate columns are skipped (the target is ambiguous).
   */
  private def projectByName(
      sourceCols: Seq[Attribute],
      targetCols: Seq[Attribute]): Option[Seq[NamedExpression]] = {

    // Case-duplicate target: skip normalization to avoid silently misdirecting the write.
    val targetGrouped = targetCols.groupBy(_.name.toLowerCase)
    if (targetGrouped.values.exists(_.size > 1)) return None

    // Case-duplicate source: skip to avoid ambiguous lookup.
    val srcGrouped = sourceCols.groupBy(_.name.toLowerCase)
    if (srcGrouped.values.exists(_.size > 1)) return None
    val srcByLower: Map[String, Attribute] =
      srcGrouped.map { case (lower, attrs) => lower -> attrs.head }

    val tz = Option(spark.conf.get("spark.sql.session.timeZone"))

    // Produce expressions in TARGET column order (replicating ResolveOutputRelation).
    // For each target column find the matching source column by case-insensitive name, then
    // align nested types case-insensitively or cast as appropriate.
    val exprs: Seq[NamedExpression] = targetCols.map { tgt =>
      srcByLower.get(tgt.name.toLowerCase) match {
        case Some(src) if src.name == tgt.name && src.dataType == tgt.dataType =>
          src // name and type (deep) already match — keep as-is
        case Some(src) =>
          val valueExpr = alignExpressionToTargetType(src, src.dataType, tgt.dataType, tz)
          Alias(valueExpr, tgt.name)(explicitMetadata = Some(tgt.metadata))
        case None => return None // unmatched column
      }
    }

    // No-op if the result is identical to the source (same column order, same names, same types).
    val unchanged = exprs.zip(sourceCols).forall {
      case (expr: Attribute, src) => expr.exprId == src.exprId
      case _ => false
    }
    if (unchanged) None else Some(exprs)
  }

  /**
   * By-position mode (e.g. {@code INSERT INTO … VALUES …}): zip source and target by position.
   * For each pair, replicate what {@code ResolveOutputRelation} would have done:
   * <ul>
   *   <li>If names and types already match, keep the source attribute as-is.</li>
   *   <li>Otherwise, wrap the source in {@code Alias(alignedValue, targetName)} where the aligned
   *       value uses name-based recursive matching for nested struct types and {@code Cast} for
   *       non-struct type mismatches.</li>
   * </ul>
   */
  private def projectByPosition(
      sourceCols: Seq[Attribute],
      targetCols: Seq[Attribute]): Option[Seq[NamedExpression]] = {

    val pairsNeedingChange = sourceCols.zip(targetCols).filter {
      case (src, tgt) =>
        src.name != tgt.name ||
          src.dataType != tgt.dataType ||
          src.metadata != tgt.metadata
    }
    if (pairsNeedingChange.isEmpty) return None

    val tz = Option(spark.conf.get("spark.sql.session.timeZone"))

    val exprs = sourceCols.zip(targetCols).map {
      case (src, tgt)
          if src.name == tgt.name && src.dataType == tgt.dataType && src.metadata == tgt.metadata =>
        src
      case (src, tgt) =>
        val valueExpr = alignExpressionToTargetType(src, src.dataType, tgt.dataType, tz)
        Alias(valueExpr, tgt.name)(explicitMetadata = Some(tgt.metadata))
    }
    Some(exprs)
  }

  /**
   * Recursively build an expression that produces {@code tgtType} from {@code srcExpr}, matching
   * nested struct fields by <em>case-insensitive name</em> rather than by position.
   *
   * <p>Why this matters: Spark's struct {@code Cast} maps fields positionally. If the source struct
   * has fields in a different order than the target (e.g. source {@code <lastName, firstName>},
   * target {@code <firstname, lastname>}), a positional cast silently misroutes values. This helper
   * uses {@code CreateNamedStruct} to pull each target field from the source by lowercased name, so
   * values land in the correct target slot regardless of source field order or casing.
   *
   * <p>For array-of-struct and map-with-struct-value types, name-based reordering would require
   * lambda-based rewrites ({@code ArrayTransform}, {@code TransformValues}) and is left as a future
   * enhancement. Such types currently fall through to {@code Cast}, which is positional.
   */
  private def alignExpressionToTargetType(
      srcExpr: Expression,
      srcType: DataType,
      tgtType: DataType,
      tz: Option[String]): Expression = (srcType, tgtType) match {
    case (s, t) if s == t => srcExpr

    case (s: StructType, t: StructType) =>
      val srcGrouped = s.fields.zipWithIndex.groupBy { case (f, _) => f.name.toLowerCase }
      if (srcGrouped.values.exists(_.size > 1)) {
        // Ambiguous source struct field names — defer to Spark's Cast (it will likely fail, which
        // surfaces a clearer error than producing silently misrouted data).
        Cast(srcExpr, tgtType, tz)
      } else {
        val srcByLower: Map[String, (StructField, Int)] =
          srcGrouped.map { case (k, v) => k -> v.head }
        val fieldArgs: Seq[Expression] = t.fields.toSeq.flatMap { tf =>
          val nameLit: Expression = Literal(tf.name)
          val valueExpr: Expression = srcByLower.get(tf.name.toLowerCase) match {
            case Some((sf, idx)) =>
              val fieldRef = GetStructField(srcExpr, idx, Some(sf.name))
              alignExpressionToTargetType(fieldRef, sf.dataType, tf.dataType, tz)
            case None =>
              // Target field has no source match — emit null literal. If the target is non-null
              // Iceberg will surface the constraint violation; that's the correct error to show.
              Literal.create(null, tf.dataType)
          }
          Seq(nameLit, valueExpr)
        }
        val built: Expression = CreateNamedStruct(fieldArgs)
        // Preserve null-struct semantics: a null source must produce a null target, not a struct
        // of nulls.
        If(IsNull(srcExpr), Literal.create(null, tgtType), built)
      }

    case _ => Cast(srcExpr, tgtType, tz)
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
