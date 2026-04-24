package com.linkedin.openhouse.spark.extensions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Analyzer rule that normalizes [[UnresolvedAttribute]] names to use the exact casing stored in
 * OpenHouse table schemas, enabling case-insensitive column resolution for OH tables regardless
 * of the {@code spark.sql.caseSensitive} session setting.
 *
 * <p>Mechanism: When a query references a column by a name whose casing differs from the stored
 * name (e.g. query uses {@code id}, table stores {@code ID}), Spark's built-in
 * {@code ResolveReferences} rule fails to resolve the attribute when
 * {@code spark.sql.caseSensitive=true}. This rule runs in the same analyzer pass and renames
 * any [[UnresolvedAttribute]] whose last name-part case-insensitively matches a column in a
 * resolved OpenHouse relation, replacing it with the stored casing. Spark's
 * {@code ResolveReferences} then finds an exact match on the next fixed-point iteration.
 *
 * <p>Scope: Only applies to tables backed by a catalog whose {@code catalog-impl} is configured
 * with "openhouse" (checked via the Spark conf). Tables where two or more columns share the same
 * case-folded name (ambiguous target) are excluded from normalization, consistent with the
 * server-side write-path guard.
 *
 * <p>The rule does NOT modify {@code spark.sql.caseSensitive} and has no effect on non-OH tables
 * or intermediate DataFrame operations in the same query.
 */
class OHCaseInsensitiveResolveRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val mappings = collectOHColumnMappings(plan)
    if (mappings.isEmpty) return plan

    plan.transformExpressions {
      case attr: UnresolvedAttribute =>
        val colName = attr.nameParts.last
        mappings.get(colName.toLowerCase) match {
          case Some(storedName) if storedName != colName =>
            // Rename to the stored casing so ResolveReferences finds an exact match.
            UnresolvedAttribute(attr.nameParts.dropRight(1) :+ storedName)
          case _ =>
            attr
        }
    }
  }

  /**
   * Scans the plan for resolved OpenHouse relations ([[DataSourceV2Relation]] nodes whose catalog
   * is configured with an OpenHouse catalog-impl) and returns a map of
   * {@code lowercase_column_name -> stored_column_name}.
   *
   * Tables with case-duplicate columns (e.g. both "id" and "ID") are excluded: the target column
   * is ambiguous and normalization could silently misdirect a read.
   */
  private def collectOHColumnMappings(plan: LogicalPlan): Map[String, String] = {
    val builder = Map.newBuilder[String, String]

    plan.foreach {
      case rel: DataSourceV2Relation if isOHRelation(rel) =>
        val fieldNames = rel.output.map(_.name)
        // Skip tables where two columns share the same case-folded name.
        val grouped = fieldNames.groupBy(_.toLowerCase)
        if (grouped.values.forall(_.size == 1)) {
          fieldNames.foreach(name => builder += (name.toLowerCase -> name))
        }
      case _ =>
    }

    builder.result()
  }

  /**
   * Returns true if the relation is backed by an OpenHouse catalog, identified by checking
   * whether the catalog-impl configured for this catalog's name contains "openhouse".
   * This avoids hardcoding the catalog name and works with any registered OH catalog instance.
   */
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
