package com.linkedin.openhouse.spark.extensions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
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
 * with "openhouse" (checked via the Spark conf). Two exclusions keep non-OH catalogs (Hive, other
 * v2 catalogs) safe: (1) tables where two or more columns share the same case-folded name are
 * skipped (ambiguous target), consistent with the server-side write-path guard; (2) column names
 * that also appear in any non-OH resolved relation in the same plan are excluded — because
 * {@code resolveOperatorsDown} + {@code transformExpressions} walks the whole plan tree and cannot
 * tell which {@link UnresolvedAttribute} belongs to which catalog, renaming a shared name could
 * break resolution for non-OH references under {@code caseSensitive=true}.
 *
 * <p>The rule does NOT modify {@code spark.sql.caseSensitive} and has no effect on non-OH tables
 * or intermediate DataFrame operations in the same query.
 */
class OHCaseInsensitiveResolveRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val mappings = collectOHColumnMappings(plan)
    if (mappings.isEmpty) return plan

    // Use resolveOperatorsDown so the rename is applied to every *unresolved* plan node in the
    // tree — Sort, Project, Filter, etc. — not just the top-level node.
    // plan.transformExpressions alone only applies mapExpressions to the root plan node's own
    // expression fields (via mapProductIterator) and does not descend into child plan nodes, so
    // for queries like "SELECT id FROM v ORDER BY id" the Project's expressions were left
    // untouched.  resolveOperatorsDown visits every unanalyzed node (skipping already-resolved
    // view bodies) and applies transformExpressions to each one.
    plan.resolveOperatorsDown {
      case p: LogicalPlan =>
        p.transformExpressions {
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
  }

  /**
   * Scans the plan for resolved OpenHouse relations ([[DataSourceV2Relation]] nodes whose catalog
   * is configured with an OpenHouse catalog-impl) and returns a map of
   * {@code lowercase_column_name -> stored_column_name}.
   *
   * Two exclusions apply:
   * <ol>
   *   <li>Tables with case-duplicate columns (e.g. both "id" and "ID") are skipped — the target
   *       column is ambiguous and normalization could silently misdirect a read.</li>
   *   <li>Column names that also appear (case-insensitively) in any non-OH resolved relation in
   *       the same plan are excluded. [[resolveOperatorsDown]] + [[transformExpressions]] walks
   *       the whole plan tree and cannot distinguish which [[UnresolvedAttribute]] belongs to
   *       which catalog. Renaming a name that also exists in a Hive/other-catalog relation would
   *       corrupt resolution for those references under {@code caseSensitive=true}.</li>
   * </ol>
   */
  private def collectOHColumnMappings(plan: LogicalPlan): Map[String, String] = {
    val ohBuilder = Map.newBuilder[String, String]
    val nonOHLower = collection.mutable.Set[String]()

    plan.foreach {
      case rel: DataSourceV2Relation if isOHRelation(rel) =>
        val fieldNames = rel.output.map(_.name)
        // Skip tables where two columns share the same case-folded name.
        val grouped = fieldNames.groupBy(_.toLowerCase)
        if (grouped.values.forall(_.size == 1)) {
          fieldNames.foreach(name => ohBuilder += (name.toLowerCase -> name))
        }
      case node: LeafNode if node.resolved =>
        // Track all column names from every other resolved relation (Hive, other v2 catalogs,
        // file scans, etc.) so we can exclude ambiguous names below.
        node.output.foreach(attr => nonOHLower += attr.name.toLowerCase)
      case _ =>
    }

    // Only keep names that are unambiguously OH-specific. If the same case-folded name appears
    // in a non-OH relation, skip the rename — we cannot tell which UnresolvedAttribute belongs
    // to which catalog when transformExpressions walks the whole plan tree.
    ohBuilder.result().filterKeys(k => !nonOHLower.contains(k))
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
