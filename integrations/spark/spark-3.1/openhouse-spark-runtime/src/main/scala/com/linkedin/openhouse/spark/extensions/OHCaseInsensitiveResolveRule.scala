package com.linkedin.openhouse.spark.extensions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Analyzer rule that normalizes [[UnresolvedAttribute]] names to use the exact casing stored in
 * OpenHouse table schemas, enabling case-insensitive column resolution for OH tables regardless
 * of the {@code spark.sql.caseSensitive} session setting.
 *
 * <p>Mechanism: When a query references a column by a name whose casing differs from the stored
 * name (e.g. query uses {@code id}, table stores {@code ID}), Spark's built-in
 * {@code ResolveReferences} rule fails to resolve the attribute when
 * {@code spark.sql.caseSensitive=true}. This rule runs in the same analyzer pass and renames
 * any [[UnresolvedAttribute]] whose name parts case-insensitively match a column (or nested struct
 * field) in a resolved OpenHouse relation, replacing each part with the stored casing. Spark's
 * {@code ResolveReferences} then finds an exact match on the next fixed-point iteration.
 *
 * <p>Nested struct fields: for a dotted attribute reference such as {@code PAYLOAD.event_id}
 * where the stored schema is {@code PAYLOAD STRUCT<EVENT_ID: string>}, the rule normalizes the
 * full name-part chain — {@code ["PAYLOAD", "event_id"]} becomes {@code ["PAYLOAD", "EVENT_ID"]}.
 * Normalization descends recursively through arbitrarily nested struct types. Case-duplicate
 * struct fields at any level are skipped (ambiguous target).
 *
 * <p><b>Batch-ordering constraint</b>: this rule is injected via {@code injectResolutionRule},
 * which places it <em>after</em> Spark's built-in {@code ResolveReferences} in the Resolution
 * batch. {@code ResolveReferences} throws an {@code AnalysisException} immediately when it finds
 * the top-level attribute (case-sensitively) but cannot resolve the nested field — so
 * normalization of nested fields only succeeds when the <em>top-level</em> column name also has a
 * case mismatch (e.g. query uses {@code payload}, stored as {@code PAYLOAD}). In that situation
 * {@code ResolveReferences} leaves the whole dotted reference unresolved (no exception), our rule
 * normalises the full path, and the next fixed-point iteration resolves it. If the top-level name
 * is an exact match but a nested field is not (e.g. {@code payload.event_id} with stored schema
 * {@code payload STRUCT<EVENT_ID: string>}), {@code ResolveReferences} throws before this rule
 * can act. The typical production case — Hive-migrated tables where all identifiers are
 * upper-cased — is fully covered.
 *
 * <p>Scope: Only applies to tables backed by a catalog whose {@code catalog-impl} is configured
 * with "openhouse" (checked via the Spark conf). Two exclusions keep non-OH catalogs (Hive, other
 * v2 catalogs) safe: (1) tables where two or more columns share the same case-folded name are
 * skipped (ambiguous target), consistent with the server-side write-path guard; (2) column names
 * that also appear in any non-OH resolved relation in the same plan are excluded — because
 * {@code transformExpressions} walks the whole plan tree and cannot tell which
 * {@link UnresolvedAttribute} belongs to which catalog, renaming a shared name could break
 * resolution for non-OH references under {@code caseSensitive=true}.
 *
 * <p>The rule does NOT modify {@code spark.sql.caseSensitive} and has no effect on non-OH tables
 * or intermediate DataFrame operations in the same query.
 */
class OHCaseInsensitiveResolveRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val (topLevelMap, typeMap) = collectOHColumnMappings(plan)
    if (topLevelMap.isEmpty) return plan

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
            val newParts = normalizeNameParts(attr.nameParts, topLevelMap, typeMap)
            if (newParts != attr.nameParts) UnresolvedAttribute(newParts)
            else attr
        }
    }
  }

  /**
   * Normalizes a sequence of name parts from an [[UnresolvedAttribute]] to use the exact casing
   * stored in OH table schemas, handling both top-level columns and nested struct field access.
   *
   * <p>Three patterns are handled:
   * <ol>
   *   <li>Single-part: {@code ["id"]} → {@code ["ID"]} (top-level column, no qualifier)</li>
   *   <li>Qualifier + column: {@code ["t", "id"]} → {@code ["t", "ID"]} (table alias or catalog
   *       prefix before the column name)</li>
   *   <li>Nested struct access: {@code ["payload", "event_id"]} →
   *       {@code ["payload", "EVENT_ID"]} where {@code payload} is an OH struct column whose
   *       stored field name is {@code EVENT_ID}; or {@code ["t", "payload", "event_id"]} →
   *       {@code ["t", "payload", "EVENT_ID"]} with a qualifier prefix. Normalization recurses
   *       into arbitrarily nested struct types.</li>
   * </ol>
   *
   * <p>The algorithm scans {@code parts} left-to-right looking for the first index {@code i}
   * where {@code parts(i)} case-insensitively matches a top-level OH column. The prefix
   * {@code parts(0..i-1)} (qualifiers) is preserved unchanged, {@code parts(i)} is replaced with
   * the stored column name, and the remaining parts are recursively normalized against that
   * column's struct schema (if any). If no top-level OH column is found, the parts are returned
   * unchanged.
   */
  private def normalizeNameParts(
      parts: Seq[String],
      topLevelMap: Map[String, String],
      typeMap: Map[String, DataType]): Seq[String] = {
    if (parts.isEmpty) return parts

    for (i <- parts.indices) {
      topLevelMap.get(parts(i).toLowerCase) match {
        case Some(storedName) =>
          val remainingNormalized = typeMap.get(storedName) match {
            case Some(dt) => normalizeStructPath(parts.drop(i + 1), dt)
            case None     => parts.drop(i + 1)
          }
          return parts.take(i) ++ Seq(storedName) ++ remainingNormalized
        case None =>
      }
    }
    parts
  }

  /**
   * Recursively normalizes a path of field name parts through a nested [[StructType]].
   *
   * <p>For each part, looks up the stored field name case-insensitively within the current struct
   * and recurses into that field's type for subsequent parts. Returns the input unchanged if:
   * <ul>
   *   <li>The current type is not a {@link StructType} (no struct to traverse)</li>
   *   <li>No field matches the current part (part is not a field name at this level)</li>
   *   <li>Two or more fields at this level share the same case-folded name (ambiguous)</li>
   * </ul>
   */
  private def normalizeStructPath(parts: Seq[String], dataType: DataType): Seq[String] = {
    if (parts.isEmpty) return parts
    dataType match {
      case st: StructType =>
        val grouped = st.fields.groupBy(_.name.toLowerCase)
        // Skip normalization at this level if any two fields share the same case-folded name.
        if (grouped.values.exists(_.size > 1)) return parts
        val fieldByLower = grouped.collect { case (lower, arr) if arr.size == 1 => lower -> arr.head }
        fieldByLower.get(parts.head.toLowerCase) match {
          case Some(field) =>
            Seq(field.name) ++ normalizeStructPath(parts.tail, field.dataType)
          case None =>
            parts
        }
      case _ =>
        parts
    }
  }

  /**
   * Scans the plan for resolved OpenHouse relations ([[DataSourceV2Relation]] nodes whose catalog
   * is configured with an OpenHouse catalog-impl) and returns:
   * <ul>
   *   <li>A top-level map of {@code lowercase_column_name -> stored_column_name}, with names
   *       that appear in any non-OH resolved relation excluded (cross-catalog safety).</li>
   *   <li>A type map of {@code stored_column_name -> DataType} used to traverse nested struct
   *       schemas during normalization of dotted attribute paths.</li>
   * </ul>
   *
   * <p>Two exclusions apply:
   * <ol>
   *   <li>Tables with case-duplicate columns (e.g. both "id" and "ID") are skipped — the target
   *       column is ambiguous and normalization could silently misdirect a read.</li>
   *   <li>Column names that also appear (case-insensitively) in any non-OH resolved relation in
   *       the same plan are excluded from the top-level map. [[plan.transformExpressions]] is
   *       applied to the whole plan tree and cannot distinguish which [[UnresolvedAttribute]]
   *       belongs to which catalog. Renaming a name that also exists in a Hive/other-catalog
   *       relation would corrupt resolution for those references under
   *       {@code caseSensitive=true}.</li>
   * </ol>
   */
  private def collectOHColumnMappings(
      plan: LogicalPlan): (Map[String, String], Map[String, DataType]) = {
    val ohBuilder   = Map.newBuilder[String, String]
    val typeBuilder = Map.newBuilder[String, DataType]
    val nonOHLower  = collection.mutable.Set[String]()

    plan.foreach {
      case rel: DataSourceV2Relation if isOHRelation(rel) =>
        val fieldNames = rel.output.map(_.name)
        // Skip tables where two columns share the same case-folded name.
        val grouped = fieldNames.groupBy(_.toLowerCase)
        if (grouped.values.forall(_.size == 1)) {
          rel.output.foreach { attr =>
            ohBuilder   += (attr.name.toLowerCase -> attr.name)
            typeBuilder += (attr.name             -> attr.dataType)
          }
        }
      case node: LeafNode if node.resolved =>
        // Track all column names from every other resolved relation (Hive, other v2 catalogs,
        // file scans, etc.) so we can exclude ambiguous names below.
        node.output.foreach(attr => nonOHLower += attr.name.toLowerCase)
      case _ =>
    }

    val rawMap      = ohBuilder.result()
    val typeMap     = typeBuilder.result()
    // Only keep names that are unambiguously OH-specific.
    val topLevelMap = rawMap.filterKeys(k => !nonOHLower.contains(k))
    (topLevelMap, typeMap)
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
