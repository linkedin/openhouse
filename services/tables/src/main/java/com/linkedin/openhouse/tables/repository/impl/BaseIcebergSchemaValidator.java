package com.linkedin.openhouse.tables.repository.impl;

import com.linkedin.openhouse.common.exception.InvalidSchemaEvolutionException;
import com.linkedin.openhouse.tables.repository.SchemaValidator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.MapDifference;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.springframework.stereotype.Component;

/**
 * A base implementation which delegates the validation to iceberg library that is configured to: -
 * not allow to write optional values to a required field. - allow input schema to have different
 * ordering than table.
 *
 * <p>Also provides static helpers used by {@link OpenHouseInternalRepositoryImpl} to normalize a
 * write schema's top-level column names to the casing already present in the table schema, enabling
 * case-insensitive writes without mutating the table's existing column casing.
 */
@Component
public class BaseIcebergSchemaValidator implements SchemaValidator {

  /**
   * Returns true if {@code schema} has two or more top-level columns whose names differ only in
   * case (e.g. "id" and "ID"). Such tables are excluded from case-insensitive write normalization
   * because the target column would be ambiguous.
   */
  static boolean hasCaseDuplicateFields(Schema schema) {
    Map<String, Integer> seen = new HashMap<>();
    for (Types.NestedField field : schema.columns()) {
      String lower = field.name().toLowerCase();
      if (seen.containsKey(lower) && !seen.get(lower).equals(field.fieldId())) {
        return true;
      }
      seen.put(lower, field.fieldId());
    }
    return false;
  }

  /**
   * Renames top-level fields in {@code writeSchema} to use the casing from {@code tableSchema},
   * matched by Iceberg field ID (not by name). Fields in {@code writeSchema} whose ID does not
   * appear in {@code tableSchema} (genuinely new columns) are left unchanged.
   *
   * <p>This preserves the table's existing column casing when a writer submits columns with
   * different casing (e.g. writer sends "id", table has "ID" → normalized to "ID").
   */
  static Schema normalizeSchemaCasingToTable(Schema writeSchema, Schema tableSchema) {
    // Build fieldId → table column name map for O(1) lookup
    Map<Integer, String> tableNameById = new HashMap<>();
    for (Types.NestedField field : tableSchema.columns()) {
      tableNameById.put(field.fieldId(), field.name());
    }

    List<Types.NestedField> normalizedFields = new ArrayList<>();
    for (Types.NestedField field : writeSchema.columns()) {
      String tableName = tableNameById.get(field.fieldId());
      if (tableName != null && !tableName.equals(field.name())) {
        // Existing column with different casing: rename to table casing, keep all other attributes
        normalizedFields.add(
            field.isOptional()
                ? Types.NestedField.optional(field.fieldId(), tableName, field.type(), field.doc())
                : Types.NestedField.required(
                    field.fieldId(), tableName, field.type(), field.doc()));
      } else {
        normalizedFields.add(field);
      }
    }
    return new Schema(writeSchema.schemaId(), normalizedFields, writeSchema.identifierFieldIds());
  }

  @Override
  public void validateWriteSchema(Schema oldSchema, Schema newSchema, String tableUri)
      throws InvalidSchemaEvolutionException, IllegalArgumentException {
    validateFields(oldSchema, newSchema, tableUri);
    enforceDeleteColumnFailure(oldSchema, newSchema, tableUri);
    TypeUtil.validateSchema(
        "OpenHouse Server Schema validation Validation", newSchema, oldSchema, true, false);
  }

  private void enforceDeleteColumnFailure(Schema oldSchema, Schema newSchema, String tableUri) {
    // if old schema couldn't find a key in the new schema map, it should fail
    MapDifference<Integer, String> diff =
        Maps.difference(oldSchema.idToName(), newSchema.idToName());

    if (!diff.entriesOnlyOnLeft().isEmpty()) {
      throw new InvalidSchemaEvolutionException(
          tableUri, newSchema.toString(), oldSchema.toString(), "Some columns are dropped");
    }
    if (!diff.entriesDiffering().isEmpty()) {
      throw new InvalidSchemaEvolutionException(
          tableUri,
          newSchema.toString(),
          oldSchema.toString(),
          "Given same id, column name is different");
    }
  }

  /**
   * Since @param newSchema is considered to be the new source-of-truth table schema, OpenHouse just
   * need to ensure all field-Id are still matching before directly use it to assemble {@link
   * org.apache.iceberg.TableMetadata}
   *
   * <p>Note that Iceberg by itself is case-sensitive by default, the casing info can be lost
   * through the SQL engine layer. Here OH is honoring the default case sensitivity for column
   * name-based resolution.
   *
   * @param oldSchema existing schema in table catalog
   * @param newSchema user-provided schema that may contain evolution
   * @param tableUri tableUri
   */
  protected void validateFields(Schema oldSchema, Schema newSchema, String tableUri) {
    for (Types.NestedField field : oldSchema.columns()) {
      Types.NestedField columnInNewSchema = newSchema.findField(field.name());
      if (columnInNewSchema == null) {
        throw new InvalidSchemaEvolutionException(
            tableUri,
            newSchema.toString(),
            oldSchema.toString(),
            String.format("Column[%s] not found in newSchema", field.name()));
      }

      if (columnInNewSchema.fieldId() != field.fieldId()) {
        throw new InvalidSchemaEvolutionException(
            tableUri,
            newSchema.toString(),
            oldSchema.toString(),
            String.format(
                "Internal Error: Column name [%s] in newSchema has a different fieldId",
                field.name()));
      }
    }
  }
}
