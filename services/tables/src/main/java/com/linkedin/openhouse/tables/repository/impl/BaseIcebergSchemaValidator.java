package com.linkedin.openhouse.tables.repository.impl;

import com.linkedin.openhouse.common.exception.InvalidSchemaEvolutionException;
import com.linkedin.openhouse.tables.repository.SchemaValidator;
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
 */
@Component
public class BaseIcebergSchemaValidator implements SchemaValidator {
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
