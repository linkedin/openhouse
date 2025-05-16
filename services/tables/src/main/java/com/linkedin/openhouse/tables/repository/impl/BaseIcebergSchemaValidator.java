package com.linkedin.openhouse.tables.repository.impl;

import com.linkedin.openhouse.common.exception.InvalidSchemaEvolutionException;
import com.linkedin.openhouse.tables.repository.SchemaValidator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.MapDifference;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
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
    // TODO: Detect and resolve rename and reorder
    // This implementation assumes both old and new schemas have field ids internally assigned by
    // iceberg rather than specified by the users
    enforceDeleteColumnFailure(oldSchema, newSchema, tableUri);
    TypeUtil.validateWriteSchema(
        oldSchema, newSchema, /*checkNullability*/ true, /*checkOrdering*/ false);
  }

  private void enforceDeleteColumnFailure(Schema oldSchema, Schema newSchema, String tableUri) {
    // if old schema couldn't find a key in the new schema map, it should fail
    MapDifference<Integer, String> diff =
        Maps.difference(oldSchema.idToName(), newSchema.idToName());

    if (diff.entriesOnlyOnLeft().size() != 0) {
      throw new InvalidSchemaEvolutionException(
          tableUri, newSchema.toString(), oldSchema.toString(), "Some columns are dropped");
    }
    if (diff.entriesDiffering().size() != 0) {
      throw new InvalidSchemaEvolutionException(
          tableUri,
          newSchema.toString(),
          oldSchema.toString(),
          "Given same id, column name is different");
    }
  }
}
