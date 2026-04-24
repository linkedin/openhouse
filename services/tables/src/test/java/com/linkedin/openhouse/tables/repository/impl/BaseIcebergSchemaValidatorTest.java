package com.linkedin.openhouse.tables.repository.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.common.exception.InvalidSchemaEvolutionException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class BaseIcebergSchemaValidatorTest {

  private static final BaseIcebergSchemaValidator VALIDATOR = new BaseIcebergSchemaValidator();

  // ===== hasCaseDuplicateFields =====

  @Test
  void hasCaseDuplicateFields_returnsFalse_whenAllNamesUnique() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    assertFalse(BaseIcebergSchemaValidator.hasCaseDuplicateFields(schema));
  }

  @Test
  void hasCaseDuplicateFields_returnsTrue_whenTwoColumnsDifferOnlyInCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "ID", Types.StringType.get()));
    assertTrue(BaseIcebergSchemaValidator.hasCaseDuplicateFields(schema));
  }

  @Test
  void hasCaseDuplicateFields_returnsTrue_forMixedCaseDuplicate() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "datePartition", Types.StringType.get()),
            Types.NestedField.optional(2, "datepartition", Types.StringType.get()),
            Types.NestedField.optional(3, "value", Types.LongType.get()));
    assertTrue(BaseIcebergSchemaValidator.hasCaseDuplicateFields(schema));
  }

  @Test
  void hasCaseDuplicateFields_returnsFalse_whenSingleColumn() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.StringType.get()));
    assertFalse(BaseIcebergSchemaValidator.hasCaseDuplicateFields(schema));
  }

  // ===== normalizeSchemaCasingToTable =====

  @Test
  void normalizeSchemaCasingToTable_noChange_whenCasingAlreadyMatches() {
    Schema tableSchema =
        new Schema(
            Types.NestedField.required(1, "ID", Types.StringType.get()),
            Types.NestedField.optional(2, "Name", Types.StringType.get()));
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(1, "ID", Types.StringType.get()),
            Types.NestedField.optional(2, "Name", Types.StringType.get()));

    Schema normalized =
        BaseIcebergSchemaValidator.normalizeSchemaCasingToTable(writeSchema, tableSchema);

    assertEquals("ID", normalized.findField(1).name());
    assertEquals("Name", normalized.findField(2).name());
  }

  @Test
  void normalizeSchemaCasingToTable_renamesColumn_toMatchTableCasing() {
    // Table has "ID" (uppercase), writer submits "id" (lowercase)
    Schema tableSchema = new Schema(Types.NestedField.required(1, "ID", Types.StringType.get()));
    Schema writeSchema = new Schema(Types.NestedField.required(1, "id", Types.StringType.get()));

    Schema normalized =
        BaseIcebergSchemaValidator.normalizeSchemaCasingToTable(writeSchema, tableSchema);

    assertEquals("ID", normalized.findField(1).name());
  }

  @Test
  void normalizeSchemaCasingToTable_preservesNewColumns_unchanged() {
    // Table has id=1, writer adds a new column id=2 with different-cased name
    Schema tableSchema = new Schema(Types.NestedField.required(1, "ID", Types.StringType.get()));
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "newCol", Types.LongType.get()));

    Schema normalized =
        BaseIcebergSchemaValidator.normalizeSchemaCasingToTable(writeSchema, tableSchema);

    // Existing column normalized to table casing
    assertEquals("ID", normalized.findField(1).name());
    // New column left unchanged
    assertEquals("newCol", normalized.findField(2).name());
  }

  @Test
  void normalizeSchemaCasingToTable_preservesOptionalRequired() {
    Schema tableSchema =
        new Schema(
            Types.NestedField.required(1, "ID", Types.StringType.get()),
            Types.NestedField.optional(2, "Name", Types.StringType.get()));
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    Schema normalized =
        BaseIcebergSchemaValidator.normalizeSchemaCasingToTable(writeSchema, tableSchema);

    assertFalse(normalized.findField(1).isOptional(), "id=1 should remain required");
    assertTrue(normalized.findField(2).isOptional(), "id=2 should remain optional");
  }

  @Test
  void normalizeSchemaCasingToTable_preservesFieldDoc() {
    Schema tableSchema =
        new Schema(Types.NestedField.optional(1, "ID", Types.StringType.get(), "the identifier"));
    Schema writeSchema =
        new Schema(Types.NestedField.optional(1, "id", Types.StringType.get(), "the identifier"));

    Schema normalized =
        BaseIcebergSchemaValidator.normalizeSchemaCasingToTable(writeSchema, tableSchema);

    assertEquals("the identifier", normalized.findField(1).doc());
  }

  // ===== Integration: validateWriteSchema passes after normalization =====

  @Test
  void validateWriteSchema_passes_afterCasingNormalization() {
    // Table has "ID" (uppercase); writer submits "id" (lowercase) — same field ID
    Schema tableSchema = new Schema(Types.NestedField.required(1, "ID", Types.StringType.get()));
    Schema writeSchema = new Schema(Types.NestedField.required(1, "id", Types.StringType.get()));

    assertFalse(
        BaseIcebergSchemaValidator.hasCaseDuplicateFields(tableSchema),
        "table has no case duplicates, normalization should apply");

    Schema normalized =
        BaseIcebergSchemaValidator.normalizeSchemaCasingToTable(writeSchema, tableSchema);

    // After normalization, validation should pass without exception
    assertDoesNotThrow(
        () -> VALIDATOR.validateWriteSchema(tableSchema, normalized, "db.table"),
        "validateWriteSchema should succeed after casing normalization");
  }

  @Test
  void validateWriteSchema_fails_forCaseDuplicateTable_withMismatchedCasing() {
    // Table has case-duplicate columns — normalization is skipped for such tables.
    // Validator should still reject a write with mismatched casing on these tables.
    Schema tableSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "ID", Types.StringType.get()));
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(1, "Id", Types.StringType.get()),
            Types.NestedField.optional(2, "ID", Types.StringType.get()));

    assertTrue(
        BaseIcebergSchemaValidator.hasCaseDuplicateFields(tableSchema),
        "table should be detected as having case-duplicate columns");

    // Since normalization is skipped, validateWriteSchema sees "Id" vs expected "id" → failure
    assertThrows(
        InvalidSchemaEvolutionException.class,
        () -> VALIDATOR.validateWriteSchema(tableSchema, writeSchema, "db.table"));
  }
}
