package com.linkedin.openhouse.tables.repository.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.common.exception.InvalidSchemaEvolutionException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class BaseIcebergSchemaValidatorTest {

  private static final BaseIcebergSchemaValidator VALIDATOR = new BaseIcebergSchemaValidator();

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

  @Test
  void normalizeSchemaCasingToTable_renamesFieldsInsideNestedStruct() {
    // Table: event: struct<EventId: string, Value: long>
    // Writer: event: struct<eventid: string, value: long>  (wrong casing at nested level)
    Schema tableSchema =
        new Schema(
            Types.NestedField.optional(
                1,
                "event",
                Types.StructType.of(
                    Types.NestedField.required(2, "EventId", Types.StringType.get()),
                    Types.NestedField.optional(3, "Value", Types.LongType.get()))));
    Schema writeSchema =
        new Schema(
            Types.NestedField.optional(
                1,
                "event",
                Types.StructType.of(
                    Types.NestedField.required(2, "eventid", Types.StringType.get()),
                    Types.NestedField.optional(3, "value", Types.LongType.get()))));

    Schema normalized =
        BaseIcebergSchemaValidator.normalizeSchemaCasingToTable(writeSchema, tableSchema);

    Types.StructType nestedStruct = normalized.findField(1).type().asStructType();
    assertEquals("EventId", nestedStruct.field(2).name());
    assertEquals("Value", nestedStruct.field(3).name());
  }

  @Test
  void normalizeSchemaCasingToTable_renamesFieldsInsideListElement() {
    // Table: events: list<struct<EventId: string>>
    // Writer: events: list<struct<eventid: string>>
    Schema tableSchema =
        new Schema(
            Types.NestedField.optional(
                1,
                "events",
                Types.ListType.ofOptional(
                    2,
                    Types.StructType.of(
                        Types.NestedField.required(3, "EventId", Types.StringType.get())))));
    Schema writeSchema =
        new Schema(
            Types.NestedField.optional(
                1,
                "events",
                Types.ListType.ofOptional(
                    2,
                    Types.StructType.of(
                        Types.NestedField.required(3, "eventid", Types.StringType.get())))));

    Schema normalized =
        BaseIcebergSchemaValidator.normalizeSchemaCasingToTable(writeSchema, tableSchema);

    Types.ListType listType = (Types.ListType) normalized.findField(1).type();
    Types.StructType elementStruct = listType.elementType().asStructType();
    assertEquals("EventId", elementStruct.field(3).name());
  }

  @Test
  void normalizeSchemaCasingToTable_renamesFieldsInsideMapValue() {
    // Table: metadata: map<string, struct<Key: string, Val: long>>
    // Writer: metadata: map<string, struct<key: string, val: long>>
    Schema tableSchema =
        new Schema(
            Types.NestedField.optional(
                1,
                "metadata",
                Types.MapType.ofOptional(
                    2,
                    3,
                    Types.StringType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(4, "Key", Types.StringType.get()),
                        Types.NestedField.optional(5, "Val", Types.LongType.get())))));
    Schema writeSchema =
        new Schema(
            Types.NestedField.optional(
                1,
                "metadata",
                Types.MapType.ofOptional(
                    2,
                    3,
                    Types.StringType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(4, "key", Types.StringType.get()),
                        Types.NestedField.optional(5, "val", Types.LongType.get())))));

    Schema normalized =
        BaseIcebergSchemaValidator.normalizeSchemaCasingToTable(writeSchema, tableSchema);

    Types.MapType mapType = (Types.MapType) normalized.findField(1).type();
    Types.StructType valueStruct = mapType.valueType().asStructType();
    assertEquals("Key", valueStruct.field(4).name());
    assertEquals("Val", valueStruct.field(5).name());
  }

  // ===== Integration: validateWriteSchema passes after normalization =====

  @Test
  void validateWriteSchema_passes_afterCasingNormalization() {
    // Table has "ID" (uppercase); writer submits "id" (lowercase) — same field ID
    Schema tableSchema = new Schema(Types.NestedField.required(1, "ID", Types.StringType.get()));
    Schema writeSchema = new Schema(Types.NestedField.required(1, "id", Types.StringType.get()));

    assertFalse(
        SchemaValidationUtil.hasDuplicateCaseInsensitiveColumnNames(tableSchema),
        "table has no case duplicates, normalization should apply");

    Schema normalized =
        BaseIcebergSchemaValidator.normalizeSchemaCasingToTable(writeSchema, tableSchema);

    // After normalization, validation should pass without exception
    assertDoesNotThrow(
        () -> VALIDATOR.validateWriteSchema(tableSchema, normalized, "db.table"),
        "validateWriteSchema should succeed after casing normalization");
  }

  @Test
  void validateWriteSchema_passes_afterCasingNormalization_withColumnAddition() {
    // Table has "ID" (id=1); writer submits "id" (id=1) with an extra new column (id=2).
    // After normalization "id" → "ID", sameSchema is false so validateWriteSchema is called.
    // The evolution is valid (existing field ID unchanged, new column appended) — must succeed.
    Schema tableSchema = new Schema(Types.NestedField.required(1, "ID", Types.StringType.get()));
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "new_col", Types.LongType.get()));

    assertFalse(
        SchemaValidationUtil.hasDuplicateCaseInsensitiveColumnNames(tableSchema),
        "table has no case duplicates, normalization should apply");

    Schema normalized =
        BaseIcebergSchemaValidator.normalizeSchemaCasingToTable(writeSchema, tableSchema);

    assertEquals("ID", normalized.findField(1).name(), "existing column must be normalized");
    assertEquals("new_col", normalized.findField(2).name(), "new column must be preserved as-is");

    assertDoesNotThrow(
        () -> VALIDATOR.validateWriteSchema(tableSchema, normalized, "db.table"),
        "validateWriteSchema must accept valid column addition after casing normalization");
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
        SchemaValidationUtil.hasDuplicateCaseInsensitiveColumnNames(tableSchema),
        "table should be detected as having case-duplicate columns");

    // Since normalization is skipped, validateWriteSchema sees "Id" vs expected "id" → failure
    assertThrows(
        InvalidSchemaEvolutionException.class,
        () -> VALIDATOR.validateWriteSchema(tableSchema, writeSchema, "db.table"));
  }
}
