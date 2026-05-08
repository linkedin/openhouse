package com.linkedin.openhouse.tables.repository.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class SchemaValidationUtilTest {

  // ===== hasDuplicateCaseInsensitiveColumnNames =====

  @Test
  void hasDuplicateCaseInsensitiveColumnNames_returnsFalse_whenAllNamesUnique() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    assertFalse(SchemaValidationUtil.hasDuplicateCaseInsensitiveColumnNames(schema));
  }

  @Test
  void hasDuplicateCaseInsensitiveColumnNames_returnsTrue_whenTwoColumnsDifferOnlyInCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "ID", Types.StringType.get()));
    assertTrue(SchemaValidationUtil.hasDuplicateCaseInsensitiveColumnNames(schema));
  }

  @Test
  void hasDuplicateCaseInsensitiveColumnNames_returnsTrue_forMixedCaseDuplicate() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "datePartition", Types.StringType.get()),
            Types.NestedField.optional(2, "datepartition", Types.StringType.get()),
            Types.NestedField.optional(3, "value", Types.LongType.get()));
    assertTrue(SchemaValidationUtil.hasDuplicateCaseInsensitiveColumnNames(schema));
  }

  @Test
  void hasDuplicateCaseInsensitiveColumnNames_returnsFalse_whenSingleColumn() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.StringType.get()));
    assertFalse(SchemaValidationUtil.hasDuplicateCaseInsensitiveColumnNames(schema));
  }

  @Test
  void hasDuplicateCaseInsensitiveColumnNames_returnsTrue_forCaseDuplicateInsideNestedStruct() {
    // event: struct<ID: string, id: string> — siblings inside the nested struct are duplicates
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "event",
                Types.StructType.of(
                    Types.NestedField.required(2, "ID", Types.StringType.get()),
                    Types.NestedField.optional(3, "id", Types.StringType.get()))));
    assertTrue(SchemaValidationUtil.hasDuplicateCaseInsensitiveColumnNames(schema));
  }

  @Test
  void hasDuplicateCaseInsensitiveColumnNames_returnsFalse_forSameNameInDifferentStructs() {
    // user.id and session.id are in different structs — not siblings, not duplicates
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "user",
                Types.StructType.of(Types.NestedField.required(2, "id", Types.StringType.get()))),
            Types.NestedField.optional(
                3,
                "session",
                Types.StructType.of(Types.NestedField.required(4, "id", Types.StringType.get()))));
    assertFalse(SchemaValidationUtil.hasDuplicateCaseInsensitiveColumnNames(schema));
  }

  @Test
  void hasDuplicateCaseInsensitiveColumnNames_returnsTrue_forCaseDuplicateInsideListElement() {
    // events: list<struct<ID: string, id: string>> — duplicate inside the list element struct
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "events",
                Types.ListType.ofOptional(
                    2,
                    Types.StructType.of(
                        Types.NestedField.required(3, "ID", Types.StringType.get()),
                        Types.NestedField.optional(4, "id", Types.StringType.get())))));
    assertTrue(SchemaValidationUtil.hasDuplicateCaseInsensitiveColumnNames(schema));
  }

  @Test
  void hasDuplicateCaseInsensitiveColumnNames_returnsTrue_forCaseDuplicateInsideMapValue() {
    // metadata: map<string, struct<ID: string, id: string>> — duplicate in map value struct
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "metadata",
                Types.MapType.ofOptional(
                    2,
                    3,
                    Types.StringType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(4, "ID", Types.StringType.get()),
                        Types.NestedField.optional(5, "id", Types.StringType.get())))));
    assertTrue(SchemaValidationUtil.hasDuplicateCaseInsensitiveColumnNames(schema));
  }

  // ===== findDuplicateCaseInsensitiveColumnNames =====

  @Test
  void findDuplicateCaseInsensitiveColumnNames_returnsEmpty_whenNoConflicts() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "userid", Types.StringType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    assertTrue(SchemaValidationUtil.findDuplicateCaseInsensitiveColumnNames(schema).isEmpty());
  }

  @Test
  void findDuplicateCaseInsensitiveColumnNames_returnsConflict_forTopLevelDuplicate() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "userid", Types.StringType.get()),
            Types.NestedField.required(2, "userId", Types.StringType.get()));
    List<String> conflicts = SchemaValidationUtil.findDuplicateCaseInsensitiveColumnNames(schema);
    assertEquals(1, conflicts.size());
    assertTrue(conflicts.get(0).contains("userid") && conflicts.get(0).contains("userId"));
  }

  @Test
  void findDuplicateCaseInsensitiveColumnNames_returnsConflictWithPath_forNestedDuplicate() {
    // col1: struct<userid: string, userId: string>
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1,
                "col1",
                Types.StructType.of(
                    Types.NestedField.required(2, "userid", Types.StringType.get()),
                    Types.NestedField.required(3, "userId", Types.StringType.get()))));
    List<String> conflicts = SchemaValidationUtil.findDuplicateCaseInsensitiveColumnNames(schema);
    assertEquals(1, conflicts.size());
    assertTrue(
        conflicts.get(0).contains("col1.userid") && conflicts.get(0).contains("col1.userId"));
  }

  @Test
  void findDuplicateCaseInsensitiveColumnNames_returnsConflictWithPath_forListElementDuplicate() {
    // events: list<struct<ID: string, id: string>>
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "events",
                Types.ListType.ofOptional(
                    2,
                    Types.StructType.of(
                        Types.NestedField.required(3, "ID", Types.StringType.get()),
                        Types.NestedField.optional(4, "id", Types.StringType.get())))));
    List<String> conflicts = SchemaValidationUtil.findDuplicateCaseInsensitiveColumnNames(schema);
    assertEquals(1, conflicts.size());
    assertTrue(conflicts.get(0).contains("events.ID") && conflicts.get(0).contains("events.id"));
  }
}
