package com.linkedin.openhouse.javaclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

/** Decoder and sanitize path for {@link ReadBridge}. */
class ReadBridgeTest {

  private static final String PREFIX = ReadBridge.COLUMN_DEFAULT_PREFIX;

  @Test
  void decodesColumnDefaultsByFieldId() {
    // Avoid naming JsonNode: it is relocated in the shaded client, and this module has no `var`.
    Map<String, String> config = new HashMap<>();
    config.put(PREFIX + "5", "\"US\"");
    config.put(PREFIX + "7", "0");
    assertEquals(2, ReadBridge.from(config).columnDefaults().size());
    assertEquals("US", ReadBridge.from(config).columnDefaults().get(5).asText());
    assertEquals(0, ReadBridge.from(config).columnDefaults().get(7).asInt());
  }

  @Test
  void inertWhenConfigNullOrNoReadBridgeKeys() {
    assertSame(ReadBridge.INERT, ReadBridge.from(null));
    assertSame(ReadBridge.INERT, ReadBridge.from(Collections.singletonMap("other.key", "x")));
    assertTrue(ReadBridge.INERT.columnDefaults().isEmpty());
  }

  @Test
  void failsLoudOnKnownEntryWithBadFieldId() {
    // Non-integer suffix on a key we own is a bug, not a missing default.
    Map<String, String> config = new HashMap<>();
    config.put(PREFIX + "5", "\"US\"");
    config.put(PREFIX + "notAnInt", "\"x\"");
    assertThrows(IllegalStateException.class, () -> ReadBridge.from(config));
  }

  @Test
  void failsLoudOnKnownEntryWithUnparseableValue() {
    Map<String, String> config = new HashMap<>();
    config.put(PREFIX + "7", "{bad json");
    assertThrows(IllegalStateException.class, () -> ReadBridge.from(config));
  }

  @Test
  void ignoresUnknownKeysWithoutFailing() {
    // Keys outside the prefix are ignored so a newer server stays readable.
    Map<String, String> config = new HashMap<>();
    config.put(PREFIX + "5", "\"US\"");
    config.put("openhouse.read-bridge.some-future-feature.3", "{not a default}");
    assertEquals(1, ReadBridge.from(config).columnDefaults().size());
    assertEquals("US", ReadBridge.from(config).columnDefaults().get(5).asText());
  }

  @Test
  void sanitizeReturnsSameInstanceWhenRawIsNullOrIdentical() {
    TableMetadata raw = newTable("file:/tmp/rb-sanitize-same", twoColumns(null, null));
    assertSame(raw, ReadBridge.sanitize(null, raw));
    assertSame(raw, ReadBridge.sanitize(raw, raw));
    assertSame(null, ReadBridge.sanitize(raw, null));
  }

  @Test
  void sanitizeRestoresDefaultsOnFieldIdsThatExistedOnDisk() {
    TableMetadata raw = newTable("file:/tmp/rb-sanitize-restore", twoColumns(null, null));
    TableMetadata commit = newTable("file:/tmp/rb-sanitize-restore-c", twoColumns(null, "US"));

    TableMetadata sanitized = ReadBridge.sanitize(raw, commit);

    assertNull(sanitized.schema().findField(1).initialDefault());
    assertNull(sanitized.schema().findField(2).initialDefault());
    assertEquals(raw.schema().asStruct(), sanitized.schema().asStruct());
  }

  @Test
  void sanitizeKeepsWriterDefaultsOnNewFieldIds() {
    TableMetadata raw = newTable("file:/tmp/rb-sanitize-add", twoColumns(null, null));
    TableMetadata commit =
        newTable(
            "file:/tmp/rb-sanitize-add-c",
            new Schema(
                optionalInt(1, "id"),
                withInitialDefault(optionalString(2, "country"), "US"),
                withInitialDefault(optionalString(3, "email"), "none")));

    TableMetadata sanitized = ReadBridge.sanitize(raw, commit);

    assertNull(sanitized.schema().findField(2).initialDefault());
    assertEquals("none", sanitized.schema().findField(3).initialDefault());
    assertEquals("email", sanitized.schema().findField(3).name());
  }

  @Test
  void sanitizePreservesRenameAndTypeWidenOnExistingIds() {
    TableMetadata raw = newTable("file:/tmp/rb-sanitize-evolve", twoColumns(null, null));
    TableMetadata commit =
        newTable(
            "file:/tmp/rb-sanitize-evolve-c",
            new Schema(
                NestedField.from(optionalInt(1, "id")).ofType(Types.LongType.get()).build(),
                withInitialDefault(optionalString(2, "nation"), "US")));

    TableMetadata sanitized = ReadBridge.sanitize(raw, commit);

    NestedField id = sanitized.schema().findField(1);
    assertEquals(Types.LongType.get(), id.type());
    assertNull(id.initialDefault());
    NestedField nation = sanitized.schema().findField(2);
    assertEquals("nation", nation.name());
    assertNull(nation.initialDefault());
  }

  @Test
  void sanitizeRestoresWriteDefaultOnExistingIds() {
    TableMetadata raw =
        newTable(
            "file:/tmp/rb-sanitize-write",
            new Schema(
                optionalInt(1, "id"),
                NestedField.from(optionalString(2, "country"))
                    .withWriteDefault(Expressions.lit("CA"))
                    .build()));

    TableMetadata commit =
        newTable(
            "file:/tmp/rb-sanitize-write-c",
            new Schema(
                optionalInt(1, "id"),
                NestedField.from(optionalString(2, "country"))
                    .withInitialDefault(Expressions.lit("US"))
                    .withWriteDefault(Expressions.lit("MX"))
                    .build()));

    TableMetadata sanitized = ReadBridge.sanitize(raw, commit);

    NestedField country = sanitized.schema().findField(2);
    assertNull(country.initialDefault());
    assertEquals("CA", country.writeDefault());
  }

  @Test
  void sanitizeRestoresNestedExistingIdsAndKeepsNewNestedIds() {
    TableMetadata raw =
        newTable(
            "file:/tmp/rb-sanitize-nested",
            new Schema(
                optionalInt(1, "id"),
                NestedField.optional(
                    2, "address", Types.StructType.of(optionalString(3, "country")))));

    TableMetadata commit =
        newTable(
            "file:/tmp/rb-sanitize-nested-c",
            new Schema(
                optionalInt(1, "id"),
                NestedField.optional(
                    2,
                    "address",
                    Types.StructType.of(
                        withInitialDefault(optionalString(3, "country"), "US"),
                        withInitialDefault(optionalString(4, "region"), "west")))));

    TableMetadata sanitized = ReadBridge.sanitize(raw, commit);

    Types.StructType address = sanitized.schema().findField(2).type().asStructType();
    assertNull(address.field(3).initialDefault());
    assertEquals("west", address.field(4).initialDefault());
  }

  @Test
  void sanitizeRestoresEverySchemaId() {
    TableMetadata raw = newTable("file:/tmp/rb-sanitize-multi", twoColumns(null, null));
    TableMetadata commit =
        TableMetadata.buildFrom(
                newTable(
                    "file:/tmp/rb-sanitize-multi-c",
                    new Schema(
                        optionalInt(1, "id"),
                        withInitialDefault(optionalString(2, "country"), "US"))))
            .addSchema(
                new Schema(
                    1,
                    optionalInt(1, "id"),
                    withInitialDefault(optionalString(2, "country"), "US"),
                    withInitialDefault(optionalString(3, "region"), "west")),
                3)
            .setCurrentSchema(1)
            .build();

    TableMetadata sanitized = ReadBridge.sanitize(raw, commit);

    assertEquals(2, sanitized.schemas().size());
    for (Schema schema : sanitized.schemas()) {
      assertNull(schema.findField(2).initialDefault());
    }
    assertNull(sanitized.schemasById().get(0).findField(3));
    assertEquals("west", sanitized.schemasById().get(1).findField(3).initialDefault());
  }

  private static TableMetadata newTable(String location, Schema schema) {
    TableMetadata created =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), location, Collections.emptyMap());
    // newTableMetadata reassigns ids and drops defaults; put this schema back with defaults.
    return ReadBridge.replaceSchemas(
        created,
        Collections.singletonMap(
            created.currentSchemaId(),
            new Schema(
                created.currentSchemaId(),
                schema.columns(),
                schema.getAliases(),
                schema.identifierFieldIds())));
  }

  private static Schema twoColumns(String idDefault, String countryDefault) {
    NestedField id = optionalInt(1, "id");
    NestedField country = optionalString(2, "country");
    if (idDefault != null) {
      id = withInitialDefault(id, idDefault);
    }
    if (countryDefault != null) {
      country = withInitialDefault(country, countryDefault);
    }
    return new Schema(id, country);
  }

  private static NestedField optionalInt(int id, String name) {
    return NestedField.optional(id, name, Types.IntegerType.get());
  }

  private static NestedField optionalString(int id, String name) {
    return NestedField.optional(id, name, Types.StringType.get());
  }

  private static NestedField withInitialDefault(NestedField field, String value) {
    return NestedField.from(field).withInitialDefault(Expressions.lit(value)).build();
  }
}
