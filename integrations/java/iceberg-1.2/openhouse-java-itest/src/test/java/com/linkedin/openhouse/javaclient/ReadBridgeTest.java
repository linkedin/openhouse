package com.linkedin.openhouse.javaclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

/** Decoder and sanitize for {@link ReadBridge}. */
class ReadBridgeTest {

  private static final String PREFIX = ReadBridge.COLUMN_DEFAULT_PREFIX;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void decodesColumnDefaultsByFieldId() {
    // Avoid naming JsonNode: it is relocated in the shaded client, and this module has no `var`.
    Map<String, String> config = new HashMap<>();
    config.put(PREFIX + "5", "\"US\"");
    config.put(PREFIX + "7", "0");
    ReadBridge bridge = ReadBridge.from(config);
    assertEquals(2, bridge.columnDefaults().size());
    // Original JSON strings so apply can bind without a relocated JsonNode.
    assertEquals("\"US\"", bridge.columnDefaults().get(5));
    assertEquals("0", bridge.columnDefaults().get(7));
  }

  @Test
  void inertWhenConfigNullOrNoReadBridgeKeys() {
    assertSame(ReadBridge.INERT, ReadBridge.from(null));
    assertSame(ReadBridge.INERT, ReadBridge.from(Collections.singletonMap("other.key", "x")));
    assertTrue(ReadBridge.from(null).columnDefaults().isEmpty());
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
    ReadBridge bridge = ReadBridge.from(config);
    assertEquals(1, bridge.columnDefaults().size());
    assertEquals("\"US\"", bridge.columnDefaults().get(5));
  }

  @Test
  void sanitizeReturnsSameInstanceWhenNothingToStrip() {
    TableMetadata raw = tableWith("file:/tmp/rb-sanitize-same", twoColumns(null, null));
    assertSame(raw, ReadBridge.INERT.sanitize(raw));
    assertSame(raw, stamping(2).sanitize(raw));
    assertSame(null, stamping(2).sanitize(null));
  }

  @Test
  void sanitizeStripsStampedIds() {
    TableMetadata commit = tableWith("file:/tmp/rb-sanitize-strip", twoColumns(null, "US"));

    TableMetadata sanitized = stamping(2).sanitize(commit);

    assertNull(sanitized.schema().findField(1).initialDefault());
    assertNull(sanitized.schema().findField(2).initialDefault());
  }

  @Test
  void sanitizeKeepsWriterDefaultsOnUnstampedIds() {
    TableMetadata commit =
        tableWith(
            "file:/tmp/rb-sanitize-add-c",
            new Schema(
                optionalInt(1, "id"),
                withInitialDefault(optionalString(2, "country"), "US"),
                withInitialDefault(optionalString(3, "email"), "none")));

    TableMetadata sanitized = stamping(2).sanitize(commit);

    assertNull(sanitized.schema().findField(2).initialDefault());
    assertEquals("none", sanitized.schema().findField(3).initialDefault());
    assertEquals("email", sanitized.schema().findField(3).name());
  }

  @Test
  void sanitizeLeavesUnstampedExistingDefaults() {
    TableMetadata commit =
        tableWith(
            "file:/tmp/rb-sanitize-native",
            new Schema(
                NestedField.from(optionalInt(1, "id"))
                    .withInitialDefault(Expressions.lit(0))
                    .build(),
                withInitialDefault(optionalString(2, "country"), "US")));

    TableMetadata sanitized = stamping(2).sanitize(commit);

    assertEquals(0, sanitized.schema().findField(1).initialDefault());
    assertNull(sanitized.schema().findField(2).initialDefault());
  }

  @Test
  void sanitizePreservesRenameAndTypeWidenOnStampedIds() {
    TableMetadata commit =
        tableWith(
            "file:/tmp/rb-sanitize-evolve-c",
            new Schema(
                NestedField.from(optionalInt(1, "id")).ofType(Types.LongType.get()).build(),
                withInitialDefault(optionalString(2, "nation"), "US")));

    TableMetadata sanitized = stamping(2).sanitize(commit);

    NestedField id = sanitized.schema().findField(1);
    assertEquals(Types.LongType.get(), id.type());
    assertNull(id.initialDefault());
    NestedField nation = sanitized.schema().findField(2);
    assertEquals("nation", nation.name());
    assertNull(nation.initialDefault());
  }

  @Test
  void sanitizeDoesNotTouchWriteDefault() {
    TableMetadata commit =
        tableWith(
            "file:/tmp/rb-sanitize-write-c",
            new Schema(
                optionalInt(1, "id"),
                NestedField.from(optionalString(2, "country"))
                    .withInitialDefault(Expressions.lit("US"))
                    .withWriteDefault(Expressions.lit("MX"))
                    .build()));

    NestedField country = stamping(2).sanitize(commit).schema().findField(2);
    assertNull(country.initialDefault());
    assertEquals("MX", country.writeDefault());
  }

  @Test
  void sanitizeStripsNestedStampedIdsAndKeepsNewNestedIds() {
    TableMetadata commit =
        tableWith(
            "file:/tmp/rb-sanitize-nested-c",
            new Schema(
                optionalInt(1, "id"),
                NestedField.optional(
                    2,
                    "address",
                    Types.StructType.of(
                        withInitialDefault(optionalString(3, "country"), "US"),
                        withInitialDefault(optionalString(4, "region"), "west")))));

    Types.StructType address =
        stamping(3).sanitize(commit).schema().findField(2).type().asStructType();
    assertNull(address.field(3).initialDefault());
    assertEquals("west", address.field(4).initialDefault());
  }

  @Test
  void sanitizeStripsStampedIdsOnEverySchemaId() {
    TableMetadata commit =
        TableMetadata.buildFrom(
                tableWith(
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

    TableMetadata sanitized = stamping(2).sanitize(commit);

    assertEquals(2, sanitized.schemas().size());
    for (Schema schema : sanitized.schemas()) {
      assertNull(schema.findField(2).initialDefault());
    }
    assertNull(sanitized.schemasById().get(0).findField(3));
    assertEquals("west", sanitized.schemasById().get(1).findField(3).initialDefault());
  }

  private static ReadBridge stamping(int... fieldIds) {
    Map<String, String> config = new HashMap<>();
    for (int fieldId : fieldIds) {
      config.put(PREFIX + fieldId, "\"US\"");
    }
    return ReadBridge.from(config);
  }

  /**
   * {@link TableMetadata#newTableMetadata} reassigns ids and drops defaults. Put this schema back
   * so sanitize tests can see writer/overlay defaults.
   */
  private static TableMetadata tableWith(String location, Schema schema) {
    TableMetadata created =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), location, Collections.emptyMap());
    try {
      ObjectNode root = (ObjectNode) MAPPER.readTree(TableMetadataParser.toJson(created));
      Schema kept =
          new Schema(
              created.currentSchemaId(),
              schema.columns(),
              schema.getAliases(),
              schema.identifierFieldIds());
      ((ArrayNode) root.get("schemas")).set(0, MAPPER.readTree(SchemaParser.toJson(kept)));
      return TableMetadataParser.fromJson(
          created.metadataFileLocation(), MAPPER.writeValueAsString(root));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
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
