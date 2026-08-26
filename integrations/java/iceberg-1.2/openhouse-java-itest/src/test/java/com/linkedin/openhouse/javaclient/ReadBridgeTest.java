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
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Decoder and apply for {@link ReadBridge}. */
class ReadBridgeTest {

  private static final String PREFIX = ReadBridge.COLUMN_DEFAULT_PREFIX;

  @Test
  void decodesColumnDefaultsByFieldId() throws ReadBridgeException {
    Map<String, String> config = new HashMap<>();
    config.put(PREFIX + "5", "\"US\"");
    config.put(PREFIX + "7", "0");
    ReadBridge bridge = ReadBridge.from(config);
    assertEquals(2, bridge.columnDefaults().size());
    assertEquals("US", bridge.columnDefaults().get(5).asText());
    assertEquals(0, bridge.columnDefaults().get(7).asInt());
  }

  @Test
  void inertWhenConfigNullOrNoReadBridgeKeys() throws ReadBridgeException {
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
    assertThrows(ReadBridgeException.class, () -> ReadBridge.from(config));
  }

  @Test
  void failsLoudOnKnownEntryWithUnparseableValue() {
    Map<String, String> config = new HashMap<>();
    config.put(PREFIX + "7", "{bad json");
    assertThrows(ReadBridgeException.class, () -> ReadBridge.from(config));
  }

  @Test
  void ignoresUnknownKeysWithoutFailing() throws ReadBridgeException {
    // Keys outside the prefix are ignored so a newer server stays readable.
    Map<String, String> config = new HashMap<>();
    config.put(PREFIX + "5", "\"US\"");
    config.put("openhouse.read-bridge.some-future-feature.3", "{not a default}");
    ReadBridge bridge = ReadBridge.from(config);
    assertEquals(1, bridge.columnDefaults().size());
    assertEquals("US", bridge.columnDefaults().get(5).asText());
  }

  @Test
  void applyReturnsSameInstanceWhenNothingToBridge() throws ReadBridgeException {
    TableMetadata raw = newTable("file:/tmp/rb-inert");
    assertSame(raw, ReadBridge.INERT.apply(raw));
    assertSame(raw, ReadBridge.from(Collections.singletonMap("other.key", "x")).apply(raw));
  }

  @Test
  void applySetsInitialDefaultOnMatchingField() throws ReadBridgeException {
    TableMetadata raw = newTable("file:/tmp/rb-apply");
    Map<String, String> config = Collections.singletonMap(PREFIX + "2", "\"US\"");

    TableMetadata bridged = ReadBridge.from(config).apply(raw);

    assertEquals("US", bridged.schema().findField(2).initialDefault());
    assertNull(bridged.schema().findField(1).initialDefault());
    assertEquals(raw.uuid(), bridged.uuid());
    assertEquals(raw.currentSchemaId(), bridged.currentSchemaId());
    assertEquals(raw.metadataFileLocation(), bridged.metadataFileLocation());
  }

  @Test
  void applyOverlaysEverySchemaId() throws ReadBridgeException {
    Schema v0 =
        new Schema(
            0,
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "country", Types.StringType.get()));
    Schema v1 =
        new Schema(
            1,
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "country", Types.StringType.get()),
            Types.NestedField.optional(3, "region", Types.StringType.get()));
    TableMetadata raw =
        TableMetadata.buildFrom(
                TableMetadata.newTableMetadata(
                    v0,
                    PartitionSpec.unpartitioned(),
                    "file:/tmp/rb-multischema",
                    Collections.emptyMap()))
            .addSchema(v1, 3)
            .setCurrentSchema(1)
            .build();

    Map<String, String> config = new HashMap<>();
    config.put(PREFIX + "2", "\"US\"");
    config.put(PREFIX + "3", "\"west\"");

    TableMetadata bridged = ReadBridge.from(config).apply(raw);

    assertEquals(2, bridged.schemas().size());
    for (Schema schema : bridged.schemas()) {
      assertEquals("US", schema.findField(2).initialDefault());
    }
    assertNull(bridged.schemasById().get(0).findField(3));
    assertEquals("west", bridged.schemasById().get(1).findField(3).initialDefault());
  }

  @Test
  void applyIgnoresFieldIdsAbsentFromAllSchemas() throws ReadBridgeException {
    TableMetadata raw = newTable("file:/tmp/rb-gap");
    Map<String, String> config = Collections.singletonMap(PREFIX + "99", "\"x\"");
    assertSame(raw, ReadBridge.from(config).apply(raw));
  }

  @Test
  void applyFailsLoudWhenDefaultCannotBindToColumnType() {
    TableMetadata raw = newTable("file:/tmp/rb-bad-bind");
    Map<String, String> config = Collections.singletonMap(PREFIX + "1", "\"not-an-int\"");
    assertThrows(ReadBridgeException.class, () -> ReadBridge.from(config).apply(raw));
  }

  @Test
  void applySetsDefaultOnNestedStructField() throws ReadBridgeException {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(
                2,
                "address",
                Types.StructType.of(
                    Types.NestedField.optional(3, "country", Types.StringType.get()))));
    TableMetadata raw =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), "file:/tmp/rb-nested", Collections.emptyMap());
    Map<String, String> config = Collections.singletonMap(PREFIX + "3", "\"US\"");

    TableMetadata bridged = ReadBridge.from(config).apply(raw);

    assertEquals(
        "US", bridged.schema().findField(2).type().asStructType().field(3).initialDefault());
  }

  private static TableMetadata newTable(String location) {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "country", Types.StringType.get()));
    return TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), location, Collections.emptyMap());
  }
}
