package com.linkedin.openhouse.javaclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the client-side read-bridge config decoder ({@link ReadBridge#columnDefaults}),
 * exercised in isolation. Mirrors the server-side encoder {@code ReadBridgeConfigResolver}.
 */
class ReadBridgeTest {

  private static final String PREFIX = ReadBridge.COLUMN_DEFAULT_PREFIX;

  @Test
  void decodesColumnDefaultsByFieldId() {
    // Inline calls avoid naming Jackson's JsonNode, which is relocated in the shaded client uber
    // (and this module compiles at a source level without `var`).
    Map<String, String> config = new HashMap<>();
    config.put(PREFIX + "5", "\"US\"");
    config.put(PREFIX + "7", "0");
    assertEquals(2, ReadBridge.columnDefaults(config).size());
    assertEquals("US", ReadBridge.columnDefaults(config).get(5).asText());
    assertEquals(0, ReadBridge.columnDefaults(config).get(7).asInt());
  }

  @Test
  void emptyWhenConfigNullOrNoReadBridgeKeys() {
    assertTrue(ReadBridge.columnDefaults(null).isEmpty());
    assertTrue(ReadBridge.columnDefaults(Collections.singletonMap("other.key", "x")).isEmpty());
  }

  @Test
  void failsLoudOnKnownEntryWithBadFieldId() {
    // A non-integer field-id on a key we own can't come from the server encoder (it stamps int
    // field-ids and JsonNode values), so it's a bug/corruption and throws rather than degrading.
    Map<String, String> config = new HashMap<>();
    config.put(PREFIX + "5", "\"US\"");
    config.put(PREFIX + "notAnInt", "\"x\"");
    assertThrows(IllegalStateException.class, () -> ReadBridge.columnDefaults(config));
  }

  @Test
  void failsLoudOnKnownEntryWithUnparseableValue() {
    Map<String, String> config = new HashMap<>();
    config.put(PREFIX + "7", "{bad json");
    assertThrows(IllegalStateException.class, () -> ReadBridge.columnDefaults(config));
  }

  @Test
  void ignoresUnknownKeysWithoutFailing() {
    // Forward compatibility: a key outside the column-default prefix (e.g. a newer server feature)
    // is ignored, never enforced — even if its value would not parse as a default.
    Map<String, String> config = new HashMap<>();
    config.put(PREFIX + "5", "\"US\"");
    config.put("openhouse.read-bridge.some-future-feature.3", "{not a default}");
    assertEquals(1, ReadBridge.columnDefaults(config).size());
    assertEquals("US", ReadBridge.columnDefaults(config).get(5).asText());
  }
}
