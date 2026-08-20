package com.linkedin.openhouse.javaclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Decoder for {@link ReadBridge#from}. */
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
}
