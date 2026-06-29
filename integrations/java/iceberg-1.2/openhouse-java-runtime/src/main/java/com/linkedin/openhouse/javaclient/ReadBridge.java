package com.linkedin.openhouse.javaclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.TableMetadata;

/**
 * Read-time bridge: overlays Iceberg V3 read semantics onto loaded metadata for tables/clients that
 * don't yet carry them natively, using behavior the server delivers in the per-table {@code
 * config}. Today it applies per-column initial-defaults; further V3 features can be backported
 * through the same entry point as they are added.
 *
 * <p>Client end of the read-bridge wire contract — mirror of the server encoder {@code
 * ReadBridgeConfigResolver} (services/tables). The contract is flat, namespaced config keys (no
 * envelope/POJO): {@code openhouse.read-bridge.column-default.<fieldId> = <single-value-json>}.
 *
 * <p>Kept out of {@link OpenHouseTableOperations} so the read path stays slim. Pure and
 * fail-closed: an entry with a non-integer field-id or an unparseable value is skipped, and nothing
 * to apply returns {@code raw} unchanged.
 */
@Slf4j
final class ReadBridge {

  /** Mirror of {@code ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX}. */
  static final String COLUMN_DEFAULT_PREFIX = "openhouse.read-bridge.column-default.";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ReadBridge() {}

  /**
   * Applies the bridged read-time behavior the server stamped into {@code config} onto {@code raw},
   * returning the transformed metadata (or {@code raw} when there is nothing to bridge).
   */
  static TableMetadata apply(TableMetadata raw, Map<String, String> config) {
    Map<Integer, JsonNode> columnDefaults = columnDefaults(config);
    if (columnDefaults.isEmpty()) {
      return raw;
    }
    // TODO(read-bridge): overlay columnDefaults onto raw.schemas() via withSchemaOverlay; future V3
    // features bridged from config are applied here too.
    return raw;
  }

  /**
   * Decodes {@code field-id -> initial-default} from the {@code
   * openhouse.read-bridge.column-default.*} config entries; empty when there are none. Skips any
   * entry with a non-integer field-id or an unparseable value (fail-closed). Package-visible for
   * unit testing in isolation.
   */
  static Map<Integer, JsonNode> columnDefaults(Map<String, String> config) {
    if (config == null) {
      return Collections.emptyMap();
    }
    Map<Integer, JsonNode> byFieldId = new HashMap<>();
    for (Map.Entry<String, String> entry : config.entrySet()) {
      if (!entry.getKey().startsWith(COLUMN_DEFAULT_PREFIX)) {
        continue;
      }
      try {
        int fieldId = Integer.parseInt(entry.getKey().substring(COLUMN_DEFAULT_PREFIX.length()));
        byFieldId.put(fieldId, MAPPER.readTree(entry.getValue()));
      } catch (RuntimeException | JsonProcessingException e) {
        log.warn(
            "Skipping unparseable read-bridge config entry {}={}",
            entry.getKey(),
            entry.getValue(),
            e);
      }
    }
    return byFieldId;
  }
}
