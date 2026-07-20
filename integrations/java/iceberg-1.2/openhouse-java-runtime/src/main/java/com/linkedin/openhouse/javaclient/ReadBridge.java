package com.linkedin.openhouse.javaclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
 * <p>Kept out of {@link OpenHouseTableOperations} so the read path stays slim. A read-bridge entry
 * is produced by the server encoder ({@code ReadBridgeConfigResolver}) from typed {@code JsonNode}s
 * keyed by integer field-id, so its value always round-trips through {@code readTree} and its
 * suffix always parses as an int. A decode failure on a *known* entry is therefore a bug or
 * transport corruption, not an expected runtime state, and this fails loud rather than silently
 * degrading to NULL. An *unknown* key (a newer server feature this client doesn't recognize) is
 * ignored, preserving forward compatibility. With nothing to apply, {@code raw} is returned
 * unchanged.
 *
 * <p>Note this guarantees only that a stamped value is <em>well-formed</em>, not that it is the
 * <em>correct</em> default for its column — that semantic (default-to-schema) consistency is a
 * write-time concern owned by whatever server path sources the defaults.
 */
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
    // features bridged from config are applied here too. Two failure categories apply there, as
    // here: a capability gap we don't yet support degrades to NULL, while an invariant violation
    // (e.g. a default that can't bind to its column) fails loud.
    return raw;
  }

  /**
   * Decodes {@code field-id -> initial-default} from the {@code
   * openhouse.read-bridge.column-default.*} config entries; empty when there are none. On a known
   * entry, the server encoder guarantees an integer field-id and a value that round-trips through
   * {@code readTree}, so a non-integer field-id or an unparseable value is an encoder bug or
   * transport corruption — it throws rather than degrading. Unknown keys are ignored above.
   * Package-visible for unit testing in isolation.
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
        // The server encoder stamps an int field-id and a JsonNode value that round-trips through
        // readTree, so reaching here means an encoder bug or transport corruption, not an expected
        // state. Fail loud so it is caught, rather than silently reading NULL.
        throw new IllegalStateException(
            "read-bridge: unusable "
                + COLUMN_DEFAULT_PREFIX
                + " entry "
                + entry.getKey()
                + "="
                + entry.getValue(),
            e);
      }
    }
    return byFieldId;
  }
}
