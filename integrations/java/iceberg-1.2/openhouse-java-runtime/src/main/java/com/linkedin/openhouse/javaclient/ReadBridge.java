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
 * <h3>Why decoding and applying are separate</h3>
 *
 * <p>{@link #from(Map)} decodes the config; {@link #apply(TableMetadata)} overlays the result onto
 * loaded metadata. They are split because they fail for completely different reasons and must not
 * share a retry policy.
 *
 * <p>Applying happens inside {@code BaseMetastoreTableOperations.refreshFromMetadataLocation},
 * which wraps the metadata loader in {@code Tasks.retry(20)} with exponential backoff. That retry
 * exists for the metadata <em>file read</em>, which fails transiently (network blips, a file not
 * yet visible) and succeeds on a later attempt.
 *
 * <p>Decoding is in-memory work on a string the server already delivered on the table-load
 * response. If it fails it fails identically on every attempt, so retrying can only burn time and
 * re-read the metadata file — ~87s of backoff and 21 storage reads to reach the error already
 * available on the first one. So decoding happens once, eagerly, in {@code doRefresh}, off the
 * retry path; only the overlay runs per attempt.
 *
 * <p>A read-bridge entry is produced by the server encoder from typed {@code JsonNode}s keyed by
 * integer field-id, so its value always round-trips through {@code readTree} and its suffix always
 * parses as an int. A decode failure on a <em>known</em> entry is therefore a bug or transport
 * corruption, not an expected runtime state, and this fails loud rather than silently degrading to
 * NULL. An <em>unknown</em> key (a newer server feature this client doesn't recognize) is ignored,
 * preserving forward compatibility. With nothing to bridge, metadata is returned unchanged.
 *
 * <p>Note this guarantees only that a stamped value is <em>well-formed</em>, not that it is the
 * <em>correct</em> default for its column — that semantic (default-to-schema) consistency is a
 * write-time concern owned by whatever server path sources the defaults.
 */
final class ReadBridge {

  /** Mirror of {@code ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX}. */
  static final String COLUMN_DEFAULT_PREFIX = "openhouse.read-bridge.column-default.";

  /** Nothing to bridge; {@link #apply(TableMetadata)} returns metadata untouched. */
  static final ReadBridge INERT = new ReadBridge(Collections.emptyMap());

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Map<Integer, JsonNode> columnDefaults;

  private ReadBridge(Map<Integer, JsonNode> columnDefaults) {
    this.columnDefaults = columnDefaults;
  }

  /**
   * Decodes the read-bridge behavior the server stamped into {@code config}, returning {@link
   * #INERT} when there is nothing to bridge.
   *
   * <p>Call this once per refresh and NOT from the metadata loader — see the class javadoc: a
   * failure here is deterministic, and the loader runs inside Iceberg's retry loop.
   *
   * @throws IllegalStateException if an entry this client owns is malformed (encoder bug or
   *     transport corruption); unknown keys are ignored.
   */
  static ReadBridge from(Map<String, String> config) {
    Map<Integer, JsonNode> columnDefaults = columnDefaults(config);
    return columnDefaults.isEmpty() ? INERT : new ReadBridge(columnDefaults);
  }

  /**
   * Applies the bridged read-time behavior onto {@code raw}, returning the transformed metadata (or
   * {@code raw} when there is nothing to bridge).
   *
   * <p>Pure in-memory transformation over already-decoded state. Runs per retry attempt, so it must
   * not do work whose failure a retry cannot fix.
   */
  TableMetadata apply(TableMetadata raw) {
    if (columnDefaults.isEmpty()) {
      return raw;
    }
    // TODO(read-bridge): overlay columnDefaults onto raw.schemas() via withSchemaOverlay; future V3
    // features bridged from config are applied here too. Two failure categories apply there, as
    // here: a capability gap we don't yet support degrades to NULL, while an invariant violation
    // (e.g. a default that can't bind to its column) fails loud.
    return raw;
  }

  /** The decoded {@code field-id -> initial-default} entries. Package-visible for testing. */
  Map<Integer, JsonNode> columnDefaults() {
    return columnDefaults;
  }

  /**
   * Decodes {@code field-id -> initial-default} from the {@code
   * openhouse.read-bridge.column-default.*} config entries; empty when there are none. On a known
   * entry, the server encoder guarantees an integer field-id and a value that round-trips through
   * {@code readTree}, so a non-integer field-id or an unparseable value is an encoder bug or
   * transport corruption — it throws rather than degrading. Unknown keys are ignored above.
   */
  private static Map<Integer, JsonNode> columnDefaults(Map<String, String> config) {
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
