package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Open-source encoder for the {@code read-bridge} feature: it asks the pluggable {@link
 * ColumnDefaultsSource} for a table's column initial-defaults and stamps each as a namespaced entry
 * in the per-table {@code config} — {@code openhouse.read-bridge.column-default.<fieldId> =
 * <single-value-json>}. The client decoder ({@code ReadBridge} in {@code openhouse-java-runtime})
 * reads these entries and overlays the defaults at metadata-load time.
 *
 * <p>No envelope/POJO: the flat config map (Iceberg REST {@code LoadTableResponse.config}
 * convention) carries the structure directly. Behaviorless by default — the open-source {@link
 * ColumnDefaultsSource} bean supplies nothing (see {@code ApiConfig}), so no entries are stamped. A
 * deployment delivers the bridge by overriding only {@link ColumnDefaultsSource}.
 *
 * <p><b>Mirror:</b> {@link #COLUMN_DEFAULT_PREFIX} is the shared contract with the client decoder;
 * keep it in sync. Further V3 features ride the same {@code openhouse.read-bridge.*} namespace as
 * additional keys.
 */
public class ReadBridgeConfigResolver {

  /** Config key prefix for a per-column read-time default; suffixed with the Iceberg field-id. */
  public static final String COLUMN_DEFAULT_PREFIX = "openhouse.read-bridge.column-default.";

  private final ColumnDefaultsSource columnDefaultsSource;

  public ReadBridgeConfigResolver(ColumnDefaultsSource columnDefaultsSource) {
    this.columnDefaultsSource = columnDefaultsSource;
  }

  public Map<String, String> resolve(String databaseId, String tableId, TableDto tableDto) {
    Map<Integer, JsonNode> columnDefaults = columnDefaultsSource.defaults(tableDto);
    if (columnDefaults == null || columnDefaults.isEmpty()) {
      return Collections.emptyMap(); // nothing to bridge -> stamp nothing
    }
    Map<String, String> config = new HashMap<>();
    // JsonNode.toString() is the single-value JSON (e.g. "US" -> "\"US\"", 0 -> "0").
    columnDefaults.forEach(
        (fieldId, value) -> config.put(COLUMN_DEFAULT_PREFIX + fieldId, value.toString()));
    return config;
  }
}
