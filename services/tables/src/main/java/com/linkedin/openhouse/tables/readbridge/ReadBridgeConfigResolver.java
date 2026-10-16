package com.linkedin.openhouse.tables.readbridge;

import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Builds the per-table {@code config} that read-bridge clients apply. Owns policy (capability id,
 * opt-in, keys); deployments supply data via {@link ColumnDefaultsSource}.
 *
 * <p>Column defaults apply only to a table that carries {@code
 * read-bridge.column-default.enabled=true}. Turning them on is therefore a table commit like any
 * other: a writer that loaded the table earlier conflicts on its next commit and reloads the table,
 * defaults included.
 */
public class ReadBridgeConfigResolver {

  /** Capability id; also names {@code <id>.enabled} and the config key prefix below. */
  public static final String COLUMN_DEFAULT_FEATURE_ID = "read-bridge.column-default";

  /**
   * Table property: {@code true} turns column defaults on for the table; any other value, or none,
   * leaves them off. {@link ColumnDefaultCommitPolicy} keeps a committed {@code true} from being
   * changed or removed unless the request carries the column-default policy bypass.
   */
  public static final String COLUMN_DEFAULT_ENABLED_PROP = COLUMN_DEFAULT_FEATURE_ID + ".enabled";

  /** Client contract: {@code openhouse.read-bridge.column-default.<fieldId>}. */
  public static final String COLUMN_DEFAULT_PREFIX = "openhouse." + COLUMN_DEFAULT_FEATURE_ID + ".";

  private final ColumnDefaultsSource columnDefaultsSource;

  public ReadBridgeConfigResolver(ColumnDefaultsSource columnDefaultsSource) {
    this.columnDefaultsSource =
        Objects.requireNonNull(columnDefaultsSource, "columnDefaultsSource");
  }

  /**
   * Merges the config of every read-bridge capability that applies to the table; empty when none
   * does. Config that cannot be produced fails instead of being left out: the client would silently
   * run without it.
   *
   * @throws TableConfigUnavailableException if the table's stored settings cannot be applied
   */
  public Map<String, String> resolve(TableDto tableDto) {
    return storedColumnDefaults(tableDto).entrySet().stream()
        .collect(toMap(e -> COLUMN_DEFAULT_PREFIX + e.getKey(), Map.Entry::getValue));
  }

  /**
   * The column defaults declared by a table a write sends, as Iceberg single-value JSON keyed by
   * field ID. Empty when the table is not bridged or the source declares none.
   *
   * @throws ColumnDefaultException if a default the table declares cannot be applied
   */
  public Map<Integer, String> incomingColumnDefaults(TableDto incoming)
      throws ColumnDefaultException {
    return columnDefaultsByFieldId(incoming);
  }

  /**
   * The column defaults declared by a stored table, as Iceberg single-value JSON keyed by field ID.
   * A stored default that cannot be applied leaves the table's config unavailable.
   *
   * @throws TableConfigUnavailableException if a stored default cannot be applied
   */
  public Map<Integer, String> storedColumnDefaults(TableDto storedTable) {
    try {
      return columnDefaultsByFieldId(storedTable);
    } catch (ColumnDefaultException e) {
      throw new TableConfigUnavailableException(e.getMessage(), e);
    }
  }

  /**
   * Whether read-bridge applies to a table with these properties: it carries {@code
   * read-bridge.column-default.enabled=true}, which may be set at creation or on an existing table.
   * A committed {@code true} is immutable unless the client sends the column-default policy bypass;
   * {@code false}, an absent property and any other value mean off.
   */
  public boolean isBridged(Map<String, String> tableProperties) {
    if (tableProperties == null) {
      return false;
    }
    String value = tableProperties.get(COLUMN_DEFAULT_ENABLED_PROP);
    return value != null && "true".equalsIgnoreCase(value.trim());
  }

  private Map<Integer, String> columnDefaultsByFieldId(TableDto tableDto)
      throws ColumnDefaultException {
    Objects.requireNonNull(tableDto, "tableDto");
    if (!isBridged(tableDto.getTableProperties())) {
      return Collections.emptyMap();
    }
    Map<Integer, JsonNode> columnDefaults = columnDefaultsSource.defaults(tableDto);
    if (columnDefaults == null || columnDefaults.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<Integer, String> byId = new HashMap<>();
    columnDefaults.forEach(
        (fieldId, value) -> {
          if (fieldId != null && value != null) {
            byId.put(fieldId, value.toString());
          }
        });
    return byId;
  }
}
