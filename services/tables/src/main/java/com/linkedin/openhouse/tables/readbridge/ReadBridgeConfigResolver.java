package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds the per-table {@code config} map the client reads. OpenHouse owns ramp and keys; {@link
 * ColumnDefaultsSource} supplies the values.
 */
@Slf4j
public class ReadBridgeConfigResolver {

  /** Also names {@code <id>.enabled} and the config key prefix. */
  public static final String COLUMN_DEFAULT_FEATURE_ID = "read-bridge.column-default";

  /** {@code openhouse.read-bridge.column-default.<fieldId>}. */
  public static final String COLUMN_DEFAULT_PREFIX = "openhouse." + COLUMN_DEFAULT_FEATURE_ID + ".";

  private final ColumnDefaultsSource columnDefaultsSource;

  private final TableFeatureToggle featureToggle;

  public ReadBridgeConfigResolver(
      ColumnDefaultsSource columnDefaultsSource, TableFeatureToggle featureToggle) {
    this.columnDefaultsSource = columnDefaultsSource;
    this.featureToggle = featureToggle;
  }

  /** Per-table config the client applies at load. Empty when nothing is bridged. */
  public Map<String, String> resolve(TableDto tableDto) {
    Map<String, String> config = new HashMap<>();
    config.putAll(columnDefaultConfig(tableDto));
    return config;
  }

  private Map<String, String> columnDefaultConfig(TableDto tableDto) {
    // No source registered: skip the HouseTables lookup.
    if (columnDefaultsSource == ColumnDefaultsSource.NONE) {
      return Collections.emptyMap();
    }
    if (!isColumnDefaultRamped(tableDto)) {
      return Collections.emptyMap();
    }
    Map<Integer, JsonNode> columnDefaults = columnDefaultsSource.defaults(tableDto);
    if (columnDefaults == null || columnDefaults.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> config = new HashMap<>();
    columnDefaults.forEach(
        (fieldId, value) -> config.put(COLUMN_DEFAULT_PREFIX + fieldId, value.toString()));
    return config;
  }

  /**
   * Table property {@code read-bridge.column-default.enabled} overrides HouseTables. A lookup
   * failure leaves the table unbridged (same as today's NULL reads).
   */
  private boolean isColumnDefaultRamped(TableDto tableDto) {
    try {
      return featureToggle.isFeatureActivatedWithOverride(tableDto, COLUMN_DEFAULT_FEATURE_ID);
    } catch (RuntimeException e) {
      log.warn(
          "read-bridge: toggle lookup failed for {}.{}; treating {} as not ramped",
          tableDto.getDatabaseId(),
          tableDto.getTableId(),
          COLUMN_DEFAULT_FEATURE_ID,
          e);
      return false;
    }
  }
}
