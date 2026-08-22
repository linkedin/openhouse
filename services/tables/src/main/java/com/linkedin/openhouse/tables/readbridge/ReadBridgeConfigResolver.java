package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Stamps per-table {@code config} for read-bridge capabilities. Owns policy (feature id, ramp,
 * keys); deployments supply data via {@link ColumnDefaultsSource}.
 */
@Slf4j
public class ReadBridgeConfigResolver {

  /** Capability id; also names {@code <id>.enabled} and the config key prefix below. */
  public static final String COLUMN_DEFAULT_FEATURE_ID = "read-bridge.column-default";

  /** Client contract: {@code openhouse.read-bridge.column-default.<fieldId>}. */
  public static final String COLUMN_DEFAULT_PREFIX = "openhouse." + COLUMN_DEFAULT_FEATURE_ID + ".";

  private final ColumnDefaultsSource columnDefaultsSource;

  private final TableFeatureToggle featureToggle;

  public ReadBridgeConfigResolver(
      ColumnDefaultsSource columnDefaultsSource, TableFeatureToggle featureToggle) {
    this.columnDefaultsSource = columnDefaultsSource;
    this.featureToggle = featureToggle;
  }

  /** Merges independently gated capabilities; empty when nothing is bridged. */
  public Map<String, String> resolve(TableDto tableDto) {
    Map<String, String> config = new HashMap<>();
    config.putAll(columnDefaultConfig(tableDto));
    return config;
  }

  /**
   * Write-path stamps, keyed by Iceberg field-id. Empty when there is no source or the table is not
   * ramped. Toggle or source failure throws — the write path fail-closes; {@link #resolve} does
   * not.
   */
  public Map<Integer, String> stampedColumnDefaults(TableDto tableDto) {
    return columnDefaultsByFieldId(tableDto, true);
  }

  /**
   * Write-path ramp. Toggle failure throws. {@code ColumnDefaultsSource.NONE} is never ramped, so
   * the toggle is not consulted.
   */
  public boolean isRampedForCommit(TableDto tableDto) {
    return isColumnDefaultRamped(tableDto, true);
  }

  private Map<String, String> columnDefaultConfig(TableDto tableDto) {
    Map<Integer, String> byId = columnDefaultsByFieldId(tableDto, false);
    if (byId.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> config = new HashMap<>();
    byId.forEach((fieldId, json) -> config.put(COLUMN_DEFAULT_PREFIX + fieldId, json));
    return config;
  }

  private Map<Integer, String> columnDefaultsByFieldId(TableDto tableDto, boolean failClosed) {
    if (columnDefaultsSource == ColumnDefaultsSource.NONE) {
      return Collections.emptyMap();
    }
    if (!isColumnDefaultRamped(tableDto, failClosed)) {
      return Collections.emptyMap();
    }
    Map<Integer, JsonNode> columnDefaults;
    try {
      columnDefaults = columnDefaultsSource.defaults(tableDto);
    } catch (RuntimeException e) {
      if (failClosed) {
        throw new IllegalStateException(
            "read-bridge: column-defaults source failed for "
                + tableDto.getDatabaseId()
                + "."
                + tableDto.getTableId(),
            e);
      }
      log.warn(
          "read-bridge: column-defaults source failed for {}.{}; treating as not bridged",
          tableDto.getDatabaseId(),
          tableDto.getTableId(),
          e);
      return Collections.emptyMap();
    }
    if (columnDefaults == null || columnDefaults.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<Integer, String> byId = new HashMap<>();
    columnDefaults.forEach((fieldId, value) -> byId.put(fieldId, value.toString()));
    return byId;
  }

  /**
   * Uses {@link TableFeatureToggle#isFeatureActivatedWithOverride} so {@code
   * read-bridge.column-default.enabled} can opt in/out without HTS. GET fail-opens on lookup
   * errors: not bridging equals today's NULL reads. The write path fail-closes instead.
   */
  private boolean isColumnDefaultRamped(TableDto tableDto, boolean failClosed) {
    if (columnDefaultsSource == ColumnDefaultsSource.NONE) {
      return false;
    }
    try {
      return featureToggle.isFeatureActivatedWithOverride(tableDto, COLUMN_DEFAULT_FEATURE_ID);
    } catch (RuntimeException e) {
      if (failClosed) {
        throw new IllegalStateException(
            "read-bridge: toggle lookup failed for "
                + tableDto.getDatabaseId()
                + "."
                + tableDto.getTableId(),
            e);
      }
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
