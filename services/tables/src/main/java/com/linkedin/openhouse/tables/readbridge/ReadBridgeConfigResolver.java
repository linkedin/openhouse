package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
    this.columnDefaultsSource =
        Objects.requireNonNull(columnDefaultsSource, "columnDefaultsSource");
    this.featureToggle = Objects.requireNonNull(featureToggle, "featureToggle");
  }

  /** Merges independently gated capabilities; empty when nothing is bridged. */
  public Map<String, String> resolve(TableDto tableDto) {
    Objects.requireNonNull(tableDto, "tableDto");
    Map<String, String> config = new HashMap<>();
    config.putAll(columnDefaultConfig(tableDto));
    return config;
  }

  /**
   * Write-path stamps, keyed by Iceberg field-id. Empty when there is no source or the table is not
   * ramped. Toggle or source failure throws — the write path fail-closes; {@link #resolve} does
   * not.
   *
   * @throws ColumnDefaultException if the source or ramp lookup cannot answer
   */
  public Map<Integer, String> stampedColumnDefaults(TableDto tableDto)
      throws ColumnDefaultException {
    Objects.requireNonNull(tableDto, "tableDto");
    try {
      return columnDefaultsByFieldId(tableDto);
    } catch (RuntimeException e) {
      throw ColumnDefaultException.unusable(tableDto, e);
    }
  }

  /**
   * Write-path ramp. Toggle failure throws. {@code ColumnDefaultsSource.NONE} is never ramped, so
   * the toggle is not consulted.
   *
   * @throws ColumnDefaultException if the ramp lookup cannot answer
   */
  public boolean isRampedForCommit(TableDto tableDto) throws ColumnDefaultException {
    Objects.requireNonNull(tableDto, "tableDto");
    try {
      return isColumnDefaultRamped(tableDto);
    } catch (RuntimeException e) {
      throw ColumnDefaultException.unusable(tableDto, e);
    }
  }

  private Map<String, String> columnDefaultConfig(TableDto tableDto) {
    Map<Integer, String> byId;
    try {
      byId = columnDefaultsByFieldId(tableDto);
    } catch (RuntimeException e) {
      log.warn(
          "read-bridge: column-defaults lookup failed for {}.{}; treating as not bridged",
          tableDto.getDatabaseId(),
          tableDto.getTableId(),
          e);
      return Collections.emptyMap();
    }
    if (byId.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> config = new HashMap<>();
    byId.forEach((fieldId, json) -> config.put(COLUMN_DEFAULT_PREFIX + fieldId, json));
    return config;
  }

  private Map<Integer, String> columnDefaultsByFieldId(TableDto tableDto) {
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
    Map<Integer, String> byId = new HashMap<>();
    columnDefaults.forEach(
        (fieldId, value) -> {
          if (fieldId != null && value != null) {
            byId.put(fieldId, value.toString());
          }
        });
    return byId;
  }

  /**
   * Uses {@link TableFeatureToggle#isFeatureActivatedWithOverride} so {@code
   * read-bridge.column-default.enabled} can opt in/out without HTS. GET fail-opens on lookup
   * errors: not bridging equals today's NULL reads. The write path fail-closes instead.
   */
  private boolean isColumnDefaultRamped(TableDto tableDto) {
    if (columnDefaultsSource == ColumnDefaultsSource.NONE) {
      return false;
    }
    return featureToggle.isFeatureActivatedWithOverride(tableDto, COLUMN_DEFAULT_FEATURE_ID);
  }
}
