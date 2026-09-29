package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Stamps per-table {@code config} for read-bridge capabilities. Owns policy (feature id, ramp,
 * keys); deployments supply data via {@link ColumnDefaultsSource}.
 */
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

  /**
   * Merges independently gated capabilities; empty when nothing is bridged. A failing source or
   * ramp lookup fails the read: omitting defaults would silently read NULL.
   *
   * @throws ColumnDefaultException if the source or ramp lookup cannot answer
   */
  public Map<String, String> resolve(TableDto tableDto) throws ColumnDefaultException {
    Objects.requireNonNull(tableDto, "tableDto");
    Map<String, String> config = new HashMap<>();
    config.putAll(columnDefaultConfig(tableDto));
    return config;
  }

  /**
   * Stamps keyed by Iceberg field-id. Empty when there is no source or the table is not ramped.
   * Toggle or source failures propagate on both reads and writes; source-reported failures keep
   * their reason and column context.
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

  private Map<String, String> columnDefaultConfig(TableDto tableDto) throws ColumnDefaultException {
    Map<Integer, String> byId = stampedColumnDefaults(tableDto);
    if (byId.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> config = new HashMap<>();
    byId.forEach((fieldId, json) -> config.put(COLUMN_DEFAULT_PREFIX + fieldId, json));
    return config;
  }

  private Map<Integer, String> columnDefaultsByFieldId(TableDto tableDto)
      throws ColumnDefaultException {
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
    // A null entry would silently drop a declared default.
    columnDefaults.forEach(
        (fieldId, value) ->
            byId.put(
                Objects.requireNonNull(fieldId, "Column-default field id is null"),
                Objects.requireNonNull(value, "Column-default value is null").toString()));
    return byId;
  }

  /**
   * Uses {@link TableFeatureToggle#isFeatureActivatedWithOverride} so {@code
   * read-bridge.column-default.enabled} can opt in/out without HTS. A lookup failure fails the read
   * or write rather than silently omitting defaults.
   */
  private boolean isColumnDefaultRamped(TableDto tableDto) {
    if (columnDefaultsSource == ColumnDefaultsSource.NONE) {
      return false;
    }
    return featureToggle.isFeatureActivatedWithOverride(tableDto, COLUMN_DEFAULT_FEATURE_ID);
  }
}
