package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
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
   * @throws UnsupportedClientOperationException if the source or ramp lookup cannot answer
   */
  public Map<Integer, String> stampedColumnDefaults(TableDto tableDto) {
    Objects.requireNonNull(tableDto, "tableDto");
    try {
      return columnDefaultsByFieldId(tableDto);
    } catch (RuntimeException e) {
      throw unusable(tableDto, e);
    }
  }

  /**
   * Write-path ramp. Toggle failure throws. {@code ColumnDefaultsSource.NONE} is never ramped, so
   * the toggle is not consulted.
   *
   * @throws UnsupportedClientOperationException if the ramp lookup cannot answer
   */
  public boolean isRampedForCommit(TableDto tableDto) {
    Objects.requireNonNull(tableDto, "tableDto");
    try {
      return isColumnDefaultRamped(tableDto);
    } catch (RuntimeException e) {
      throw unusable(tableDto, e);
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

  static UnsupportedClientOperationException unusable(TableDto tableDto, Throwable cause) {
    return unusable(tableDto, null, cause);
  }

  static UnsupportedClientOperationException unusable(
      TableDto incoming, TableDto existing, Throwable cause) {
    return unusable(incoming, existing, causeMessage(cause), cause);
  }

  static UnsupportedClientOperationException unusable(
      TableDto incoming, TableDto existing, String reason, Throwable cause) {
    UnsupportedClientOperationException thrown =
        new UnsupportedClientOperationException(
            UnsupportedClientOperationException.Operation.COLUMN_DEFAULT_UNUSABLE,
            String.format(
                "COLUMN_DEFAULT_UNUSABLE: OpenHouse could not validate column defaults on %s.%s, so"
                    + " the commit was rejected. Retry. If it persists, contact the OpenHouse team"
                    + " with the Spark application logs and the table metadata path: %s. Cause: %s",
                incoming.getDatabaseId(),
                incoming.getTableId(),
                metadataPath(incoming, existing),
                reason));
    if (cause != null) {
      thrown.initCause(cause);
    }
    return thrown;
  }

  private static String causeMessage(Throwable cause) {
    if (cause == null || cause.getMessage() == null) {
      return cause == null ? "unknown" : cause.toString();
    }
    return cause.getMessage();
  }

  private static String metadataPath(TableDto incoming, TableDto existing) {
    if (incoming != null
        && incoming.getTableLocation() != null
        && !incoming.getTableLocation().isEmpty()) {
      return incoming.getTableLocation();
    }
    if (existing != null
        && existing.getTableLocation() != null
        && !existing.getTableLocation().isEmpty()) {
      return existing.getTableLocation();
    }
    return "unavailable";
  }
}
