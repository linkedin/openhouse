package com.linkedin.openhouse.tables.readbridge;

import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.common.exception.DependencyUnavailableException;
import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Builds the per-table {@code config} that read-bridge clients apply. Owns policy (feature id,
 * ramp, keys); deployments supply data via {@link ColumnDefaultsSource}.
 */
public class ReadBridgeConfigResolver {

  /** Capability id; also names {@code <id>.enabled} and the config key prefix below. */
  public static final String COLUMN_DEFAULT_FEATURE_ID = "read-bridge.column-default";

  /**
   * Table property: {@code true} turns column defaults on for the table and {@code false} turns
   * them off; without it, the rollout toggle decides. {@link ColumnDefaultCommitPolicy} keeps a
   * committed {@code true} from being changed or removed unless the request carries the
   * column-default policy bypass.
   */
  public static final String COLUMN_DEFAULT_ENABLED_PROP = COLUMN_DEFAULT_FEATURE_ID + ".enabled";

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
   * Merges the config of every read-bridge capability that applies to the table; empty when none
   * does. Config that cannot be produced fails instead of being left out: the client would silently
   * run without it.
   *
   * @throws TableConfigUnavailableException if the table's stored settings cannot be applied
   * @throws DependencyUnavailableException if the ramp lookup cannot answer
   */
  public Map<String, String> resolve(TableDto tableDto) {
    return storedColumnDefaults(tableDto).entrySet().stream()
        .collect(toMap(e -> COLUMN_DEFAULT_PREFIX + e.getKey(), Map.Entry::getValue));
  }

  /**
   * The column defaults declared by a table a write sends, as Iceberg single-value JSON keyed by
   * field ID. Empty when there is no source or the table is not bridged.
   *
   * @throws ColumnDefaultException if a default the table declares cannot be applied
   * @throws DependencyUnavailableException if the ramp lookup cannot answer
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
   * @throws DependencyUnavailableException if the ramp lookup cannot answer
   */
  public Map<Integer, String> storedColumnDefaults(TableDto storedTable) {
    try {
      return columnDefaultsByFieldId(storedTable);
    } catch (ColumnDefaultException e) {
      throw new TableConfigUnavailableException(e.getMessage(), e);
    }
  }

  /**
   * Whether read-bridge applies to the table: its capability is ramped or the table opts in. {@code
   * ColumnDefaultsSource.NONE} is never bridged, so the toggle is not consulted.
   *
   * @throws DependencyUnavailableException if the ramp lookup cannot answer
   */
  public boolean isBridged(TableDto tableDto) {
    Objects.requireNonNull(tableDto, "tableDto");
    return isColumnDefaultRamped(tableDto);
  }

  private Map<Integer, String> columnDefaultsByFieldId(TableDto tableDto)
      throws ColumnDefaultException {
    Objects.requireNonNull(tableDto, "tableDto");
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
   * read-bridge.column-default.enabled} can opt in/out without HTS. The table property may be set
   * at creation or on an existing table. A committed {@code true} is immutable unless the client
   * sends the column-default policy bypass; {@code false} and an absent property remain changeable,
   * and absence still follows the ramp.
   */
  private boolean isColumnDefaultRamped(TableDto tableDto) {
    if (columnDefaultsSource == ColumnDefaultsSource.NONE) {
      return false;
    }
    return featureToggle.isFeatureActivatedWithOverride(tableDto, COLUMN_DEFAULT_FEATURE_ID);
  }
}
