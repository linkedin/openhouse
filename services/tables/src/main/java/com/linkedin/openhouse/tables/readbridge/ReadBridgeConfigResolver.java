package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.common.exception.DependencyUnavailableException;
import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
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
  public static final String COLUMN_DEFAULT_FEATURE_ID = CatalogConstants.COLUMN_DEFAULT_FEATURE_ID;

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
   * does. Config the server cannot produce fails the request instead of being left out: the client
   * would silently run without it.
   *
   * @throws TableConfigUnavailableException if the table's stored settings cannot be applied
   * @throws DependencyUnavailableException if the ramp lookup cannot answer
   */
  public Map<String, String> resolve(TableDto tableDto) {
    Map<String, String> config = new HashMap<>();
    storedColumnDefaults(tableDto)
        .forEach((fieldId, json) -> config.put(COLUMN_DEFAULT_PREFIX + fieldId, json));
    return config;
  }

  /**
   * Stamps for a table a request sends, keyed by Iceberg field-id. Empty when there is no source or
   * the table is not ramped. A default the request declares that cannot be applied is the request's
   * fault.
   *
   * @throws UnsupportedClientOperationException if a default the request declares cannot be applied
   * @throws DependencyUnavailableException if the ramp lookup cannot answer
   */
  public Map<Integer, String> incomingColumnDefaults(TableDto incoming) {
    try {
      return columnDefaultsByFieldId(incoming);
    } catch (ColumnDefaultException e) {
      UnsupportedClientOperationException rejected =
          new UnsupportedClientOperationException(
              UnsupportedClientOperationException.Operation.COLUMN_DEFAULT_UNUSABLE,
              e.getMessage());
      rejected.initCause(e);
      throw rejected;
    }
  }

  /**
   * Stamps for a stored table. A stored default that cannot be applied is the server's state at
   * fault, not the request's.
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
   * Write-path ramp. {@code ColumnDefaultsSource.NONE} is never ramped, so the toggle is not
   * consulted.
   *
   * @throws DependencyUnavailableException if the ramp lookup cannot answer
   */
  public boolean isRampedForCommit(TableDto tableDto) {
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
