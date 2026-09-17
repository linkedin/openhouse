package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Collections;
import java.util.Map;

/**
 * Deployment-supplied column defaults (data only). Keyed by Iceberg field-id; values are Iceberg
 * single-value JSON. Policy/ramp lives in {@link ReadBridgeConfigResolver}.
 */
public interface ColumnDefaultsSource {

  /** Sentinel when no deployment bean is registered; resolver short-circuits before HTS. */
  ColumnDefaultsSource NONE = tableDto -> Collections.emptyMap();

  /**
   * Field-id → Iceberg single-value JSON. Omit absent or valid null defaults and explicitly
   * unsupported representations. Invalid supported declarations must throw a checked failure, never
   * return an empty or partial success. Sources identify the reason and field context; operation
   * handlers own fallback, logging, and user-facing responses.
   *
   * @throws ColumnDefaultException if supported defaults cannot be derived safely
   */
  Map<Integer, JsonNode> defaults(TableDto tableDto) throws ColumnDefaultException;
}
