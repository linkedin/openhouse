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
   * Field-id → Iceberg single-value JSON. Empty/null stamps nothing. Throw {@link
   * ColumnDefaultException} if a declared default cannot be applied: omitting it would read as
   * NULL. Callers blame the request or the stored table depending on which one was passed. Any
   * other exception is a bug and fails the request as itself.
   */
  Map<Integer, JsonNode> defaults(TableDto tableDto) throws ColumnDefaultException;
}
