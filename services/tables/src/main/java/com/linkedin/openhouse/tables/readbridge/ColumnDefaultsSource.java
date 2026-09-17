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
   * Field-id → Iceberg single-value JSON. Empty/null stamps nothing. Omit absent or valid null
   * defaults and explicitly unsupported representations. Throw when a supported declared default is
   * malformed, incompatible, or cannot be converted safely; do not report failure as absence. The
   * resolver fail-opens GET and fails closed on writes when the source throws.
   */
  Map<Integer, JsonNode> defaults(TableDto tableDto);
}
