package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Collections;
import java.util.Map;

/**
 * Deployment-supplied column defaults (data only). Keyed by Iceberg field-id; values are Iceberg
 * single-value JSON. Policy (the opt-in) lives in {@link ReadBridgeConfigResolver}.
 */
public interface ColumnDefaultsSource {

  /** Sentinel when no deployment bean is registered; the resolver never consults it. */
  ColumnDefaultsSource NONE = tableDto -> Collections.emptyMap();

  /**
   * Field-id → Iceberg single-value JSON. Empty/null stamps nothing. Throw {@link
   * ColumnDefaultException} if a declared default cannot be applied: omitting it would read as
   * NULL. Any other exception is a bug and propagates as itself.
   */
  Map<Integer, JsonNode> defaults(TableDto tableDto) throws ColumnDefaultException;
}
