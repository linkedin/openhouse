package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Collections;
import java.util.Map;

/**
 * Column defaults for one table, keyed by Iceberg field-id. Values are Iceberg single-value JSON.
 * Ramp is {@link ReadBridgeConfigResolver}.
 */
public interface ColumnDefaultsSource {

  /** Used when no deployment bean is registered. */
  ColumnDefaultsSource NONE = tableDto -> Collections.emptyMap();

  /**
   * Field-id to default JSON. Empty/null stamps nothing. Throw if a declared default cannot bind.
   */
  Map<Integer, JsonNode> defaults(TableDto tableDto);
}
