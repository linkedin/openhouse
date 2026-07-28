package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Map;

/**
 * Pluggable input to the open-source {@code read-bridge} feature: the per-column initial-defaults
 * to overlay at read time, keyed by Iceberg field-id and valued as Iceberg single-value JSON.
 *
 * <p>This is the only part of read-bridge a deployment supplies. The open-source default (see
 * {@code ApiConfig}) returns nothing, so the feature is wired but inert until a deployment
 * overrides this bean (e.g. li-openhouse derives the defaults from the {@code avro.schema.literal}
 * table property).
 *
 * <p>Called on every table-load/commit response, so implementations must be cheap. An empty map
 * means "nothing to bridge for this table" — no default is declared, or a declared default is of a
 * kind this source does not support; either way the column keeps reading {@code NULL} as it does
 * today. Throw instead when a default <em>is</em> declared but cannot be honored (e.g. it does not
 * bind to its column's type): degrading there would leave the column reading {@code NULL} while the
 * table claims to be bridged, hiding a real defect.
 */
public interface ColumnDefaultsSource {
  /**
   * @param tableDto the already-loaded table state (no extra fetch needed)
   * @return field-id -&gt; initial-default as Iceberg single-value JSON; empty/{@code null} = none
   */
  Map<Integer, JsonNode> defaults(TableDto tableDto);
}
