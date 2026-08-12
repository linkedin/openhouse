package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Collections;
import java.util.Map;

/**
 * Pluggable input to the open-source {@code read-bridge} feature: the per-column initial-defaults
 * to overlay at read time, keyed by Iceberg field-id and valued as Iceberg single-value JSON.
 *
 * <p>This is the only part of read-bridge a deployment supplies, and it supplies <em>data only</em>
 * — not policy. Whether a table is bridged at all is decided by {@link ReadBridgeConfigResolver}
 * from the open-source feature toggle, so an implementation neither consults nor knows about the
 * ramp. Deriving the defaults — from whatever a deployment treats as the authority on a column's
 * declared default — is the one deployment-specific step, and the only reason this interface
 * exists.
 *
 * <p>No open-source default bean exists: {@code ApiConfig} resolves the type through an {@code
 * ObjectProvider} and falls back to {@link #NONE}, so the feature is wired but completely inert out
 * of the box — including skipping the toggle lookup entirely.
 *
 * <p>{@code JsonNode} rather than {@code String} is deliberate. It makes a stamped value
 * well-formed <em>by construction</em> at the only place that produces one, which is what entitles
 * the client decoder to treat a malformed entry as a bug and fail loud instead of degrading.
 *
 * <p>Called only for tables the ramp has activated, so an implementation may do real work (parsing
 * a schema, say) without paying it on every table load fleet-wide. An empty map means "nothing to
 * bridge for this table" — no default is declared, or a declared default is of a kind this source
 * does not support; either way the column keeps reading {@code NULL} as it does today. Throw
 * instead when a default <em>is</em> declared but cannot be honored (e.g. it does not bind to its
 * column's type): degrading there would leave the column reading {@code NULL} while the table
 * claims to be bridged, hiding a real defect.
 */
public interface ColumnDefaultsSource {

  /**
   * Supplies nothing. The value {@code ApiConfig} falls back to when a deployment supplies no
   * source; {@link ReadBridgeConfigResolver} recognises it and short-circuits before the toggle.
   */
  ColumnDefaultsSource NONE = tableDto -> Collections.emptyMap();

  /**
   * @param tableDto the already-loaded table state (no extra fetch needed)
   * @return field-id -&gt; initial-default as Iceberg single-value JSON; empty/{@code null} = none
   */
  Map<Integer, JsonNode> defaults(TableDto tableDto);
}
