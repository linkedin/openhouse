package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.util.Locale;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Per-database on/off switch for the /v1 views API. Off by default: when a database is not enabled,
 * every view get, list, put and delete in it fails with {@code VIEWS_DISABLED} (404).
 *
 * <p>The switch is a {@code views} feature rule in the House Tables {@code table_toggle_rule} MySQL
 * table; there is no API to write rules. To enable views for database {@code db1}, insert:
 *
 * <pre>
 * INSERT INTO table_toggle_rule (feature, database_pattern, table_pattern)
 * VALUES ('views', 'db1', '*');
 * </pre>
 *
 * <p>{@code database_pattern} is a case-sensitive glob matched against the lower-cased database id,
 * so write it in lower case; {@code '*'} enables every database. To disable, delete the row. Rules
 * are read on every request, so changes take effect immediately.
 *
 * <p>A {@code views.enabled} table property cannot turn views on: the gate never consults table
 * property overrides.
 */
@Component
public class ViewsFeatureGate {

  /**
   * Fixed, {@code ALPHA_NUM_UNDERSCORE}-valid probe table id. The gate decision is purely
   * per-database: this constant is never a real view id, so the gate can never accidentally key on
   * a per-view toggle.
   */
  public static final String GATE_PROBE_TABLE_ID = "views_gate_probe";

  private static final String FEATURE_ID = "views";

  private final TableFeatureToggle tableFeatureToggle;

  @Autowired
  public ViewsFeatureGate(TableFeatureToggle tableFeatureToggle) {
    this.tableFeatureToggle = tableFeatureToggle;
  }

  public boolean isEnabled(String databaseId) {
    Objects.requireNonNull(databaseId, "databaseId must not be null");
    return tableFeatureToggle.isFeatureActivated(
        databaseId.toLowerCase(Locale.ROOT), GATE_PROBE_TABLE_ID, FEATURE_ID);
  }
}
