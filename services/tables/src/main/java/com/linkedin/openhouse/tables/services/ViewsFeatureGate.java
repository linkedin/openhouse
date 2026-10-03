package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.util.Locale;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Database-scoped {@code views.enabled} gate.
 *
 * <p>Default off: with no matching {@code views} {@link
 * com.linkedin.openhouse.tables.toggle.model.TableToggleRule} the gate reports disabled. Enabling a
 * database is an external administrative rollout prerequisite: an operator provisions the rule
 * directly in the HTS toggle-rule store out of band; there is no supported write API and none is
 * added here.
 *
 * <p>Calls only {@link TableFeatureToggle#isFeatureActivated(String, String, String)}, never {@link
 * TableFeatureToggle#isFeatureActivatedWithOverride}, so a user-writable {@code views.enabled}
 * table property can never toggle views on.
 *
 * <p>The underlying rule matcher ({@code WildcardTableToggleRuleMatcher}, via {@code
 * AntPathMatcher}) is case-sensitive, while HTS identity is case-insensitive, so the database id is
 * canonicalized with {@link Locale#ROOT} (never the default-locale form) before the probe, and
 * provisioning uses the matching lower-case literal {@code databasePattern}.
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
