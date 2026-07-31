package com.linkedin.openhouse.tables.toggle;

import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface to check if a feature is toggled-on for a table.
 *
 * <p>TODO: the two forms below differ in who may influence the decision, which today is conveyed by
 * their names and javadoc rather than enforced. The intended model declares that per feature
 * instead of per call site — {@code TableFeature.capability(id)} vs {@code
 * TableFeature.rollout(id)} — so a permission-bearing feature cannot acquire self-service opt-in by
 * calling the wrong method. The same change would carry a decision's cause for metrics, give rules
 * an explicit effect, priority and expiry rather than presence-implies-active, and evaluate rules
 * from a locally replicated snapshot so a table read no longer blocks on HouseTables.
 */
public interface TableFeatureToggle {
  Logger LOG = LoggerFactory.getLogger(TableFeatureToggle.class);

  /** Suffix appended to a feature id to form its self-service table property. */
  String ENABLED_PROPERTY_SUFFIX = ".enabled";

  /**
   * Determines the server-side activation decision for a table.
   *
   * <p>Authorization gates — features deciding whether a user may write an otherwise preserved
   * property, like {@code enable_mor} — must use this form, since the table property honored by
   * {@link #isFeatureActivatedWithOverride(TableDto, String)} is writable by the user being gated.
   */
  boolean isFeatureActivated(String databaseId, String tableId, String featureId);

  /**
   * Determines activation, letting the table override the server-side decision.
   *
   * <p>An explicit {@code <featureId>.enabled} property opts the table in or out; when absent, the
   * server-side toggle decides. An unparseable value fails closed, so a typo cannot make the table
   * unusable.
   */
  default boolean isFeatureActivatedWithOverride(TableDto tableDto, String featureId) {
    Map<String, String> properties = tableDto.getTableProperties();
    String tableProperty = featureId + ENABLED_PROPERTY_SUFFIX;
    String override = properties == null ? null : properties.get(tableProperty);
    if (override == null) {
      return isFeatureActivated(tableDto.getDatabaseId(), tableDto.getTableId(), featureId);
    }

    String normalized = override.trim();
    if ("true".equalsIgnoreCase(normalized)) {
      return true;
    }
    if ("false".equalsIgnoreCase(normalized)) {
      return false;
    }
    LOG.warn(
        "Ignoring unparseable table property {}={} for {}.{}; treating feature {} as inactive",
        tableProperty,
        override,
        tableDto.getDatabaseId(),
        tableDto.getTableId(),
        featureId);
    return false;
  }
}
