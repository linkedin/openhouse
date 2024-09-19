package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.housetables.model.TableToggleRule;

public interface TableToggleRuleMatcher {
  boolean matches(TableToggleRule rule, String tableId, String databaseId);
}
