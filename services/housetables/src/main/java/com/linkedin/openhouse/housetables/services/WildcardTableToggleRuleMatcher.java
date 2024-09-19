package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.housetables.model.TableToggleRule;
import org.springframework.stereotype.Component;

/** An implementation of {@link TableToggleRuleMatcher} that supports '*' to match any entities */
@Component
public class WildcardTableToggleRuleMatcher implements TableToggleRuleMatcher {
  @Override
  public boolean matches(TableToggleRule rule, String tableId, String databaseId) {
    boolean tableMatches =
        rule.getTablePattern().equals("*") || rule.getTablePattern().equals(tableId);
    boolean databaseMatches =
        rule.getDatabasePattern().equals("*") || rule.getDatabasePattern().equals(databaseId);

    return tableMatches && databaseMatches;
  }
}
