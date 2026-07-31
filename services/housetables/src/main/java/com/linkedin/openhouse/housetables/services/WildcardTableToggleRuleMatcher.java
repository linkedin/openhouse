package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.housetables.model.TableToggleRule;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.PathMatcher;

/**
 * A {@link TableToggleRuleMatcher} matching database and table names as case-sensitive globs, so
 * {@code *} matches any run of characters wherever it appears and {@code ?} matches one.
 *
 * <p>TODO: patterns are unvalidated because rules are inserted straight into MySQL, so a malformed
 * pattern is only discovered by matching nothing. Validation belongs with a rule-write path, which
 * does not exist yet.
 */
@Component
public class WildcardTableToggleRuleMatcher implements TableToggleRuleMatcher {
  private static final PathMatcher MATCHER = new AntPathMatcher();

  @Override
  public boolean matches(TableToggleRule rule, String tableId, String databaseId) {
    return MATCHER.match(rule.getTablePattern(), tableId)
        && MATCHER.match(rule.getDatabasePattern(), databaseId);
  }
}
