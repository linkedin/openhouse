package com.linkedin.openhouse.housetables.mock;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.housetables.model.TableToggleRule;
import com.linkedin.openhouse.housetables.services.WildcardTableToggleRuleMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class WildcardTableToggleRuleMatcherTest {

  @Mock private TableToggleRule mockRule;

  private WildcardTableToggleRuleMatcher matcher;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    matcher = new WildcardTableToggleRuleMatcher();
  }

  @Test
  void testExactMatch() {
    when(mockRule.getTablePattern()).thenReturn("table1");
    when(mockRule.getDatabasePattern()).thenReturn("db1");

    assertTrue(matcher.matches(mockRule, "table1", "db1"));
    assertFalse(matcher.matches(mockRule, "table2", "db1"));
    assertFalse(matcher.matches(mockRule, "table1", "db2"));
  }

  @Test
  void testWildcardTableMatch() {
    when(mockRule.getTablePattern()).thenReturn("*");
    when(mockRule.getDatabasePattern()).thenReturn("db1");

    assertTrue(matcher.matches(mockRule, "table1", "db1"));
    assertTrue(matcher.matches(mockRule, "table2", "db1"));
    assertFalse(matcher.matches(mockRule, "table1", "db2"));
  }

  @Test
  void testWildcardDatabaseMatch() {
    when(mockRule.getTablePattern()).thenReturn("table1");
    when(mockRule.getDatabasePattern()).thenReturn("*");

    assertTrue(matcher.matches(mockRule, "table1", "db1"));
    assertTrue(matcher.matches(mockRule, "table1", "db2"));
    assertFalse(matcher.matches(mockRule, "table2", "db1"));
  }

  @Test
  void testBothWildcardMatch() {
    when(mockRule.getTablePattern()).thenReturn("*");
    when(mockRule.getDatabasePattern()).thenReturn("*");

    assertTrue(matcher.matches(mockRule, "table1", "db1"));
    assertTrue(matcher.matches(mockRule, "table2", "db2"));
    assertTrue(matcher.matches(mockRule, "anyTable", "anyDb"));
  }

  @Test
  void testNoMatch() {
    when(mockRule.getTablePattern()).thenReturn("table1");
    when(mockRule.getDatabasePattern()).thenReturn("db1");

    assertFalse(matcher.matches(mockRule, "table2", "db2"));
  }
}
