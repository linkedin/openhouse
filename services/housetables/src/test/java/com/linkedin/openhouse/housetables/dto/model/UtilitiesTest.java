package com.linkedin.openhouse.housetables.dto.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class UtilitiesTest {
  @Test
  public void testFieldMatch_withNullFilterObj_shouldReturnTrue() {
    assertTrue(Utilities.fieldMatch("repoObject", null));
  }

  @Test
  public void testFieldMatch_withEqualObjects_shouldReturnTrue() {
    assertTrue(Utilities.fieldMatchCaseInsensitive("repoObject", "repoObject"));
  }

  @Test
  public void testFieldMatch_withEqualObjectsIgnoreCase_shouldReturnTrue() {
    assertTrue(Utilities.fieldMatchCaseInsensitive("repoObject", "REPOOBJECT"));
  }

  @Test
  public void testFieldMatch_withNonEqualObjects_shouldReturnFalse() {
    assertFalse(Utilities.fieldMatchCaseInsensitive("repoObject", "differentObject"));
  }

  @Test
  public void testFieldMatch_withNonStringObjects_shouldReturnFalse() {
    assertFalse(Utilities.fieldMatch(42, "42"));
  }

  @Test
  public void testFieldMatch_withNonStringEqualObjects_shouldReturnTrue() {
    assertTrue(Utilities.fieldMatch(42, 42));
  }

  @Test
  public void testFieldMatch_withNonEqualNonStringObjects_shouldReturnFalse() {
    assertFalse(Utilities.fieldMatch(42, 43));
  }
}
