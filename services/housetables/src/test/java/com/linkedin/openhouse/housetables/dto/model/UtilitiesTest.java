package com.linkedin.openhouse.housetables.dto.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class UtilitiesTest {
  @Test
  public void testFieldMatchWithNullFilterObjShouldReturnTrue() {
    assertTrue(Utilities.fieldMatch("repoObject", null));
  }

  @Test
  public void testFieldMatchWithEqualObjectsShouldReturnTrue() {
    assertTrue(Utilities.fieldMatchCaseInsensitive("repoObject", "repoObject"));
  }

  @Test
  public void testFieldMatchWithEqualObjectsIgnoreCaseShouldReturnTrue() {
    assertTrue(Utilities.fieldMatchCaseInsensitive("repoObject", "REPOOBJECT"));
  }

  @Test
  public void testFieldMatchWithNonEqualObjectsShouldReturnFalse() {
    assertFalse(Utilities.fieldMatchCaseInsensitive("repoObject", "differentObject"));
  }

  @Test
  public void testFieldMatchWithNonStringObjectsShouldReturnFalse() {
    assertFalse(Utilities.fieldMatch(42, "42"));
  }

  @Test
  public void testFieldMatchWithNonStringEqualObjectsShouldReturnTrue() {
    assertTrue(Utilities.fieldMatch(42, 42));
  }

  @Test
  public void testFieldMatchWithNonEqualNonStringObjectsShouldReturnFalse() {
    assertFalse(Utilities.fieldMatch(42, 43));
  }
}
