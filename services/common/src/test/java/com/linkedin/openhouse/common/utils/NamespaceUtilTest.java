package com.linkedin.openhouse.common.utils;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NamespaceUtilTest {

  @Test
  public void testIsSingleLevel() {
    Assertions.assertFalse(NamespaceUtil.isSingleLevel(null));
    Assertions.assertFalse(NamespaceUtil.isSingleLevel(Namespace.empty()));
    Assertions.assertTrue(NamespaceUtil.isSingleLevel(Namespace.of("db")));
    Assertions.assertFalse(NamespaceUtil.isSingleLevel(Namespace.of("a", "b")));
    Assertions.assertFalse(NamespaceUtil.isSingleLevel(Namespace.of("a", "b", "c")));
  }

  @Test
  public void testCheckAtMostSingleLevelNamespaceAllowsEmpty() {
    Assertions.assertDoesNotThrow(
        () -> NamespaceUtil.checkAtMostSingleLevelNamespace(Namespace.empty()));
  }

  @Test
  public void testCheckAtMostSingleLevelNamespaceAllowsSingleLevel() {
    Assertions.assertDoesNotThrow(
        () -> NamespaceUtil.checkAtMostSingleLevelNamespace(Namespace.of("db")));
  }

  @Test
  public void testCheckAtMostSingleLevelNamespaceRejectsMultiLevel() {
    ValidationException twoLevel =
        Assertions.assertThrows(
            ValidationException.class,
            () -> NamespaceUtil.checkAtMostSingleLevelNamespace(Namespace.of("a", "b")));
    Assertions.assertEquals("Input namespace has more than one levels a.b", twoLevel.getMessage());

    ValidationException threeLevel =
        Assertions.assertThrows(
            ValidationException.class,
            () -> NamespaceUtil.checkAtMostSingleLevelNamespace(Namespace.of("a", "b", "c")));
    Assertions.assertEquals(
        "Input namespace has more than one levels a.b.c", threeLevel.getMessage());
  }
}
