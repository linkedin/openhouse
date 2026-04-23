package com.linkedin.openhouse.common.utils;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NamespaceUtilTest {

  @Test
  public void testCheckSingleLevelNamespaceAllowsEmpty() {
    Assertions.assertDoesNotThrow(() -> NamespaceUtil.checkSingleLevelNamespace(Namespace.empty()));
  }

  @Test
  public void testCheckSingleLevelNamespaceAllowsSingleLevel() {
    Assertions.assertDoesNotThrow(
        () -> NamespaceUtil.checkSingleLevelNamespace(Namespace.of("db")));
  }

  @Test
  public void testCheckSingleLevelNamespaceRejectsMultiLevel() {
    ValidationException twoLevel =
        Assertions.assertThrows(
            ValidationException.class,
            () -> NamespaceUtil.checkSingleLevelNamespace(Namespace.of("a", "b")));
    Assertions.assertEquals("Input namespace has more than one levels a.b", twoLevel.getMessage());

    ValidationException threeLevel =
        Assertions.assertThrows(
            ValidationException.class,
            () -> NamespaceUtil.checkSingleLevelNamespace(Namespace.of("a", "b", "c")));
    Assertions.assertEquals(
        "Input namespace has more than one levels a.b.c", threeLevel.getMessage());
  }
}
