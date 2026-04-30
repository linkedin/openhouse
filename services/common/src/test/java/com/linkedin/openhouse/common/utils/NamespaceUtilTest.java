package com.linkedin.openhouse.common.utils;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NamespaceUtilTest {

  @Test
  public void testIsTableNamespace() {
    Assertions.assertFalse(NamespaceUtil.isTableNamespace(null));
    Assertions.assertFalse(NamespaceUtil.isTableNamespace(Namespace.empty()));
    Assertions.assertTrue(NamespaceUtil.isTableNamespace(Namespace.of("db")));
    Assertions.assertFalse(NamespaceUtil.isTableNamespace(Namespace.of("a", "b")));
    Assertions.assertFalse(NamespaceUtil.isTableNamespace(Namespace.of("a", "b", "c")));
  }

  @Test
  public void testValidateOperationNamespaceAllowsEmpty() {
    Assertions.assertDoesNotThrow(
        () -> NamespaceUtil.validateOperationNamespace(Namespace.empty()));
  }

  @Test
  public void testValidateOperationNamespaceAllowsSingleLevel() {
    Assertions.assertDoesNotThrow(
        () -> NamespaceUtil.validateOperationNamespace(Namespace.of("db")));
  }

  @Test
  public void testValidateOperationNamespaceRejectsMultiLevel() {
    ValidationException twoLevel =
        Assertions.assertThrows(
            ValidationException.class,
            () -> NamespaceUtil.validateOperationNamespace(Namespace.of("a", "b")));
    Assertions.assertEquals("Input namespace has more than one levels a.b", twoLevel.getMessage());

    ValidationException threeLevel =
        Assertions.assertThrows(
            ValidationException.class,
            () -> NamespaceUtil.validateOperationNamespace(Namespace.of("a", "b", "c")));
    Assertions.assertEquals(
        "Input namespace has more than one levels a.b.c", threeLevel.getMessage());
  }
}
