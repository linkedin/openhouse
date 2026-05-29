package com.linkedin.openhouse.internal.catalog;

import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OpenHouseInternalCatalogTest {

  @Test
  void testIsValidIdentifierRequiresDatabaseTableShape() {
    TestOpenHouseInternalCatalog catalog = new TestOpenHouseInternalCatalog();

    Assertions.assertFalse(catalog.isValidBaseIdentifier(TableIdentifier.of("db")));
    Assertions.assertTrue(catalog.isValidBaseIdentifier(TableIdentifier.of("db", "table")));
    Assertions.assertTrue(catalog.isValidBaseIdentifier(TableIdentifier.of("db", "partitions")));
    Assertions.assertFalse(
        catalog.isValidBaseIdentifier(TableIdentifier.of("db", "table", "partitions")));
  }

  private static class TestOpenHouseInternalCatalog extends OpenHouseInternalCatalog {

    boolean isValidBaseIdentifier(TableIdentifier identifier) {
      return isValidIdentifier(identifier);
    }
  }
}
