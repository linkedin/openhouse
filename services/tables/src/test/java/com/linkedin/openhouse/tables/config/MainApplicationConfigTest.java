package com.linkedin.openhouse.tables.config;

import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MainApplicationConfigTest {

  @Test
  public void testRawPathMapping() {
    // Following are the actual exhausted list of paths in Tables services at version
    Set<String> paths = new HashSet<>();
    paths.add("/v0/databases");
    paths.add("/v0.9/databases");
    paths.add("/v1/databases");
    paths.add("/databases");
    paths.add("/databases/{databaseId}/aclPolicies");
    paths.add("/v0/databases/{databaseId}/aclPolicies");
    paths.add("/databases/{databaseId}/tables/{tableId}/aclPolicies");
    paths.add("/v0/databases/{databaseId}/tables/{tableId}/aclPolicies");
    paths.add("/v0/databases/{databaseId}/tables");
    paths.add("/databases/{databaseId}/tables");
    paths.add("/databases/{databaseId}/tables/{tableId}");
    paths.add("/v0/databases/{databaseId}/tables/{tableId}");
    paths.add("/v0/databases/{databaseId}/tables/{tableId}/iceberg/v2/snapshots");
    paths.add("/databases/{databaseId}/tables/{tableId}/iceberg/v2/snapshots");

    Map<String, String> result =
        MainApplicationConfig.generatePostfix(
            paths.stream().map(Paths::get).collect(Collectors.toSet()));
    Assertions.assertEquals(result.size(), paths.size());
    Assertions.assertEquals(result.get("/v0/databases"), "V0");
    Assertions.assertEquals(result.get("/v1/databases"), "V1");
    Assertions.assertEquals(result.get("/databases"), "");
    Assertions.assertEquals(result.get("/databases/{databaseId}/aclPolicies"), "");
    Assertions.assertEquals(result.get("/v0/databases/{databaseId}/aclPolicies"), "V0");
    Assertions.assertEquals(result.get("/databases/{databaseId}/tables/{tableId}/aclPolicies"), "");
    Assertions.assertEquals(
        result.get("/v0/databases/{databaseId}/tables/{tableId}/aclPolicies"), "V0");
    Assertions.assertEquals(result.get("/v0/databases/{databaseId}/tables"), "V0");
    Assertions.assertEquals(result.get("/databases/{databaseId}/tables"), "");
    Assertions.assertEquals(result.get("/databases/{databaseId}/tables/{tableId}"), "");
    Assertions.assertEquals(result.get("/v0/databases/{databaseId}/tables/{tableId}"), "V0");
    Assertions.assertEquals(
        result.get("/v0/databases/{databaseId}/tables/{tableId}/iceberg/v2/snapshots"), "V0");
    Assertions.assertEquals(
        result.get("/databases/{databaseId}/tables/{tableId}/iceberg/v2/snapshots"), "");
  }

  @Test
  public void testVersionRegex() {
    Assertions.assertTrue(MainApplicationConfig.isVersion("v9"));
    Assertions.assertTrue(MainApplicationConfig.isVersion("v45"));
    // temporarily don't support minor version. This case should fail when that support is in place
    Assertions.assertFalse(MainApplicationConfig.isVersion("v9.0"));
    Assertions.assertFalse(MainApplicationConfig.isVersion("views"));
  }

  @Test
  public void testAssumptionOfDefaultOperationId() {
    Assertions.assertDoesNotThrow(() -> MainApplicationConfig.sanitizeOpId("getTable_1"));
    Assertions.assertThrows(
        IllegalStateException.class, () -> MainApplicationConfig.sanitizeOpId("_getTable"));
    Assertions.assertThrows(
        IllegalStateException.class, () -> MainApplicationConfig.sanitizeOpId("get_Table_1"));
  }
}
