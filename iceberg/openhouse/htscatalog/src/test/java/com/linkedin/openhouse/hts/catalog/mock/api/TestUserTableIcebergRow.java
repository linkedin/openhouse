package com.linkedin.openhouse.hts.catalog.mock.api;

import com.linkedin.openhouse.hts.catalog.api.IcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.model.usertable.UserTableIcebergRow;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestUserTableIcebergRow {

  static UserTableIcebergRow userTableIcebergRow;

  @BeforeAll
  static void setup() {
    userTableIcebergRow =
        UserTableIcebergRow.builder()
            .databaseId("db1")
            .tableId("tb1")
            .version("v1")
            .metadataLocation("file:/ml1")
            .build();
  }

  @Test
  void testVersioning() {
    Assertions.assertEquals(userTableIcebergRow.getVersionColumnName(), "version");
    Assertions.assertDoesNotThrow(() -> Integer.parseInt(userTableIcebergRow.getNextVersion()));
    Assertions.assertEquals(userTableIcebergRow.getCurrentVersion(), "v1");
  }

  @Test
  void testSchema() {
    Assertions.assertTrue(
        userTableIcebergRow.getSchema().columns().stream()
            .map(Types.NestedField::name)
            .collect(Collectors.toList())
            .containsAll(Arrays.asList("databaseId", "tableId")));
  }

  @Test
  void testRecord() {
    Assertions.assertEquals(userTableIcebergRow.getRecord().getField("databaseId"), "db1");
    Assertions.assertEquals(userTableIcebergRow.getRecord().getField("tableId"), "tb1");
    Assertions.assertEquals(userTableIcebergRow.getRecord().getField("version"), "v1");
    Assertions.assertEquals(
        userTableIcebergRow.getRecord().getField("metadataLocation"), "file:/ml1");
  }

  @Test
  void testToPrimaryKey() {
    IcebergRowPrimaryKey irpk = userTableIcebergRow.getIcebergRowPrimaryKey();
    Assertions.assertEquals(irpk.getRecord().getField("databaseId"), "db1");
    Assertions.assertEquals(irpk.getRecord().getField("tableId"), "tb1");
  }
}
