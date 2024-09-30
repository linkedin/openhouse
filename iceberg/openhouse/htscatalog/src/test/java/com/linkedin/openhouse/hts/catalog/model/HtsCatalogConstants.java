package com.linkedin.openhouse.hts.catalog.model;

import static com.linkedin.openhouse.hts.catalog.model.HtsCatalogConstants.Helpers.*;

import com.linkedin.openhouse.hts.catalog.api.IcebergRow;
import com.linkedin.openhouse.hts.catalog.mock.model.TestIcebergRow;
import com.linkedin.openhouse.hts.catalog.mock.model.TestIcebergRowPrimaryKey;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public final class HtsCatalogConstants {
  public static final Catalog TEST_CATALOG = initializeCatalogOnRandomFilePath();
  public static final TestIcebergRow TEST_ICEBERG_ROW = ir("foo", 1, "v1", "bar");

  private HtsCatalogConstants() {}

  @SneakyThrows
  private static Catalog initializeCatalogOnRandomFilePath() {
    String random = UUID.randomUUID().toString();
    HadoopCatalog catalog = new HadoopCatalog();
    catalog.setConf(new org.apache.hadoop.conf.Configuration());
    catalog.initialize(
        random,
        Collections.singletonMap(
            CatalogProperties.WAREHOUSE_LOCATION, Files.createTempDirectory(random).toString()));
    return catalog;
  }

  public static class Helpers {
    public static TestIcebergRowPrimaryKey irpk(String stringId, Integer intId) {
      return TestIcebergRowPrimaryKey.builder().stringId(stringId).intId(intId).build();
    }

    public static TestIcebergRow ir(String stringId, Integer intId, String version, String data) {
      return TestIcebergRow.builder()
          .stringId(stringId)
          .intId(intId)
          .version(version)
          .data(data)
          .build();
    }

    public static TestIcebergRow ir(String stringId, Integer intId, String data) {
      return TestIcebergRow.builder().stringId(stringId).intId(intId).data(data).build();
    }

    public static TestIcebergRow ir(
        String stringId,
        Integer intId,
        String version,
        String data,
        List<Long> complexType1,
        TestIcebergRow.NestedStruct complexType2,
        Map<Integer, String> complexType3) {
      return TestIcebergRow.builder()
          .stringId(stringId)
          .intId(intId)
          .data(data)
          .version(version)
          .complexType1(complexType1)
          .complexType2(complexType2)
          .complexType3(complexType3)
          .build();
    }

    public static Boolean isRecordEqualWithVersionIgnored(IcebergRow left, IcebergRow right) {
      return left.getRecord()
          .copy(ImmutableMap.of(left.getVersionColumnName(), ""))
          .equals(right.getRecord().copy(ImmutableMap.of(right.getVersionColumnName(), "")));
    }
  }
}
