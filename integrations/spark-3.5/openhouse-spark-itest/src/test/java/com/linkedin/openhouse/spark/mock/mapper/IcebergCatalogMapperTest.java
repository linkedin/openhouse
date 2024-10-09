package com.linkedin.openhouse.spark.mock.mapper;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.spark.sql.execution.datasources.v2.mapper.IcebergCatalogMapper;
import java.nio.file.Files;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IcebergCatalogMapperTest {
  private static SparkSession spark = null;

  public void testSparkCatalog() {
    SparkCatalog sparkCatalog = new SparkCatalog();
    sparkCatalog.initialize(
        "oh_spark_catalog",
        new CaseInsensitiveStringMap(
            ImmutableMap.of("type", "hadoop", "warehouse", createTempDirForCatalog())));

    Catalog icebergCatalog = IcebergCatalogMapper.toIcebergCatalog(sparkCatalog);
    assert icebergCatalog != null;
  }

  public void testSparkCatalogCacheDisabled() {
    SparkCatalog sparkCatalog = new SparkCatalog();
    sparkCatalog.initialize(
        "oh_spark_catalog_cache_disabled",
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                "type",
                "hadoop",
                "warehouse",
                createTempDirForCatalog(),
                "cache-enabled",
                "false")));

    Catalog icebergCatalog = IcebergCatalogMapper.toIcebergCatalog(sparkCatalog);
    assert icebergCatalog != null;
  }

  public void testSparkSessionCatalog() {
    SparkSessionCatalog sparkCatalog = new SparkSessionCatalog();
    sparkCatalog.initialize(
        "oh_spark_session_catalog",
        new CaseInsensitiveStringMap(
            ImmutableMap.of("type", "hadoop", "warehouse", createTempDirForCatalog())));

    Catalog icebergCatalog = IcebergCatalogMapper.toIcebergCatalog(sparkCatalog);
    assert icebergCatalog != null;
  }

  public void testSparkSessionCatalogCacheDisabled() {
    SparkSessionCatalog sparkCatalog = new SparkSessionCatalog();
    sparkCatalog.initialize(
        "oh_spark_session_catalog_cache_disabled",
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                "type",
                "hadoop",
                "warehouse",
                createTempDirForCatalog(),
                "cache-enabled",
                "false")));

    Catalog icebergCatalog = IcebergCatalogMapper.toIcebergCatalog(sparkCatalog);
    assert icebergCatalog != null;
  }

  public void testNonIcebergCatalog() {
    TableCatalog fakeTableCatalog = Mockito.mock(TableCatalog.class);
    assert IcebergCatalogMapper.toIcebergCatalog(fakeTableCatalog) == null;
  }

  @SneakyThrows
  private String createTempDirForCatalog() {
    return Files.createTempDirectory("unittest" + UUID.randomUUID()).toString();
  }

  @SneakyThrows
  @BeforeAll
  public void setupSpark() {
    spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterAll
  public void tearDownSpark() {
    spark.close();
  }
}
