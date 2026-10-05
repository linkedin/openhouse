package com.linkedin.openhouse.jobs.spark.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.client.ssl.TablesApiClientFactory;
import com.linkedin.openhouse.jobs.spark.replication.ReferenceReplicationDataPlane.CopyResult;
import com.linkedin.openhouse.jobs.spark.replication.ReferenceReplicationDataPlane.TableGeneration;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class ReferenceReplicationDataPlaneTest extends OpenHouseSparkITest {
  private static final String CATALOG_NAME = "openhouse";
  private static final String DATABASE = "db";

  @Test
  public void replicatesInterleavedWritesAndFollowsCatalogRename() throws Exception {
    SparkSession spark = getSparkSession();
    Catalog catalog = getOpenHouseCatalog(spark);
    ApiClient apiClient =
        TablesApiClientFactory.getInstance()
            .createApiClient(
                getOpenHouseLocalServerURI().toString(),
                spark.conf().get("spark.sql.catalog.openhouse.auth-token"),
                null);
    TableApi tableApi = new TableApi(apiClient);
    ReferenceReplicationDataPlane dataPlane =
        new ReferenceReplicationDataPlane(
            spark,
            (catalogName, identifier) ->
                tableApi
                    .getTableV1(identifier.namespace().level(0), identifier.name())
                    .block(Duration.ofSeconds(30)));

    String suffix = UUID.randomUUID().toString().replace("-", "");
    TableIdentifier source = TableIdentifier.of(DATABASE, "replication_source_" + suffix);
    TableIdentifier renamedSource =
        TableIdentifier.of(DATABASE, "replication_source_renamed_" + suffix);
    TableIdentifier destination = TableIdentifier.of(DATABASE, "replication_destination_" + suffix);
    TableIdentifier renamedDestination =
        TableIdentifier.of(DATABASE, "replication_destination_renamed_" + suffix);
    String sourceSql = sqlIdentifier(source);
    String destinationSql = sqlIdentifier(destination);

    try {
      spark.sql("CREATE TABLE " + sourceSql + " (id BIGINT, value STRING) USING iceberg");
      spark.sql(
          "CREATE TABLE "
              + destinationSql
              + " (id BIGINT, value STRING) USING iceberg "
              + "TBLPROPERTIES ('openhouse.tableType' = 'REPLICA_TABLE')");

      TableGeneration sourceGeneration = dataPlane.getGeneration(CATALOG_NAME, source);
      TableGeneration destinationGeneration = dataPlane.getGeneration(CATALOG_NAME, destination);
      assertEquals(
          List.of(source),
          dataPlane.findTableByGeneration(CATALOG_NAME, sourceGeneration).stream()
              .collect(Collectors.toList()));

      insertAndReplicate(
          spark,
          dataPlane,
          source,
          destination,
          sourceGeneration,
          destinationGeneration,
          1L,
          "one");
      insertAndReplicate(
          spark,
          dataPlane,
          source,
          destination,
          sourceGeneration,
          destinationGeneration,
          2L,
          "two");
      insertAndReplicate(
          spark,
          dataPlane,
          source,
          destination,
          sourceGeneration,
          destinationGeneration,
          3L,
          "three");

      catalog.renameTable(source, renamedSource);
      assertTrue(catalog.tableExists(renamedSource));
      assertFalse(catalog.tableExists(source));
      assertEquals(
          renamedSource,
          dataPlane.findTableByGeneration(CATALOG_NAME, sourceGeneration).orElseThrow());

      spark.sql("INSERT INTO " + sqlIdentifier(renamedSource) + " VALUES (4, 'four')");
      CopyResult result =
          dataPlane.copyLatestSnapshot(
              CATALOG_NAME,
              renamedSource,
              CATALOG_NAME,
              destination,
              sourceGeneration,
              destinationGeneration);
      assertTrue(result.getSourceSnapshotId() > 0);
      assertTrue(result.getDestinationSnapshotId() > 0);
      assertEquals(List.of(1L, 2L, 3L, 4L), ids(spark, destination));

      dataPlane.renameReplica(CATALOG_NAME, destination, renamedDestination, destinationGeneration);
      assertTrue(catalog.tableExists(renamedDestination));
      assertFalse(catalog.tableExists(destination));
      assertEquals(
          destinationGeneration, dataPlane.getGeneration(CATALOG_NAME, renamedDestination));
      assertEquals(List.of(1L, 2L, 3L, 4L), ids(spark, renamedDestination));
    } finally {
      dropIfExists(catalog, source);
      dropIfExists(catalog, renamedSource);
      dropIfExists(catalog, destination);
      dropIfExists(catalog, renamedDestination);
    }
  }

  private static void insertAndReplicate(
      SparkSession spark,
      ReferenceReplicationDataPlane dataPlane,
      TableIdentifier source,
      TableIdentifier destination,
      TableGeneration sourceGeneration,
      TableGeneration destinationGeneration,
      long id,
      String value) {
    spark.sql("INSERT INTO " + sqlIdentifier(source) + " VALUES (" + id + ", '" + value + "')");
    CopyResult result =
        dataPlane.copyLatestSnapshot(
            CATALOG_NAME,
            source,
            CATALOG_NAME,
            destination,
            sourceGeneration,
            destinationGeneration);
    assertTrue(result.getSourceSnapshotId() > 0);
    assertTrue(result.getDestinationSnapshotId() > 0);
    assertEquals(id, ids(spark, destination).size());
  }

  private static List<Long> ids(SparkSession spark, TableIdentifier identifier) {
    return spark.sql("SELECT id FROM " + sqlIdentifier(identifier) + " ORDER BY id").collectAsList()
        .stream()
        .map(row -> row.getLong(0))
        .collect(Collectors.toList());
  }

  private static String sqlIdentifier(TableIdentifier identifier) {
    return "`"
        + CATALOG_NAME
        + "`.`"
        + identifier.namespace().level(0)
        + "`.`"
        + identifier.name()
        + "`";
  }

  private static void dropIfExists(Catalog catalog, TableIdentifier identifier) {
    if (catalog.tableExists(identifier)) {
      catalog.dropTable(identifier, true);
    }
  }
}
