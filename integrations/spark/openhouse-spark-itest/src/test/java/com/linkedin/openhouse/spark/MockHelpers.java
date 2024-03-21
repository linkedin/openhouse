package com.linkedin.openhouse.spark;

import static com.linkedin.openhouse.spark.SparkTestBase.*;
import static org.apache.iceberg.CatalogUtil.*;

import com.linkedin.openhouse.gen.tables.client.api.SnapshotApi;
import com.linkedin.openhouse.gen.tables.client.api.TableApi;
import com.linkedin.openhouse.gen.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.gen.tables.client.model.AclPolicy;
import com.linkedin.openhouse.gen.tables.client.model.ClusteringColumn;
import com.linkedin.openhouse.gen.tables.client.model.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.gen.tables.client.model.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.gen.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.gen.tables.client.model.GetDatabaseResponseBody;
import com.linkedin.openhouse.gen.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.gen.tables.client.model.TimePartitionSpec;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import okhttp3.mockwebserver.MockResponse;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class MockHelpers {

  private static final ObjectMapper mapper =
      ApiClient.createDefaultObjectMapper(ApiClient.createDefaultDateFormat());

  /**
   * Helper method to create {@link GetAllDatabasesResponseBody} from list of {@link
   * GetDatabaseResponseBody}
   */
  public static GetAllDatabasesResponseBody mockGetAllDatabasesResponseBody(
      GetDatabaseResponseBody... drs) {
    Map<String, Object> hashmap = new HashMap<>();
    hashmap.put("results", Arrays.asList(drs));
    return mapper.convertValue(hashmap, GetAllDatabasesResponseBody.class);
  }

  /** Helper method to create {@link GetDatabaseResponseBody} from table required fields. */
  public static GetDatabaseResponseBody mockGetDatabaseResponseBody(
      String databaseId, String clusterId) {
    Map<String, Object> hashmap = new HashMap<>();
    hashmap.put("databaseId", databaseId);
    hashmap.put("clusterId", clusterId);
    return mapper.convertValue(hashmap, GetDatabaseResponseBody.class);
  }

  /**
   * Helper method to create {@link GetAllTablesResponseBody} from list of {@link
   * GetTableResponseBody}
   */
  public static GetAllTablesResponseBody mockGetAllTableResponseBody(GetTableResponseBody... trs) {
    Map<String, Object> hashmap = new HashMap<>();
    hashmap.put("results", Arrays.asList(trs));
    return mapper.convertValue(hashmap, GetAllTablesResponseBody.class);
  }

  /** Helper method to create {@link GetTableResponseBody} from table required fields. */
  public static GetTableResponseBody mockGetTableResponseBody(
      String databaseId,
      String tableId,
      String clusterId,
      String tableUri,
      String tableUUID,
      String tableLocation,
      String tableVersion,
      String schema,
      TimePartitionSpec timePartitionSpec,
      List<ClusteringColumn> clustering) {
    Map<String, Object> hashmap = new HashMap<>();
    hashmap.put("databaseId", databaseId);
    hashmap.put("tableId", tableId);
    hashmap.put("clusterId", clusterId);
    hashmap.put("tableUri", tableUri);
    hashmap.put("tableUUID", tableUUID);
    hashmap.put("tableLocation", tableLocation);
    hashmap.put("tableVersion", tableVersion);
    hashmap.put("schema", schema);
    hashmap.put("timePartitioning", timePartitionSpec);
    hashmap.put("clustering", clustering);
    hashmap.put("policies", null);
    return mapper.convertValue(hashmap, GetTableResponseBody.class);
  }

  /** Helper method to create {@link GetTableResponseBody} from table optional fields. */
  @SneakyThrows
  public static GetTableResponseBody decorateResponse(
      GetTableResponseBody getTableResponseBody, Map<String, String> tblProps) {
    JsonNode jsonNode = mapper.valueToTree(getTableResponseBody);
    ((ObjectNode) jsonNode).put("tableProperties", mapper.convertValue(tblProps, ObjectNode.class));
    return mapper.treeToValue(jsonNode, GetTableResponseBody.class);
  }

  public static GetAclPoliciesResponseBody mockGetAclPoliciesResponseBody(AclPolicy... aclPolicy) {
    Map<String, Object> hashmap = new HashMap<>();
    hashmap.put("results", Arrays.asList(aclPolicy));
    return mapper.convertValue(hashmap, GetAclPoliciesResponseBody.class);
  }

  public static AclPolicy mockAclPolicy(String role, String principal) {
    Map<String, Object> hashmap = new HashMap<>();
    hashmap.put("role", role);
    hashmap.put("principal", principal);
    return mapper.convertValue(hashmap, AclPolicy.class);
  }

  /** Helper method to create {@link MockResponse} that plugs in nicely to mockWebServer. */
  @SneakyThrows
  public static MockResponse mockResponse(int status, Object jsonObj) {
    ;
    return new MockResponse()
        .setResponseCode(status)
        .setBody(mapper.writeValueAsString(jsonObj))
        .addHeader("Content-Type", "application/json");
  }

  /**
   * Helper method to get a valid metadata.json path. Provides an option to also insert data into
   * that table.
   *
   * @param tableIdentifier
   * @param createData set to true if data needs to be inserted
   * @return the metadata_json path for the table
   */
  @SneakyThrows
  public static String mockTableLocationDefaultSchema(
      TableIdentifier tableIdentifier, Boolean createData) {
    String tableName =
        String.format(
            "testhelper.%s.%s", tableIdentifier.namespace().toString(), tableIdentifier.name());
    spark.sql(
        String.format(
            "CREATE OR REPLACE TABLE  %s (col1 string, col2 string) USING iceberg", tableName));
    if (createData) {
      spark.sql(String.format("INSERT INTO %s VALUES ('1', 'a'), ('2', 'b')", tableName));
    }
    return craftMetadataLocation(tableIdentifier, "testhelper");
  }

  /**
   * Helper method to get a valid metadata.json path after running an SQL operation.
   *
   * @param tableIdentifier
   * @param sql sql should have %t as the table identifier, for example: "insert into %t values.."
   * @return the metadata_json path for the table after the operation
   */
  @SneakyThrows
  public static String mockTableLocationAfterOperation(
      TableIdentifier tableIdentifier, String sql) {
    String tableName =
        String.format(
            "testhelper.%s.%s", tableIdentifier.namespace().toString(), tableIdentifier.name());
    sql = sql.replace("%t", tableName);
    spark.sql(sql);
    return craftMetadataLocation(tableIdentifier, "testhelper");
  }

  /**
   * Helper method to get a valid metadata.json path. Compare to the method
   * com.linkedin.openhouse.spark.MockHelpers#mockTableLocationDefaultSchema(org.apache.iceberg.catalog.TableIdentifier,
   * java.lang.Boolean) this method doesn't provide option to load data but provide API to specify
   * schema or specify partitionedByString.
   *
   * @param tableIdentifier
   * @param ddlSchema schema of the mocking table.
   * @return the metadata_json path for the table
   */
  @SneakyThrows
  public static String mockTableLocation(
      TableIdentifier tableIdentifier, String ddlSchema, String partitionedByString) {
    String tableName =
        String.format(
            "testhelper.%s.%s", tableIdentifier.namespace().toString(), tableIdentifier.name());
    spark.sql(
        String.format(
            "CREATE TABLE %s (%s) USING iceberg %s", tableName, ddlSchema, partitionedByString));
    return craftMetadataLocation(tableIdentifier, "testhelper");
  }

  public static DataFile createDummyDataFile(String dataPath, PartitionSpec partitionSpec)
      throws IOException {
    Files.write(Paths.get(dataPath), Lists.newArrayList(), StandardCharsets.UTF_8);
    return DataFiles.builder(partitionSpec)
        .withPath(dataPath)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  public static Snapshot mockDummySnapshot(
      TableIdentifier tableIdentifier,
      String dataPath,
      PartitionSpec partitionSpec,
      String catalogName)
      throws IOException {
    Catalog catalog =
        CatalogUtil.buildIcebergCatalog(
            catalogName,
            ImmutableMap.of(
                CatalogProperties.WAREHOUSE_LOCATION,
                spark.conf().get("spark.sql.catalog.testhelper.warehouse"),
                ICEBERG_CATALOG_TYPE,
                "hadoop"),
            new Configuration());
    Table table = catalog.loadTable(tableIdentifier);
    return table.newAppend().appendFile(createDummyDataFile(dataPath, partitionSpec)).apply();
  }

  private static String craftMetadataLocation(TableIdentifier tableIdentifier, String catalogName)
      throws IOException {
    Catalog catalog =
        CatalogUtil.buildIcebergCatalog(
            catalogName,
            ImmutableMap.of(
                CatalogProperties.WAREHOUSE_LOCATION,
                spark.conf().get("spark.sql.catalog.testhelper.warehouse"),
                ICEBERG_CATALOG_TYPE,
                "hadoop"),
            new Configuration());
    Table table = catalog.loadTable(tableIdentifier);
    Path metadataLocation =
        Paths.get(((BaseTable) table).operations().current().metadataFileLocation());
    // HadoopCatalog created metadata file has name format v1.metadata.json, which is not compatible
    // with BaseMetastoreTableOperations
    return Files.copy(
            metadataLocation,
            metadataLocation.resolveSibling(
                new Random().nextInt(Integer.MAX_VALUE) + "-.metadata.json"))
        .toString();
  }

  public static String mockTableLocationDefaultSchema(TableIdentifier tableIdentifier) {
    return mockTableLocationDefaultSchema(tableIdentifier, false);
  }

  /**
   * This method converts schema in Iceberg literal format to something that SQL DDL can incorporate
   * as part of it. Functionality limitation: IT DOESN'T SUPPORT nested schema.
   */
  public static String convertSchemaToDDLComponent(String icebergSchema) {
    return SchemaParser.fromJson(icebergSchema).columns().stream()
        .map(x -> x.name() + " " + x.type().toString())
        .collect(Collectors.joining(", "));
  }

  /**
   * This method converts each top-level field in Iceberg literal format to an array of
   * {<FIELD_NAME>, <FIELD_TYPE>, ""} in which the third element is supposed to be document. It is
   * being placed with empty string for simplicity of tests. Functionality limitation: IT DOESN'T
   * SUPPORT nested schema.
   *
   * <p>Spark SQL shows "long" as a "bigint", but their semantics are same,
   *
   * @see <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html">Spark SQL
   *     datatypes</a>
   */
  public static List<String[]> convertSchemaToFieldArray(String icebergSchema) {
    return SchemaParser.fromJson(icebergSchema).columns().stream()
        .map(
            x ->
                x.type().toString().equals("long")
                    ? Arrays.asList(x.name(), "bigint", "").toArray(new String[3])
                    : Arrays.asList(x.name(), x.type().toString(), "").toArray(new String[3]))
        .collect(Collectors.toList());
  }

  /**
   * Check if the given field is a timestamp type.
   *
   * <p>returns empty optional if type is not timestamp.
   */
  private static Optional<String> getTimestampType(Schema.Field f) {
    if (f.schema().getLogicalType() != null
        && f.schema().getLogicalType().getName().contains("timestamp")) {
      return Optional.of("timestamp");
    } else {
      return Optional.empty();
    }
  }

  /** Helper method to get ApiClient for the running mockWebServer Instance. */
  public static TableApi getTableApiClient() {
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(
        String.format("http://%s:%s", mockTableService.getHostName(), mockTableService.getPort()));
    return new TableApi(apiClient);
  }

  /** Helper method to get ApiClient for the running mockWebServer Instance. */
  public static SnapshotApi getSnapshotApiClient() {
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(
        String.format("http://%s:%s", mockTableService.getHostName(), mockTableService.getPort()));
    return new SnapshotApi(apiClient);
  }
}
