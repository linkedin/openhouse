package com.linkedin.openhouse.jobs.clients;

import static org.mockito.ArgumentMatchers.*;

import com.linkedin.openhouse.cluster.storage.filesystem.ParameterizedHdfsStorageProvider;
import com.linkedin.openhouse.jobs.client.StorageClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.TablesClientFactory;
import com.linkedin.openhouse.jobs.util.DatabaseTableFilter;
import com.linkedin.openhouse.jobs.util.DirectoryMetadata;
import com.linkedin.openhouse.jobs.util.RetentionConfig;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.tables.client.api.DatabaseApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.model.ClusteringColumn;
import com.linkedin.openhouse.tables.client.model.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetDatabaseResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.Policies;
import com.linkedin.openhouse.tables.client.model.Retention;
import com.linkedin.openhouse.tables.client.model.RetentionColumnPattern;
import com.linkedin.openhouse.tables.client.model.TimePartitionSpec;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
public class TablesClientTest {
  private final String testDbName = "test_db_name";
  private final String testTableName = "test_table_name";
  private final String testTableUUID = "1234";
  private final String testTableCreator = "test_table_creator";
  private final String testOrphanDirectoryName = "test_orphan_directory_name";
  private final String testTableNamePartitioned = "test_table_name_partitioned";
  private final String testTableNameOlder = "test_table_name_older";
  private final String testTableNameClustered = "test_table_name_clustered";
  private final String testPartitionColumnName = "test_partition_column_name";
  private final String testReplicaTableName = "test_replica_table_name";
  private final String testPatternColumnName = "test-pattern-columns";
  private final String testPattern = "test-pattern-name";
  private final int testRetentionTTLDays = 365;
  private final Retention.GranularityEnum granularity = Retention.GranularityEnum.DAY;
  private final TablesClientFactory clientFactory =
      new TablesClientFactory(
          "base_path",
          DatabaseTableFilter.of(".*", ".*", 0),
          null,
          ParameterizedHdfsStorageProvider.of("hadoop", "hdfs://localhost/", "/jobs/openhouse/"));
  private TableApi apiMock;
  private TablesClient client;
  private DatabaseApi dbApiMock;
  private StorageClient storageClient;

  @BeforeEach
  void setup() {
    apiMock = Mockito.mock(TableApi.class);
    dbApiMock = Mockito.mock(DatabaseApi.class);
    storageClient = Mockito.mock(StorageClient.class);
    RetryPolicy retryPolicy = new NeverRetryPolicy();

    client =
        clientFactory.create(
            RetryTemplate.builder().customPolicy(retryPolicy).build(),
            apiMock,
            dbApiMock,
            storageClient);
  }

  @Test
  void testGetTableMetadataList() {
    // non-partitioned table mocks setup
    GetTableResponseBody nonPartitionedTableResponseBodyMock =
        createNonPartitionedTableResponseBodyMock(testDbName, testTableName);
    Mono<GetTableResponseBody> nonPartitionedTableResponseMock =
        (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(nonPartitionedTableResponseMock.block(any(Duration.class)))
        .thenReturn(nonPartitionedTableResponseBodyMock);
    Mockito.when(apiMock.getTableV1(testDbName, testTableName))
        .thenReturn(nonPartitionedTableResponseMock);

    // partitioned table mocks setup
    GetTableResponseBody partitionedTableResponseBodyMock =
        createPartitionedTableResponseBodyMock(
            testDbName, testTableNamePartitioned, testPartitionColumnName, testRetentionTTLDays);
    Mono<GetTableResponseBody> partitionedTableResponseMock =
        (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(partitionedTableResponseMock.block(any(Duration.class)))
        .thenReturn(partitionedTableResponseBodyMock);
    Mockito.when(apiMock.getTableV1(testDbName, testTableNamePartitioned))
        .thenReturn(partitionedTableResponseMock);

    // clustered table mocks setup
    GetTableResponseBody clusteredTableResponseBodyMock =
        createNonPartitionedTableResponseBodyMock(testDbName, testTableNameClustered, true);
    Mono<GetTableResponseBody> clusteredTableResponseMock =
        (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(clusteredTableResponseMock.block(any(Duration.class)))
        .thenReturn(clusteredTableResponseBodyMock);
    Mockito.when(apiMock.getTableV1(testDbName, testTableNameClustered))
        .thenReturn(clusteredTableResponseMock);

    GetTableResponseBody nonPartitionedTableIdentifierMock =
        createTableResponseBodyMock(testDbName, testTableName);
    GetTableResponseBody partitionedTableIdentifierMock =
        createTableResponseBodyMock(testDbName, testTableNamePartitioned);
    GetTableResponseBody clusteredTableIdentifierMock =
        createTableResponseBodyMock(testDbName, testTableNameClustered);

    GetAllTablesResponseBody allTablesResponseBodyMock =
        Mockito.mock(GetAllTablesResponseBody.class);
    Mockito.when(allTablesResponseBodyMock.getResults())
        .thenReturn(
            Arrays.asList(
                nonPartitionedTableIdentifierMock,
                partitionedTableIdentifierMock,
                clusteredTableIdentifierMock));

    GetAllDatabasesResponseBody allDatabasesResponseBodyMock =
        Mockito.mock(GetAllDatabasesResponseBody.class);
    GetDatabaseResponseBody databaseResponseMock = createGetDatabaseResponseBodyMock(testDbName);
    Mockito.when(allDatabasesResponseBodyMock.getResults())
        .thenReturn(Collections.singletonList(databaseResponseMock));

    Mono<GetAllTablesResponseBody> responseMock =
        (Mono<GetAllTablesResponseBody>) Mockito.mock(Mono.class);
    Mono<GetAllDatabasesResponseBody> dbResponseMock =
        (Mono<GetAllDatabasesResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class))).thenReturn(allTablesResponseBodyMock);
    Mockito.when(dbResponseMock.block(any(Duration.class)))
        .thenReturn(allDatabasesResponseBodyMock);
    Mockito.when(dbApiMock.getAllDatabasesV1()).thenReturn(dbResponseMock);
    Mockito.when(apiMock.searchTablesV1(testDbName)).thenReturn(responseMock);

    List<TableMetadata> tableMetadataList = client.getTableMetadataList();
    Assertions.assertEquals(
        Arrays.asList(
            TableMetadata.builder()
                .dbName(testDbName)
                .tableName(testTableName)
                .isPrimary(true)
                .build(),
            TableMetadata.builder()
                .dbName(testDbName)
                .tableName(testTableNamePartitioned)
                .isTimePartitioned(true)
                .isPrimary(true)
                .isClustered(true)
                .retentionConfig(
                    RetentionConfig.builder()
                        .columnName(testPartitionColumnName)
                        .columnPattern("")
                        .granularity(Retention.GranularityEnum.DAY)
                        .count(testRetentionTTLDays)
                        .build())
                .build(),
            TableMetadata.builder()
                .dbName(testDbName)
                .tableName(testTableNameClustered)
                .isTimePartitioned(false)
                .isPrimary(true)
                .isClustered(true)
                .build()),
        tableMetadataList);
    for (TableMetadata tableMetadata : tableMetadataList) {
      Assertions.assertFalse(tableMetadata.getTableName().contains(testTableNameOlder));
    }
    Mockito.verify(nonPartitionedTableResponseMock, Mockito.times(1)).block(any(Duration.class));
    Mockito.verify(partitionedTableResponseMock, Mockito.times(1)).block(any(Duration.class));
    Mockito.verify(responseMock, Mockito.times(1)).block(any(Duration.class));
    Mockito.verify(dbResponseMock, Mockito.times(1)).block(any(Duration.class));
    Mockito.verify(allTablesResponseBodyMock, Mockito.times(1)).getResults();
  }

  @Test
  void testGetTableNamesForDBName() {
    String tableLocation = testDbName + "/" + testTableName + "-" + testTableUUID;
    String tableLocationMetadata = tableLocation + "/" + testTableUUID + ".metadata.json";
    GetAllTablesResponseBody allTablesResponseBodyMock =
        Mockito.mock(GetAllTablesResponseBody.class);
    GetTableResponseBody tableResponseBodyMock =
        createTableWithLocationResponseBodyMock(testDbName, testTableName, tableLocationMetadata);
    GetTableResponseBody tableIdentifierMock =
        createTableResponseBodyMock(testDbName, testTableName);
    Mockito.when(allTablesResponseBodyMock.getResults())
        .thenReturn(Collections.singletonList(tableIdentifierMock));
    Mono<GetAllTablesResponseBody> responseMock =
        (Mono<GetAllTablesResponseBody>) Mockito.mock(Mono.class);
    Mono<GetTableResponseBody> tableResponseMock =
        (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class))).thenReturn(allTablesResponseBodyMock);
    Mockito.when(tableResponseMock.block(any(Duration.class))).thenReturn(tableResponseBodyMock);
    Mockito.when(apiMock.searchTablesV1(testDbName)).thenReturn(responseMock);
    Mockito.when(apiMock.getTableV1(testDbName, testTableName)).thenReturn(tableResponseMock);

    Assertions.assertEquals(
        Stream.of(testTableName + "-" + testTableUUID).collect(Collectors.toSet()),
        client.getTableNamesForDbName(testDbName));
  }

  @Test
  void testGetOrphanTableDirectories() throws IOException {
    String tableLocation = testDbName + "/" + testTableName + "-" + testTableUUID;
    String tableLocationMetadata = tableLocation + "/" + testTableUUID + ".metadata.json";
    GetAllTablesResponseBody allTablesResponseBodyMock =
        Mockito.mock(GetAllTablesResponseBody.class);
    GetTableResponseBody tableResponseBodyMock =
        createTableWithLocationResponseBodyMock(testDbName, testTableName, tableLocationMetadata);
    GetTableResponseBody tableIdentifierMock =
        createTableResponseBodyMock(testDbName, testTableName);
    Mockito.when(allTablesResponseBodyMock.getResults())
        .thenReturn(Collections.singletonList(tableIdentifierMock));
    Mono<GetAllTablesResponseBody> responseMock =
        (Mono<GetAllTablesResponseBody>) Mockito.mock(Mono.class);
    Mono<GetTableResponseBody> tableResponseMock =
        (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class))).thenReturn(allTablesResponseBodyMock);
    Mockito.when(tableResponseMock.block(any(Duration.class))).thenReturn(tableResponseBodyMock);
    Mockito.when(apiMock.searchTablesV1(testDbName)).thenReturn(responseMock);
    Mockito.when(apiMock.getTableV1(testDbName, testTableName)).thenReturn(tableResponseMock);

    Mockito.when(storageClient.getSubDirectoriesWithOwners(Mockito.any(Path.class)))
        .thenAnswer(
            new Answer<List<DirectoryMetadata>>() {
              @Override
              public List<DirectoryMetadata> answer(InvocationOnMock invocation) throws Throwable {
                return createTableDirectoryStatusMock(
                    testDbName, Arrays.asList(testTableName, testOrphanDirectoryName));
              }
            });

    Assertions.assertEquals(
        Collections.singletonList(
            DirectoryMetadata.builder()
                .path(new Path(testDbName + "/" + testOrphanDirectoryName + "-" + testTableUUID))
                .creator(testTableCreator)
                .build()),
        client.getOrphanTableDirectories(new Path(testDbName)));
  }

  @Test
  void testPartitionedTableGetRetentionConfig() {
    GetTableResponseBody partitionedTableResponseBodyMock =
        createPartitionedTableResponseBodyMock(
            testDbName, testTableNamePartitioned, testPartitionColumnName, testRetentionTTLDays);
    Mono<GetTableResponseBody> responseMock = (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class)))
        .thenReturn(partitionedTableResponseBodyMock);
    Mockito.when(apiMock.getTableV1(testDbName, testTableNamePartitioned)).thenReturn(responseMock);
    Optional<RetentionConfig> result =
        client.getTableRetention(
            TableMetadata.builder().dbName(testDbName).tableName(testTableNamePartitioned).build());
    Assertions.assertTrue(
        result.isPresent(), "Retention config must be present for a test partitioned table");
    Assertions.assertEquals(
        RetentionConfig.builder()
            .columnName(testPartitionColumnName)
            .count(testRetentionTTLDays)
            .granularity(granularity)
            .columnPattern("")
            .build(),
        result.orElse(null));
    Mockito.verify(responseMock, Mockito.times(1)).block(any(Duration.class));
    Mockito.verify(apiMock, Mockito.times(1)).getTableV1(testDbName, testTableNamePartitioned);
  }

  @Test
  void testPartitionedTableGetNullRetentionConfig() {
    GetTableResponseBody partitionedTableResponseBodyMock =
        createPartitionedTableNullRetentionResponseBodyMock(
            testDbName, testTableNamePartitioned, testPartitionColumnName);
    Mono<GetTableResponseBody> responseMock = (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class)))
        .thenReturn(partitionedTableResponseBodyMock);
    Mockito.when(apiMock.getTableV1(testDbName, testTableNamePartitioned)).thenReturn(responseMock);
    Optional<RetentionConfig> result =
        client.getTableRetention(
            TableMetadata.builder().dbName(testDbName).tableName(testTableNamePartitioned).build());
    Assertions.assertFalse(result.isPresent());
    Mockito.verify(responseMock, Mockito.times(1)).block(any(Duration.class));
    Mockito.verify(apiMock, Mockito.times(1)).getTableV1(testDbName, testTableNamePartitioned);
  }

  @Test
  void testPartitionedTableNullPoliciesGetRetentionConfig() {
    GetTableResponseBody partitionedTableResponseBodyMock =
        createPartitionedTableNullPoliciesResponseBodyMock(
            testDbName, testTableNamePartitioned, testPartitionColumnName);
    Mono<GetTableResponseBody> responseMock = (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class)))
        .thenReturn(partitionedTableResponseBodyMock);

    Mockito.when(apiMock.getTableV1(testDbName, testTableName)).thenReturn(responseMock);
    Optional<RetentionConfig> result =
        client.getTableRetention(
            TableMetadata.builder().dbName(testDbName).tableName(testTableName).build());
    Assertions.assertFalse(
        result.isPresent(),
        "Retention config must not be present for a test partitioned "
            + "table with null policies");
    Mockito.verify(responseMock, Mockito.times(1)).block(any(Duration.class));
    Mockito.verify(apiMock, Mockito.times(1)).getTableV1(testDbName, testTableName);
  }

  @Test
  void testNonPartitionedTableGetRetentionConfig() {
    GetTableResponseBody tableResponseBodyMock =
        createNonPartitionedTableResponseBodyMock(testDbName, testTableName);
    Mono<GetTableResponseBody> responseMock = (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class))).thenReturn(tableResponseBodyMock);
    Mockito.when(apiMock.getTableV1(testDbName, testTableName)).thenReturn(responseMock);
    Optional<RetentionConfig> result =
        client.getTableRetention(
            TableMetadata.builder().dbName(testDbName).tableName(testTableName).build());
    Assertions.assertFalse(
        result.isPresent(), "Retention config must not be present for a test unpartitioned table");
    Mockito.verify(responseMock, Mockito.times(1)).block(any(Duration.class));
    Mockito.verify(apiMock, Mockito.times(1)).getTableV1(testDbName, testTableName);
  }

  @Test
  void testNonPartitionedTableWithoutPatternGetRetentionConfig() {
    GetTableResponseBody tableResponseBodyMock =
        createNonPartitionedTableWithOutPatternResponseBodyMock(
            testDbName, testTableName, testRetentionTTLDays);
    Mono<GetTableResponseBody> responseMock = (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class))).thenReturn(tableResponseBodyMock);
    Mockito.when(apiMock.getTableV1(testDbName, testTableName)).thenReturn(responseMock);
    Optional<RetentionConfig> result =
        client.getTableRetention(
            TableMetadata.builder().dbName(testDbName).tableName(testTableName).build());
    Assertions.assertFalse(
        result.isPresent(),
        "Retention config must not be present for a test unpartitioned table without "
            + "retention columnPattern");
    Mockito.verify(responseMock, Mockito.times(1)).block(any(Duration.class));
    Mockito.verify(apiMock, Mockito.times(1)).getTableV1(testDbName, testTableName);
  }

  @Test
  void testPartitionedTableWithPatternGetRetentionConfig() {
    GetTableResponseBody partitionedTableResponseBodyMock =
        createPartitionedTableWithPatternResponseBodyMock(
            testDbName,
            testTableNamePartitioned,
            testPartitionColumnName,
            testRetentionTTLDays,
            testPattern,
            testPatternColumnName);
    Mono<GetTableResponseBody> responseMock = (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class)))
        .thenReturn(partitionedTableResponseBodyMock);
    Mockito.when(apiMock.getTableV1(testDbName, testTableNamePartitioned)).thenReturn(responseMock);
    Optional<RetentionConfig> result =
        client.getTableRetention(
            TableMetadata.builder().dbName(testDbName).tableName(testTableNamePartitioned).build());
    Assertions.assertTrue(
        result.isPresent(), "Retention config must be present for a test partitioned table");
    // TimePartitionSpec!=null and Retention.ColumPattern!=null, hence TimePartitionSpec is
    // prioritized
    // to create a RetentionConfig
    Assertions.assertEquals(
        RetentionConfig.builder()
            .columnName(testPartitionColumnName)
            .count(testRetentionTTLDays)
            .granularity(granularity)
            .columnPattern("")
            .build(),
        result.orElse(null));
    Mockito.verify(responseMock, Mockito.times(1)).block(any(Duration.class));
    Mockito.verify(apiMock, Mockito.times(1)).getTableV1(testDbName, testTableNamePartitioned);
  }

  @Test
  void testNonPartitionedTableWithPatternGetRetentionConfig() {
    GetTableResponseBody partitionedTableResponseBodyMock =
        createNonPartitionedTableWithPatternResponseBodyMock(
            testDbName, testTableName, testRetentionTTLDays, testPattern, testPatternColumnName);
    Mono<GetTableResponseBody> responseMock = (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class)))
        .thenReturn(partitionedTableResponseBodyMock);
    Mockito.when(apiMock.getTableV1(testDbName, testTableNamePartitioned)).thenReturn(responseMock);
    Optional<RetentionConfig> result =
        client.getTableRetention(
            TableMetadata.builder().dbName(testDbName).tableName(testTableNamePartitioned).build());
    Assertions.assertTrue(
        result.isPresent(), "Retention config must be present for a test partitioned table");
    Assertions.assertEquals(
        RetentionConfig.builder()
            .columnName(testPatternColumnName)
            .count(testRetentionTTLDays)
            .granularity(granularity)
            .columnPattern(testPattern)
            .build(),
        result.orElse(null));
    Mockito.verify(responseMock, Mockito.times(1)).block(any(Duration.class));
    Mockito.verify(apiMock, Mockito.times(1)).getTableV1(testDbName, testTableNamePartitioned);
  }

  @Test
  void getDatabases() {
    GetAllDatabasesResponseBody allDatabasesResponseBodyMock =
        Mockito.mock(GetAllDatabasesResponseBody.class);
    GetDatabaseResponseBody databaseResponseMock = createGetDatabaseResponseBodyMock("db");
    Mockito.when(allDatabasesResponseBodyMock.getResults())
        .thenReturn(Arrays.asList(databaseResponseMock));
    Mono<GetAllDatabasesResponseBody> dbResponseMock =
        (Mono<GetAllDatabasesResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(dbResponseMock.block(any(Duration.class)))
        .thenReturn(allDatabasesResponseBodyMock);
    Mockito.when(dbApiMock.getAllDatabasesV1()).thenReturn(dbResponseMock);
    Assertions.assertEquals(Collections.singletonList("db"), client.getDatabases());
    Mockito.verify(dbResponseMock, Mockito.times(1)).block(any(Duration.class));
  }

  @Test
  void getDatabasesWithEmptyResponse() {
    GetAllDatabasesResponseBody allDatabasesResponseBodyMock =
        Mockito.mock(GetAllDatabasesResponseBody.class);
    Mockito.when(allDatabasesResponseBodyMock.getResults()).thenReturn(Collections.emptyList());
    Mono<GetAllDatabasesResponseBody> dbResponseMock =
        (Mono<GetAllDatabasesResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(dbResponseMock.block(any(Duration.class)))
        .thenReturn(allDatabasesResponseBodyMock);
    Mockito.when(dbApiMock.getAllDatabasesV1()).thenReturn(dbResponseMock);
    Assertions.assertEquals(client.getDatabases().size(), 0);
    Mockito.verify(dbResponseMock, Mockito.times(1)).block(any(Duration.class));
  }

  private GetTableResponseBody createTableResponseBodyMock(String dbName, String tableName) {
    GetTableResponseBody responseBody = Mockito.mock(GetTableResponseBody.class);
    Mockito.when(responseBody.getTableId()).thenReturn(tableName);
    Mockito.when(responseBody.getDatabaseId()).thenReturn(dbName);
    return responseBody;
  }

  private GetTableResponseBody createNonPartitionedTableResponseBodyMock(
      String dbName, String tableName, boolean hasClustering) {
    GetTableResponseBody responseBody = Mockito.mock(GetTableResponseBody.class);
    Mockito.when(responseBody.getTableId()).thenReturn(tableName);
    Mockito.when(responseBody.getDatabaseId()).thenReturn(dbName);
    Mockito.when(responseBody.getTimePartitioning()).thenReturn(null);
    Mockito.when(responseBody.getTableType())
        .thenReturn(GetTableResponseBody.TableTypeEnum.PRIMARY_TABLE);
    if (hasClustering) {
      Mockito.when(responseBody.getClustering())
          .thenReturn(Collections.singletonList(new ClusteringColumn().columnName("col")));
    } else {
      Mockito.when(responseBody.getClustering()).thenReturn(null);
    }
    return responseBody;
  }

  private GetTableResponseBody createNonPartitionedTableResponseBodyMock(
      String dbName, String tableName) {
    return createNonPartitionedTableResponseBodyMock(dbName, tableName, false);
  }

  private GetTableResponseBody createTableWithLocationResponseBodyMock(
      String dbName, String tableName, String tableLocation) {
    GetTableResponseBody responseBody = Mockito.mock(GetTableResponseBody.class);
    Mockito.when(responseBody.getTableLocation()).thenReturn(tableLocation);
    Mockito.when(responseBody.getTableId()).thenReturn(tableName);
    Mockito.when(responseBody.getDatabaseId()).thenReturn(dbName);
    Mockito.when(responseBody.getTableType())
        .thenReturn(GetTableResponseBody.TableTypeEnum.PRIMARY_TABLE);
    return responseBody;
  }

  private List<DirectoryMetadata> createTableDirectoryStatusMock(
      String dbName, List<String> tableNames) {
    List<DirectoryMetadata> ret = new ArrayList<>();
    for (int index = 0; index < tableNames.size(); ++index) {
      ret.add(
          DirectoryMetadata.builder()
              .path(new Path(dbName + "/" + tableNames.get(index) + "-" + testTableUUID))
              .creator(testTableCreator)
              .build());
    }
    return ret;
  }

  private GetTableResponseBody createNonPartitionedTableWithOutPatternResponseBodyMock(
      String dbName, String tableName, int ttlDays) {
    GetTableResponseBody responseBody = Mockito.mock(GetTableResponseBody.class);
    Policies policies = Mockito.mock(Policies.class);
    Retention retention = Mockito.mock(Retention.class);
    retention.setCount(ttlDays);
    Mockito.when(policies.getRetention()).thenReturn(retention);
    Mockito.when(retention.getCount()).thenReturn(ttlDays);
    Mockito.when(responseBody.getTableId()).thenReturn(tableName);
    Mockito.when(responseBody.getDatabaseId()).thenReturn(dbName);
    Mockito.when(retention.getColumnPattern()).thenReturn(null);
    Mockito.when(responseBody.getTimePartitioning()).thenReturn(null);
    return responseBody;
  }

  private GetTableResponseBody createNonPartitionedTableWithPatternResponseBodyMock(
      String dbName,
      String tableName,
      int ttlDays,
      String columnPattern,
      String columnNameForPattern) {
    Policies policies = Mockito.mock(Policies.class);
    Retention retention = Mockito.mock(Retention.class);
    RetentionColumnPattern retentionColumnPattern = Mockito.mock(RetentionColumnPattern.class);
    retention.setCount(ttlDays);
    retention.setColumnPattern(retentionColumnPattern);
    policies.setRetention(retention);
    Mockito.when(policies.getRetention()).thenReturn(retention);
    Mockito.when(retention.getCount()).thenReturn(ttlDays);
    Mockito.when(retention.getGranularity()).thenReturn(Retention.GranularityEnum.DAY);
    Mockito.when(retention.getColumnPattern()).thenReturn(retentionColumnPattern);
    Mockito.when(retentionColumnPattern.getPattern()).thenReturn(columnPattern);
    Mockito.when(retentionColumnPattern.getColumnName()).thenReturn(columnNameForPattern);
    return setUpResponseBodyMock(dbName, tableName, null, policies);
  }

  private GetTableResponseBody createPartitionedTableNullPoliciesResponseBodyMock(
      String dbName, String tableName, String partitionColummName) {
    TimePartitionSpec partitionSpec = Mockito.mock(TimePartitionSpec.class);
    Mockito.when(partitionSpec.getColumnName()).thenReturn(partitionColummName);
    return setUpResponseBodyMock(dbName, tableName, partitionSpec, null);
  }

  private GetTableResponseBody createPartitionedTableNullRetentionResponseBodyMock(
      String dbName, String tableName, String partitionColummName) {
    TimePartitionSpec partitionSpec = Mockito.mock(TimePartitionSpec.class);
    Mockito.when(partitionSpec.getColumnName()).thenReturn(partitionColummName);
    Policies policies = Mockito.mock(Policies.class);
    policies.setRetention(null);
    return setUpResponseBodyMock(dbName, tableName, partitionSpec, policies);
  }

  private GetTableResponseBody createPartitionedTableResponseBodyMock(
      String dbName, String tableName, String partitionColummName, int ttlDays) {
    TimePartitionSpec partitionSpec = Mockito.mock(TimePartitionSpec.class);
    Policies policies = Mockito.mock(Policies.class);
    Retention retention = Mockito.mock(Retention.class);
    retention.setCount(testRetentionTTLDays);
    policies.setRetention(retention);
    Mockito.when(policies.getRetention()).thenReturn(retention);
    Mockito.when(retention.getCount()).thenReturn(ttlDays);
    Mockito.when(retention.getGranularity()).thenReturn(Retention.GranularityEnum.DAY);
    Mockito.when(partitionSpec.getColumnName()).thenReturn(partitionColummName);
    GetTableResponseBody responseBody =
        setUpResponseBodyMock(dbName, tableName, partitionSpec, policies);
    Mockito.when(responseBody.getTableType())
        .thenReturn(GetTableResponseBody.TableTypeEnum.PRIMARY_TABLE);
    return responseBody;
  }

  private GetTableResponseBody createTableResponseBodyMockWithCreationTime(
      String dbName, String tableName, String partitionColummName, int ttlDays, int creationTime) {
    GetTableResponseBody responseBody =
        createPartitionedTableResponseBodyMock(dbName, tableName, partitionColummName, ttlDays);
    Mockito.when(responseBody.getCreationTime())
        .thenReturn(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(creationTime));
    return responseBody;
  }

  private GetTableResponseBody createPartitionedTableWithPatternResponseBodyMock(
      String dbName,
      String tableName,
      String partitionColummName,
      int ttlDays,
      String columnPattern,
      String columnNameForPattern) {
    TimePartitionSpec partitionSpec = Mockito.mock(TimePartitionSpec.class);
    Policies policies = Mockito.mock(Policies.class);
    Retention retention = Mockito.mock(Retention.class);
    RetentionColumnPattern retentionColumnPattern = Mockito.mock(RetentionColumnPattern.class);
    retention.setCount(testRetentionTTLDays);
    retention.setColumnPattern(retentionColumnPattern);
    policies.setRetention(retention);
    Mockito.when(policies.getRetention()).thenReturn(retention);
    Mockito.when(retention.getCount()).thenReturn(ttlDays);
    Mockito.when(retention.getGranularity()).thenReturn(Retention.GranularityEnum.DAY);
    Mockito.when(retention.getColumnPattern()).thenReturn(retentionColumnPattern);
    Mockito.when(retentionColumnPattern.getPattern()).thenReturn(columnPattern);
    Mockito.when(retentionColumnPattern.getColumnName()).thenReturn(columnNameForPattern);
    Mockito.when(partitionSpec.getColumnName()).thenReturn(partitionColummName);
    return setUpResponseBodyMock(dbName, tableName, partitionSpec, policies);
  }

  private GetTableResponseBody setUpResponseBodyMock(
      String dbName, String tableName, TimePartitionSpec partitionSpec, Policies policies) {
    GetTableResponseBody responseBody = Mockito.mock(GetTableResponseBody.class);
    Mockito.when(responseBody.getTableId()).thenReturn(tableName);
    Mockito.when(responseBody.getDatabaseId()).thenReturn(dbName);
    Mockito.when(responseBody.getTimePartitioning()).thenReturn(partitionSpec);
    Mockito.when(responseBody.getPolicies()).thenReturn(policies);
    return responseBody;
  }

  GetDatabaseResponseBody createGetDatabaseResponseBodyMock(String dbName) {
    GetDatabaseResponseBody responseBody = Mockito.mock(GetDatabaseResponseBody.class);
    Mockito.when(responseBody.getDatabaseId()).thenReturn(dbName);
    return responseBody;
  }

  private GetTableResponseBody createReplicaTableResponseBodyMock(String dbName, String tableName) {
    GetTableResponseBody responseBody = Mockito.mock(GetTableResponseBody.class);
    Mockito.when(responseBody.getTableId()).thenReturn(tableName);
    Mockito.when(responseBody.getDatabaseId()).thenReturn(dbName);
    Mockito.when(responseBody.getTableType())
        .thenReturn(GetTableResponseBody.TableTypeEnum.REPLICA_TABLE);
    return responseBody;
  }

  private GetTableResponseBody createPartitionedReplicaTableResponseBodyMock(
      String dbName, String tableName, String partitionColummName, int ttlDays) {
    TimePartitionSpec partitionSpec = Mockito.mock(TimePartitionSpec.class);
    Policies policies = Mockito.mock(Policies.class);
    Retention retention = Mockito.mock(Retention.class);
    retention.setCount(testRetentionTTLDays);
    policies.setRetention(retention);
    Mockito.when(policies.getRetention()).thenReturn(retention);
    Mockito.when(retention.getCount()).thenReturn(ttlDays);
    Mockito.when(retention.getGranularity()).thenReturn(Retention.GranularityEnum.DAY);
    Mockito.when(partitionSpec.getColumnName()).thenReturn(partitionColummName);

    GetTableResponseBody responseBody =
        setUpResponseBodyMock(dbName, tableName, partitionSpec, policies);
    Mockito.when(responseBody.getTableType())
        .thenReturn(GetTableResponseBody.TableTypeEnum.REPLICA_TABLE);
    return responseBody;
  }
}
