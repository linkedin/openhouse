package com.linkedin.openhouse.jobs.client;

import static org.mockito.ArgumentMatchers.*;

import com.linkedin.openhouse.cluster.storage.filesystem.ParameterizedHdfsStorageProvider;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.jobs.util.DatabaseTableFilter;
import com.linkedin.openhouse.jobs.util.DirectoryMetadata;
import com.linkedin.openhouse.jobs.util.HistoryConfig;
import com.linkedin.openhouse.jobs.util.RetentionConfig;
import com.linkedin.openhouse.jobs.util.TableDataLayoutMetadata;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.tables.client.api.DatabaseApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.model.ClusteringColumn;
import com.linkedin.openhouse.tables.client.model.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetAllSoftDeletedTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetDatabaseResponseBody;
import com.linkedin.openhouse.tables.client.model.GetSoftDeletedTableResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.History;
import com.linkedin.openhouse.tables.client.model.PageGetSoftDeletedTableResponseBody;
import com.linkedin.openhouse.tables.client.model.Policies;
import com.linkedin.openhouse.tables.client.model.Replication;
import com.linkedin.openhouse.tables.client.model.ReplicationConfig;
import com.linkedin.openhouse.tables.client.model.Retention;
import com.linkedin.openhouse.tables.client.model.RetentionColumnPattern;
import com.linkedin.openhouse.tables.client.model.TimePartitionSpec;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
public class TablesClientTest {
  private final String testDbName = "test_db_name";
  private final String testTableName = "test_table_name";
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

  @Test
  void testSystemActionHeaderIsSentOnlyWhenRequestedAndColumnDefaultsAlways() throws IOException {
    List<String> declarations = Collections.synchronizedList(new ArrayList<>());
    List<String> capabilities = Collections.synchronizedList(new ArrayList<>());
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/v1/databases",
        exchange -> {
          declarations.add(exchange.getRequestHeaders().getFirst("X-OpenHouse-Action-Type"));
          capabilities.add(
              exchange.getRequestHeaders().getFirst("X-OpenHouse-Client-Capabilities"));
          byte[] body = "{\"results\":[]}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
    try {
      String basePath = "http://localhost:" + server.getAddress().getPort();
      DatabaseTableFilter filter = DatabaseTableFilter.of(".*", ".*", 0);
      for (TablesClient client :
          Arrays.asList(
              new TablesClientFactory(basePath, filter, "token", null)
                  .withSystemAction(true)
                  .create(),
              new TablesClientFactory(basePath, filter, "token", null).create())) {
        client.getDatabases();
        client.getAllTables("db");
      }
    } finally {
      server.stop(0);
    }
    Assertions.assertEquals(Arrays.asList("SYSTEM", "SYSTEM", null, null), declarations);
    Assertions.assertEquals(Collections.nCopies(4, "column-defaults"), capabilities);
  }

  @BeforeEach
  void setup() {
    apiMock = Mockito.mock(TableApi.class);
    dbApiMock = Mockito.mock(DatabaseApi.class);
    storageClient = Mockito.mock(StorageClient.class);
    RetryPolicy retryPolicy = new NeverRetryPolicy();

    client =
        Mockito.spy(
            clientFactory.create(
                RetryTemplate.builder().customPolicy(retryPolicy).build(),
                apiMock,
                dbApiMock,
                storageClient));
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
  void testMaintenanceProperties() {
    GetTableResponseBody tableResponseBodyMock =
        createNonPartitionedTableResponseBodyMock(testDbName, testTableName);
    Mockito.when(tableResponseBodyMock.getTableProperties())
        .thenReturn(
            Stream.of(
                    new String[][] {
                      {"maintenance.disabled", "true"},
                      {"maintenance.RETENTION.disabled", "true"},
                      {"some.other.property", "value"}
                    })
                .collect(Collectors.toMap(data -> data[0], data -> data[1])));

    Mono<GetTableResponseBody> tableResponseMock =
        (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(tableResponseMock.block(any(Duration.class))).thenReturn(tableResponseBodyMock);
    Mockito.when(apiMock.getTableV1(testDbName, testTableName)).thenReturn(tableResponseMock);

    Optional<TableMetadata> tableMetadata =
        client.mapTableResponseToTableMetadata(tableResponseBodyMock);
    Assertions.assertTrue(tableMetadata.isPresent());
    Assertions.assertEquals(2, tableMetadata.get().getJobExecutionProperties().size());
    Assertions.assertEquals(
        "true", tableMetadata.get().getJobExecutionProperties().get("disabled"));
    Assertions.assertEquals(
        "true", tableMetadata.get().getJobExecutionProperties().get("RETENTION.disabled"));
  }

  @Test
  void testGetOrphanTableDirectories() {
    mockLiveTables(testTableName);
    mockNoSoftDeletedTables();
    mockTableDirectories(
        tableDirectoryName(testTableName), tableDirectoryName(testOrphanDirectoryName));

    Assertions.assertEquals(
        Collections.singletonList(directoryMetadata(tableDirectoryName(testOrphanDirectoryName))),
        client.getOrphanTableDirectories(new Path(testDbName)));
  }

  @Test
  void testGetOrphanTableDirectoriesProtectsSoftDeletedTables() {
    mockLiveTables(testTableName);
    mockSoftDeletedTablesPage(0, 2, 2L, "dropped_1");
    mockSoftDeletedTablesPage(1, 2, 2L, "dropped_2");
    mockTableDirectories(
        tableDirectoryName(testTableName),
        tableDirectoryName("dropped_1"),
        tableDirectoryName("dropped_2"),
        tableDirectoryName(testOrphanDirectoryName));

    Assertions.assertEquals(
        Collections.singletonList(directoryMetadata(tableDirectoryName(testOrphanDirectoryName))),
        client.getOrphanTableDirectories(new Path(testDbName)));
  }

  @Test
  void testGetOrphanTableDirectoriesProtectsTableRestoredDuringLookup() {
    mockLiveTables(testTableName, "restored");
    GetAllTablesResponseBody beforeRestore = allTablesResponse(testTableName);
    GetAllTablesResponseBody afterRestore = allTablesResponse(testTableName, "restored");
    Mockito.when(apiMock.searchTablesV1(testDbName))
        .thenReturn(Mono.just(beforeRestore))
        .thenReturn(Mono.just(afterRestore));
    mockNoSoftDeletedTables();
    mockTableDirectories(
        tableDirectoryName(testTableName),
        tableDirectoryName("restored"),
        tableDirectoryName(testOrphanDirectoryName));

    Assertions.assertEquals(
        Collections.singletonList(directoryMetadata(tableDirectoryName(testOrphanDirectoryName))),
        client.getOrphanTableDirectories(new Path(testDbName)));
    Mockito.verify(apiMock, Mockito.times(1)).getTableV1(testDbName, testTableName);
  }

  @Test
  void testGetOrphanTableDirectoriesAppliesTableFilterOnlyToCandidates() {
    TablesClient filteredClient =
        new TablesClientFactory(
                "base_path",
                DatabaseTableFilter.of(".*", "orphan.*", 0),
                null,
                ParameterizedHdfsStorageProvider.of(
                    "hadoop", "hdfs://localhost/", "/jobs/openhouse/"))
            .create(
                RetryTemplate.builder().customPolicy(new NeverRetryPolicy()).build(),
                apiMock,
                dbApiMock,
                storageClient);
    mockLiveTables(testTableName);
    mockNoSoftDeletedTables();
    mockTableDirectories(
        tableDirectoryName(testTableName),
        tableDirectoryName("orphan_1"),
        tableDirectoryName(testOrphanDirectoryName));

    Assertions.assertEquals(
        Collections.singletonList(directoryMetadata(tableDirectoryName("orphan_1"))),
        filteredClient.getOrphanTableDirectories(new Path(testDbName)));
  }

  @Test
  void testGetOrphanTableDirectoriesIgnoresDirectoriesNotNamedLikeTables() {
    mockLiveTables(testTableName);
    mockNoSoftDeletedTables();
    mockTableDirectories(
        tableDirectoryName(testTableName),
        tableDirectoryName(testOrphanDirectoryName),
        ".staging",
        "_temporary",
        "backup",
        testOrphanDirectoryName + "-1234");

    Assertions.assertEquals(
        Collections.singletonList(directoryMetadata(tableDirectoryName(testOrphanDirectoryName))),
        client.getOrphanTableDirectories(new Path(testDbName)));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "tableSearchFails",
        "tableSearchReturnsNothing",
        "noLiveTables",
        "tableLoadFails",
        "softDeletedSearchFails",
        "softDeletedTablesChangeWhilePaging",
        "noListedDirectoryInUse"
      })
  void testGetOrphanTableDirectoriesSkipsDatabaseWhenLookupIsUnreliable(String failure) {
    mockLiveTables(testTableName);
    mockNoSoftDeletedTables();
    mockTableDirectories(
        tableDirectoryName(testTableName), tableDirectoryName(testOrphanDirectoryName));
    switch (failure) {
      case "tableSearchFails":
        Mockito.when(apiMock.searchTablesV1(testDbName))
            .thenReturn(Mono.error(new RuntimeException("search failed")));
        break;
      case "tableSearchReturnsNothing":
        Mockito.when(apiMock.searchTablesV1(testDbName)).thenReturn(Mono.empty());
        break;
      case "noLiveTables":
        mockTableSearch();
        break;
      case "tableLoadFails":
        Mockito.when(apiMock.getTableV1(testDbName, testTableName))
            .thenReturn(Mono.error(new RuntimeException("load failed")));
        break;
      case "softDeletedSearchFails":
        Mockito.when(
                apiMock.searchSoftDeletedTablesV1(
                    eq(testDbName), isNull(), anyInt(), anyInt(), isNull()))
            .thenReturn(Mono.error(new RuntimeException("search failed")));
        break;
      case "softDeletedTablesChangeWhilePaging":
        mockSoftDeletedTablesPage(0, 2, 2L, "dropped_1");
        mockSoftDeletedTablesPage(1, 1, 1L);
        break;
      case "noListedDirectoryInUse":
        mockTableDirectories(
            tableDirectoryName(testOrphanDirectoryName), tableDirectoryName("orphan_1"));
        break;
      default:
        throw new IllegalArgumentException(failure);
    }

    Assertions.assertEquals(
        Collections.emptyList(), client.getOrphanTableDirectories(new Path(testDbName)));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"/data/openhouse/test_db_name/test_table_name"})
  void testGetOrphanTableDirectoriesSkipsDatabaseWhenTableLocationIsInvalid(String location) {
    mockLiveTables(testTableName);
    GetTableResponseBody table =
        createTableWithLocationResponseBodyMock(testDbName, testTableName, location);
    Mockito.when(apiMock.getTableV1(testDbName, testTableName)).thenReturn(Mono.just(table));
    mockNoSoftDeletedTables();
    mockTableDirectories(
        tableDirectoryName(testTableName), tableDirectoryName(testOrphanDirectoryName));

    Assertions.assertEquals(
        Collections.emptyList(), client.getOrphanTableDirectories(new Path(testDbName)));
  }

  @ParameterizedTest
  @CsvSource({
    "tableNotFound, true",
    "tableRecreatedInAnotherDirectory, true",
    "tableUsesDirectory, false",
    "softDeletedTableUsesDirectory, false",
    "tableLookupFails, false",
    "tableLocationInvalid, false",
    "softDeletedLookupFails, false",
    "notTableDirectory, false"
  })
  void testIsOrphanTableDirectory(String scenario, boolean expected) {
    Path directory = new Path(testDbName, tableDirectoryName(testTableName));
    Mockito.when(apiMock.getTableV1(testDbName, testTableName))
        .thenReturn(Mono.error(httpError(404)));
    mockSoftDeletedTablesPage(testTableName, 0, 0, 0L);
    GetTableResponseBody table;
    switch (scenario) {
      case "tableNotFound":
        break;
      case "tableRecreatedInAnotherDirectory":
        table =
            createTableWithLocationResponseBodyMock(
                testDbName,
                testTableName,
                String.format(
                    "/data/openhouse/%s/%s-%s/00001.metadata.json",
                    testDbName, testTableName, UUID.randomUUID()));
        Mockito.when(apiMock.getTableV1(testDbName, testTableName)).thenReturn(Mono.just(table));
        break;
      case "tableUsesDirectory":
        mockLiveTables(testTableName);
        break;
      case "softDeletedTableUsesDirectory":
        mockSoftDeletedTablesPage(testTableName, 0, 1, 1L, testTableName);
        break;
      case "tableLookupFails":
        Mockito.when(apiMock.getTableV1(testDbName, testTableName))
            .thenReturn(Mono.error(httpError(500)));
        break;
      case "tableLocationInvalid":
        table =
            createTableWithLocationResponseBodyMock(
                testDbName,
                testTableName,
                String.format("/data/openhouse/%s/%s", testDbName, directory.getName()));
        Mockito.when(apiMock.getTableV1(testDbName, testTableName)).thenReturn(Mono.just(table));
        break;
      case "softDeletedLookupFails":
        Mockito.when(
                apiMock.searchSoftDeletedTablesV1(
                    eq(testDbName), eq(testTableName), anyInt(), anyInt(), isNull()))
            .thenReturn(Mono.error(httpError(500)));
        break;
      case "notTableDirectory":
        directory = new Path(testDbName, "backup");
        break;
      default:
        throw new IllegalArgumentException(scenario);
    }

    Assertions.assertEquals(expected, client.isOrphanTableDirectory(directory));
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

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"America/Los_Angeles", " "})
  void testRetentionCarriesTimeZoneIntoJobConfiguration(String timeZone) {
    TimePartitionSpec partitionSpec = Mockito.mock(TimePartitionSpec.class);
    Policies policies = Mockito.mock(Policies.class);
    Retention retention = Mockito.mock(Retention.class);
    Mockito.when(policies.getRetention()).thenReturn(retention);
    Mockito.when(retention.getCount()).thenReturn(testRetentionTTLDays);
    Mockito.when(retention.getGranularity()).thenReturn(Retention.GranularityEnum.DAY);
    Mockito.when(retention.getTimeZone()).thenReturn(timeZone);
    Mockito.when(partitionSpec.getColumnName()).thenReturn(testPartitionColumnName);
    GetTableResponseBody responseBody =
        setUpResponseBodyMock(testDbName, testTableNamePartitioned, partitionSpec, policies);
    Mockito.when(responseBody.getTableType())
        .thenReturn(GetTableResponseBody.TableTypeEnum.PRIMARY_TABLE);
    Mono<GetTableResponseBody> responseMock = (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class))).thenReturn(responseBody);
    Mockito.when(apiMock.getTableV1(testDbName, testTableNamePartitioned)).thenReturn(responseMock);

    Optional<RetentionConfig> result =
        client.getTableRetention(
            TableMetadata.builder().dbName(testDbName).tableName(testTableNamePartitioned).build());

    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(Optional.ofNullable(timeZone), result.get().getTimeZone());
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
  void testPrimaryTableWithReplicationConfig() {
    GetTableResponseBody primaryTableWithReplicationConfigResponseBodyMock =
        createPrimaryTableWithReplicationPolicyResponseBodyMock(
            testDbName, testTableName, "schedule", "interval", "cluster");
    Mono<GetTableResponseBody> responseMock = (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class)))
        .thenReturn(primaryTableWithReplicationConfigResponseBodyMock);
    Mockito.when(apiMock.getTableV1(testDbName, testTableName)).thenReturn(responseMock);
    Optional<List<com.linkedin.openhouse.jobs.util.ReplicationConfig>> result =
        client.getTableReplication(
            TableMetadata.builder().dbName(testDbName).tableName(testTableName).build());
    Assertions.assertTrue(
        result.isPresent(), "Retention config must be present for a test partitioned table");
    List<com.linkedin.openhouse.jobs.util.ReplicationConfig> replicationConfigs = new ArrayList<>();
    com.linkedin.openhouse.jobs.util.ReplicationConfig replicationConfig =
        com.linkedin.openhouse.jobs.util.ReplicationConfig.builder()
            .schedule("schedule")
            .cluster("cluster")
            .tableOwner("")
            .build();
    replicationConfigs.add(replicationConfig);
    Assertions.assertEquals(replicationConfigs, result.orElse(null));
    Mockito.verify(responseMock, Mockito.times(1)).block(any(Duration.class));
    Mockito.verify(apiMock, Mockito.times(1)).getTableV1(testDbName, testTableName);
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

  @Test
  void testGetTableDataLayoutMetadataListInParallel() {
    Mockito.doReturn(Arrays.asList("db1", "db2")).when(client).getDatabases();
    Mockito.doReturn(
            Mono.just(
                Arrays.asList(
                    createTableResponseBodyMock("db1", "table1"),
                    createTableResponseBodyMock("db1", "table2"))))
        .when(client)
        .getAllTablesAsync("db1");
    Mockito.doReturn(
            Mono.just(
                Arrays.asList(
                    createTableResponseBodyMock("db2", "table1"),
                    createTableResponseBodyMock("db2", "table2"))))
        .when(client)
        .getAllTablesAsync("db2");
    Mockito.doReturn(Arrays.asList(createTableDataLayoutMetadataMock()))
        .when(client)
        .mapTableResponseToTableDataLayoutMetadataList(any());
    Assertions.assertEquals(4, client.getTableDataLayoutMetadataListInParallel(2).size());
  }

  private TableDataLayoutMetadata createTableDataLayoutMetadataMock() {
    TableDataLayoutMetadata metadata = Mockito.mock(TableDataLayoutMetadata.class);
    Mockito.when(metadata.getDataLayoutStrategy())
        .thenReturn(Mockito.mock(DataLayoutStrategy.class));
    Mockito.when(metadata.getDbName()).thenReturn("db");
    Mockito.when(metadata.getTableName()).thenReturn("table");
    Mockito.when(metadata.getCreationTimeMs()).thenReturn(1L);
    return metadata;
  }

  void testGetTableHistoryWithTimeBasedPolicy() {
    GetTableResponseBody responseBody =
        createTableWithTimeBasedHistoryMock(
            testDbName, testTableName, 30, History.GranularityEnum.DAY, null);
    Mono<GetTableResponseBody> responseMock = (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class))).thenReturn(responseBody);
    Mockito.when(apiMock.getTableV1(testDbName, testTableName)).thenReturn(responseMock);

    Optional<HistoryConfig> result =
        client.getTableHistory(
            TableMetadata.builder().dbName(testDbName).tableName(testTableName).build());

    Assertions.assertTrue(result.isPresent());
    HistoryConfig historyConfig = result.get();
    Assertions.assertEquals(30, historyConfig.getMaxAge());
    Assertions.assertEquals(History.GranularityEnum.DAY, historyConfig.getGranularity());
    Assertions.assertEquals(0, historyConfig.getVersions());
  }

  @Test
  void testGetTableHistoryWithVersionBasedPolicy() {
    GetTableResponseBody responseBody =
        createTableWithVersionBasedHistoryMock(testDbName, testTableName, 5);
    Mono<GetTableResponseBody> responseMock = (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class))).thenReturn(responseBody);
    Mockito.when(apiMock.getTableV1(testDbName, testTableName)).thenReturn(responseMock);

    Optional<HistoryConfig> result =
        client.getTableHistory(
            TableMetadata.builder().dbName(testDbName).tableName(testTableName).build());

    Assertions.assertTrue(result.isPresent());
    HistoryConfig historyConfig = result.get();
    Assertions.assertEquals(0, historyConfig.getMaxAge());
    Assertions.assertNull(historyConfig.getGranularity());
    Assertions.assertEquals(5, historyConfig.getVersions());
  }

  @Test
  void testGetTableHistoryWithInvalidPolicy() {
    GetTableResponseBody responseBody =
        createTableWithInvalidHistoryMock(testDbName, testTableName);
    Mono<GetTableResponseBody> responseMock = (Mono<GetTableResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(any(Duration.class))).thenReturn(responseBody);
    Mockito.when(apiMock.getTableV1(testDbName, testTableName)).thenReturn(responseMock);

    Optional<HistoryConfig> result =
        client.getTableHistory(
            TableMetadata.builder().dbName(testDbName).tableName(testTableName).build());

    Assertions.assertFalse(result.isPresent());
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
    Mockito.when(responseBody.getTableCreator()).thenReturn("");
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

  private static String tableDirectoryName(String tableName) {
    return tableName
        + "-"
        + UUID.nameUUIDFromBytes(tableName.getBytes(StandardCharsets.UTF_8)).toString();
  }

  private String metadataLocation(String tableName) {
    return String.format(
        "/data/openhouse/%s/%s/00001-%s.metadata.json",
        testDbName, tableDirectoryName(tableName), UUID.randomUUID());
  }

  // Responses are built before stubbing because Mockito cannot stub a mock inside thenReturn().
  private GetAllTablesResponseBody allTablesResponse(String... tableNames) {
    List<GetTableResponseBody> results = new ArrayList<>();
    for (String tableName : tableNames) {
      results.add(createTableResponseBodyMock(testDbName, tableName));
    }
    GetAllTablesResponseBody response = Mockito.mock(GetAllTablesResponseBody.class);
    Mockito.when(response.getResults()).thenReturn(results);
    return response;
  }

  private void mockTableSearch(String... tableNames) {
    GetAllTablesResponseBody response = allTablesResponse(tableNames);
    Mockito.when(apiMock.searchTablesV1(testDbName)).thenReturn(Mono.just(response));
  }

  private void mockLiveTables(String... tableNames) {
    for (String tableName : tableNames) {
      GetTableResponseBody table =
          createTableWithLocationResponseBodyMock(
              testDbName, tableName, metadataLocation(tableName));
      Mockito.when(apiMock.getTableV1(testDbName, tableName)).thenReturn(Mono.just(table));
    }
    mockTableSearch(tableNames);
  }

  private void mockSoftDeletedTablesPage(
      int page, int totalPages, long totalElements, String... tableNames) {
    mockSoftDeletedTablesPage(null, page, totalPages, totalElements, tableNames);
  }

  /** Mocks a page of soft-deleted tables searched by table ID, or by database only if null. */
  private void mockSoftDeletedTablesPage(
      String tableIdFilter, int page, int totalPages, long totalElements, String... tableNames) {
    List<GetSoftDeletedTableResponseBody> content = new ArrayList<>();
    for (String tableName : tableNames) {
      GetSoftDeletedTableResponseBody table = Mockito.mock(GetSoftDeletedTableResponseBody.class);
      Mockito.when(table.getTableId()).thenReturn(tableName);
      Mockito.when(table.getTableLocation()).thenReturn(metadataLocation(tableName));
      content.add(table);
    }
    GetAllSoftDeletedTablesResponseBody response =
        new GetAllSoftDeletedTablesResponseBody()
            .pageResults(
                new PageGetSoftDeletedTableResponseBody()
                    .content(content)
                    .totalPages(totalPages)
                    .totalElements(totalElements));
    Mockito.when(
            apiMock.searchSoftDeletedTablesV1(
                eq(testDbName),
                tableIdFilter == null ? isNull() : eq(tableIdFilter),
                eq(page),
                anyInt(),
                isNull()))
        .thenReturn(Mono.just(response));
  }

  private void mockNoSoftDeletedTables() {
    mockSoftDeletedTablesPage(0, 0, 0L);
  }

  private static WebClientResponseException httpError(int statusCode) {
    return WebClientResponseException.create(statusCode, "error", null, null, null);
  }

  private DirectoryMetadata directoryMetadata(String directoryName) {
    return DirectoryMetadata.builder()
        .path(new Path(testDbName, directoryName))
        .creator(testTableCreator)
        .build();
  }

  private void mockTableDirectories(String... directoryNames) {
    Mockito.when(storageClient.getSubDirectoriesWithOwners(any(Path.class)))
        .thenReturn(
            Arrays.stream(directoryNames)
                .map(this::directoryMetadata)
                .collect(Collectors.toList()));
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

  private GetTableResponseBody createPrimaryTableWithReplicationPolicyResponseBodyMock(
      String dbName, String tableName, String schedule, String interval, String cluster) {
    Policies policies = Mockito.mock(Policies.class);
    Replication replication = Mockito.mock(Replication.class);
    List<ReplicationConfig> replicationConfigs = new ArrayList<>();
    ReplicationConfig replicationConfig = Mockito.mock(ReplicationConfig.class);
    replicationConfigs.add(replicationConfig);
    replication.setConfig(replicationConfigs);

    policies.setReplication(replication);
    Mockito.when(replication.getConfig()).thenReturn(replicationConfigs);
    Mockito.when(policies.getReplication()).thenReturn(replication);
    Mockito.when(replicationConfig.getCronSchedule()).thenReturn(schedule);
    Mockito.when(replicationConfig.getDestination()).thenReturn(cluster);
    Mockito.when(replicationConfig.getInterval()).thenReturn(interval);
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
    Mockito.when(responseBody.getTableCreator()).thenReturn("");
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

  private GetTableResponseBody createTableWithTimeBasedHistoryMock(
      String dbName,
      String tableName,
      int maxAge,
      History.GranularityEnum granularity,
      Integer versions) {
    GetTableResponseBody responseBody = Mockito.mock(GetTableResponseBody.class);
    Policies policies = Mockito.mock(Policies.class);
    History history = Mockito.mock(History.class);

    Mockito.when(responseBody.getDatabaseId()).thenReturn(dbName);
    Mockito.when(responseBody.getTableId()).thenReturn(tableName);
    Mockito.when(responseBody.getPolicies()).thenReturn(policies);
    Mockito.when(policies.getHistory()).thenReturn(history);
    Mockito.when(history.getMaxAge()).thenReturn(maxAge);
    Mockito.when(history.getGranularity()).thenReturn(granularity);
    Mockito.when(history.getVersions()).thenReturn(versions);

    return responseBody;
  }

  private GetTableResponseBody createTableWithVersionBasedHistoryMock(
      String dbName, String tableName, int versions) {
    GetTableResponseBody responseBody = Mockito.mock(GetTableResponseBody.class);
    Policies policies = Mockito.mock(Policies.class);
    History history = Mockito.mock(History.class);

    Mockito.when(responseBody.getDatabaseId()).thenReturn(dbName);
    Mockito.when(responseBody.getTableId()).thenReturn(tableName);
    Mockito.when(responseBody.getPolicies()).thenReturn(policies);
    Mockito.when(policies.getHistory()).thenReturn(history);
    Mockito.when(history.getMaxAge()).thenReturn(null);
    Mockito.when(history.getGranularity()).thenReturn(null);
    Mockito.when(history.getVersions()).thenReturn(versions);

    return responseBody;
  }

  private GetTableResponseBody createTableWithInvalidHistoryMock(String dbName, String tableName) {
    GetTableResponseBody responseBody = Mockito.mock(GetTableResponseBody.class);
    Policies policies = Mockito.mock(Policies.class);
    History history = Mockito.mock(History.class);

    Mockito.when(responseBody.getDatabaseId()).thenReturn(dbName);
    Mockito.when(responseBody.getTableId()).thenReturn(tableName);
    Mockito.when(responseBody.getPolicies()).thenReturn(policies);
    Mockito.when(policies.getHistory()).thenReturn(history);
    Mockito.when(history.getMaxAge()).thenReturn(0);
    Mockito.when(history.getGranularity()).thenReturn(null);
    Mockito.when(history.getVersions()).thenReturn(0);

    return responseBody;
  }
}
