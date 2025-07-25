package com.linkedin.openhouse.jobs.client;

import com.linkedin.openhouse.datalayout.persistence.StrategiesDaoTableProps;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.jobs.util.DatabaseTableFilter;
import com.linkedin.openhouse.jobs.util.DirectoryMetadata;
import com.linkedin.openhouse.jobs.util.HistoryConfig;
import com.linkedin.openhouse.jobs.util.ReplicationConfig;
import com.linkedin.openhouse.jobs.util.RetentionConfig;
import com.linkedin.openhouse.jobs.util.RetryUtil;
import com.linkedin.openhouse.jobs.util.TableDataLayoutMetadata;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.tables.client.api.DatabaseApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.model.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetDatabaseResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.History;
import com.linkedin.openhouse.tables.client.model.Policies;
import com.linkedin.openhouse.tables.client.model.Replication;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * A read-only client for interacting with /tables service. Supports limited operations necessary
 * for reading tables metadata for scheduling.
 */
@Slf4j
@AllArgsConstructor
public class TablesClient {
  private static final String MAINTENANCE_PROPERTY_PREFIX = "maintenance.";
  private static final int REQUEST_TIMEOUT_SECONDS = 180;
  private final RetryTemplate retryTemplate;
  private final TableApi tableApi;
  private final DatabaseApi databaseApi;
  private final DatabaseTableFilter databaseTableFilter;
  @VisibleForTesting private final StorageClient storageClient;

  public Optional<RetentionConfig> getTableRetention(TableMetadata tableMetadata) {
    GetTableResponseBody response = getTable(tableMetadata);
    return getTableRetention(response);
  }

  public Optional<List<ReplicationConfig>> getTableReplication(TableMetadata tableMetadata) {
    GetTableResponseBody response = getTable(tableMetadata);
    return getTableReplication(response);
  }

  public Optional<HistoryConfig> getTableHistory(TableMetadata tableMetadata) {
    GetTableResponseBody response = getTable(tableMetadata);
    return getTableHistory(response);
  }

  private Optional<RetentionConfig> getTableRetention(GetTableResponseBody response) {
    // timePartitionSpec or retention.ColumnPattern should be present to run Retention job on a
    // table.
    if (response == null
        || response.getPolicies() == null
        || response.getPolicies().getRetention() == null
        || (response.getTimePartitioning() == null
            && response.getPolicies().getRetention().getColumnPattern() == null)) {
      return Optional.empty();
    }
    Policies policies = response.getPolicies();
    String columnName;
    String columnPattern = "";
    if (response.getTimePartitioning() != null) {
      columnName = response.getTimePartitioning().getColumnName();
    } else {
      columnName =
          Objects.requireNonNull(policies.getRetention().getColumnPattern()).getColumnName();
      columnPattern = policies.getRetention().getColumnPattern().getPattern();
    }

    return Optional.ofNullable(
        RetentionConfig.builder()
            .columnName(columnName)
            .columnPattern(columnPattern)
            .count(policies.getRetention().getCount())
            .granularity(policies.getRetention().getGranularity())
            .build());
  }

  private Optional<HistoryConfig> getTableHistory(GetTableResponseBody response) {
    if (response == null
        || response.getPolicies() == null
        || response.getPolicies().getHistory() == null) {
      return Optional.empty();
    }
    History history = response.getPolicies().getHistory();

    // Validate that at least one of maxAge/granularity or versions is configured
    boolean hasTimeBasedHistory =
        history.getMaxAge() != null && history.getMaxAge() > 0 && history.getGranularity() != null;
    boolean hasVersionBasedHistory = history.getVersions() != null && history.getVersions() > 0;

    if (!hasTimeBasedHistory && !hasVersionBasedHistory) {
      return Optional.empty();
    }

    return Optional.of(
        HistoryConfig.builder()
            .maxAge(hasTimeBasedHistory ? history.getMaxAge() : 0)
            .granularity(hasTimeBasedHistory ? history.getGranularity() : null)
            .versions(hasVersionBasedHistory ? history.getVersions() : 0)
            .build());
  }

  private Optional<List<ReplicationConfig>> getTableReplication(GetTableResponseBody response) {
    // At least one replication config must be present
    if (response == null
        || response.getPolicies() == null
        || response.getPolicies().getReplication() == null
        || response.getPolicies().getReplication().getConfig().size() <= 0) {
      return Optional.empty();
    }
    List<ReplicationConfig> replicationConfigList = new ArrayList<>();
    Replication replication = response.getPolicies().getReplication();
    List<com.linkedin.openhouse.tables.client.model.ReplicationConfig> replicationConfig =
        replication.getConfig();

    replicationConfig.forEach(
        rc ->
            replicationConfigList.add(
                ReplicationConfig.builder()
                    .cluster(rc.getDestination())
                    .tableOwner(response.getTableCreator())
                    .schedule(rc.getCronSchedule())
                    .build()));
    // since replicationConfigList is initialized, it cannot be null.
    return Optional.of(replicationConfigList);
  }

  protected GetTableResponseBody getTable(TableMetadata tableMetadata) {
    return getTable(tableMetadata.getDbName(), tableMetadata.getTableName());
  }

  protected GetTableResponseBody getTable(String dbName, String tableName) {
    return RetryUtil.executeWithRetry(
        retryTemplate,
        (RetryCallback<GetTableResponseBody, Exception>)
            context ->
                tableApi
                    .getTableV1(dbName, tableName)
                    .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS)),
        null);
  }

  /**
   * Filter database
   *
   * @param dbName
   * @return
   */
  public boolean applyDatabaseFilter(String dbName) {
    return databaseTableFilter.applyDatabaseName(dbName);
  }

  /**
   * Apply table metadata filter
   *
   * @param tableMetadata
   * @return
   */
  public boolean applyTableMetadataFilter(TableMetadata tableMetadata) {
    return databaseTableFilter.apply(tableMetadata);
  }

  /**
   * Get all tables for the given database
   *
   * @param dbName
   * @return
   */
  public GetAllTablesResponseBody getAllTables(String dbName) {
    return RetryUtil.executeWithRetry(
        retryTemplate,
        (RetryCallback<GetAllTablesResponseBody, Exception>)
            context -> {
              GetAllTablesResponseBody response =
                  tableApi
                      .searchTablesV1(dbName)
                      .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
              if (response == null) {
                return null;
              }
              return response;
            },
        null);
  }

  /**
   * Scans all databases and tables in the databases, converts Tables Service responses to {@link
   * TableMetadata}, filters out using {@link DatabaseTableFilter}, and returns as a list.
   */
  public List<TableMetadata> getTableMetadataList() {
    List<TableMetadata> tableMetadataList = new ArrayList<>();
    for (String dbName : getDatabases()) {
      if (databaseTableFilter.applyDatabaseName(dbName)) {
        tableMetadataList.addAll(
            RetryUtil.executeWithRetry(
                retryTemplate,
                (RetryCallback<List<TableMetadata>, Exception>)
                    context -> {
                      GetAllTablesResponseBody response =
                          tableApi
                              .searchTablesV1(dbName)
                              .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
                      if (response == null) {
                        return Collections.emptyList();
                      }
                      return Optional.ofNullable(response.getResults())
                          .map(Collection::stream)
                          .orElseGet(Stream::empty)
                          .flatMap(
                              shallowResponseBody ->
                                  mapTableResponseToTableMetadata(shallowResponseBody)
                                      .filter(databaseTableFilter::apply)
                                      .map(Stream::of)
                                      .orElseGet(Stream::empty))
                          .collect(Collectors.toList());
                    },
                Collections.emptyList()));
      }
    }
    return tableMetadataList;
  }

  public List<TableDataLayoutMetadata> getTableDataLayoutMetadataList() {
    List<TableDataLayoutMetadata> tableDataLayoutMetadataList = new ArrayList<>();
    for (String dbName : getDatabases()) {
      if (databaseTableFilter.applyDatabaseName(dbName)) {
        tableDataLayoutMetadataList.addAll(
            RetryUtil.executeWithRetry(
                retryTemplate,
                (RetryCallback<List<TableDataLayoutMetadata>, Exception>)
                    context -> {
                      GetAllTablesResponseBody response =
                          tableApi
                              .searchTablesV1(dbName)
                              .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
                      if (response == null) {
                        return Collections.emptyList();
                      }
                      return Optional.ofNullable(response.getResults())
                          .map(Collection::stream)
                          .orElseGet(Stream::empty)
                          .flatMap(
                              shallowResponseBody ->
                                  mapTableResponseToTableDataLayoutMetadataList(shallowResponseBody)
                                      .stream()
                                      .filter(databaseTableFilter::apply))
                          .collect(Collectors.toList());
                    },
                Collections.emptyList()));
      }
    }
    return tableDataLayoutMetadataList;
  }

  /**
   * For the given database name, get all registered tables
   *
   * @param dbName database name
   * @return a set of registered table names
   */
  public Set<String> getTableNamesForDbName(String dbName) {
    Set<String> tableNames = new HashSet<>();
    if (databaseTableFilter.applyDatabaseName(dbName)) {
      tableNames.addAll(
          RetryUtil.executeWithRetry(
              retryTemplate,
              (RetryCallback<Set<String>, Exception>)
                  context -> {
                    GetAllTablesResponseBody response =
                        tableApi
                            .searchTablesV1(dbName)
                            .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
                    if (response == null) {
                      return Collections.emptySet();
                    }
                    return Optional.ofNullable(response.getResults())
                        .map(Collection::stream)
                        .orElseGet(Stream::empty)
                        .map(this::mapTableResponseToTableDirectoryName)
                        .filter(databaseTableFilter::applyTableDirectoryPath)
                        .collect(Collectors.toSet());
                  },
              Collections.emptySet()));
    }
    return tableNames;
  }

  /**
   * Given a database path, get all orphan table directories under that path.
   *
   * @param dbPath database path to get table directories from
   * @return a list of DirectoryMetadata that are not registered
   */
  @VisibleForTesting
  public List<DirectoryMetadata> getOrphanTableDirectories(Path dbPath) {
    // a set of directory names
    Set<String> registeredTableDirectories = getTableNamesForDbName(dbPath.getName());
    List<DirectoryMetadata> allTableDirectories = storageClient.getSubDirectoriesWithOwners(dbPath);
    return allTableDirectories.stream()
        .filter(
            directoryMetadata ->
                !registeredTableDirectories.contains(directoryMetadata.getBaseName()))
        .collect(Collectors.toList());
  }

  /**
   * Get all orphan table directories in the corresponding file system.
   *
   * @return a list of DirectoryMetadata that are not registered
   */
  public List<DirectoryMetadata> getOrphanTableDirectories() {
    List<DirectoryMetadata> orphanTableDirectories = new ArrayList<>();
    // we use getDatabases interface to avoid accidentally deleting essential directories on the
    // database level
    for (String dbName : getDatabases()) {
      if (databaseTableFilter.applyDatabaseName(dbName)) {
        Path dbPath = new Path(storageClient.getRootPath(), dbName);
        orphanTableDirectories.addAll(getOrphanTableDirectories(dbPath));
      }
    }
    return orphanTableDirectories;
  }

  public List<String> getDatabases() {
    return RetryUtil.executeWithRetry(
        retryTemplate,
        (RetryCallback<List<String>, Exception>)
            context -> {
              GetAllDatabasesResponseBody response =
                  databaseApi
                      .getAllDatabasesV1()
                      .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
              return Optional.ofNullable(response == null ? null : response.getResults())
                  .map(Collection::stream)
                  .orElseGet(Stream::empty)
                  .map(GetDatabaseResponseBody::getDatabaseId)
                  .collect(Collectors.toList());
            },
        Collections.emptyList());
  }

  @VisibleForTesting
  public Optional<TableMetadata> mapTableResponseToTableMetadata(
      GetTableResponseBody shallowResponseBody) {
    GetTableResponseBody tableResponseBody =
        getTable(shallowResponseBody.getDatabaseId(), shallowResponseBody.getTableId());

    if (tableResponseBody == null) {
      log.error(
          "Error while fetching metadata for table: {}.{}",
          shallowResponseBody.getDatabaseId(),
          shallowResponseBody.getTableCreator());
      return Optional.empty();
    }

    TableMetadata.TableMetadataBuilder<?, ?> builder =
        TableMetadata.builder()
            .creator(getTableCreator(tableResponseBody))
            .dbName(Objects.requireNonNull(tableResponseBody.getDatabaseId()))
            .tableName(Objects.requireNonNull(tableResponseBody.getTableId()))
            .isPrimary(
                tableResponseBody.getTableType()
                    == GetTableResponseBody.TableTypeEnum.PRIMARY_TABLE)
            .isTimePartitioned(tableResponseBody.getTimePartitioning() != null)
            .isClustered(tableResponseBody.getClustering() != null)
            .retentionConfig(getTableRetention(tableResponseBody).orElse(null))
            .historyConfig(getTableHistory(tableResponseBody).orElse(null))
            .replicationConfig(getTableReplication(tableResponseBody).orElse(null))
            .jobExecutionProperties(getJobExecutionProperties(tableResponseBody))
            .creationTimeMs(Objects.requireNonNull(tableResponseBody.getCreationTime()));
    return Optional.of(builder.build());
  }

  @VisibleForTesting
  public List<TableDataLayoutMetadata> mapTableResponseToTableDataLayoutMetadataList(
      GetTableResponseBody shallowResponseBody) {
    GetTableResponseBody tableResponseBody =
        getTable(shallowResponseBody.getDatabaseId(), shallowResponseBody.getTableId());

    if (tableResponseBody == null) {
      log.error(
          "Error while fetching metadata for table: {}.{}",
          shallowResponseBody.getDatabaseId(),
          shallowResponseBody.getTableCreator());
      return Collections.emptyList();
    }

    TableDataLayoutMetadata.TableDataLayoutMetadataBuilder<?, ?> builder =
        TableDataLayoutMetadata.builder()
            .creator(getTableCreator(tableResponseBody))
            .dbName(Objects.requireNonNull(tableResponseBody.getDatabaseId()))
            .tableName(Objects.requireNonNull(tableResponseBody.getTableId()))
            .isPrimary(
                tableResponseBody.getTableType()
                    == GetTableResponseBody.TableTypeEnum.PRIMARY_TABLE)
            .isTimePartitioned(tableResponseBody.getTimePartitioning() != null)
            .isClustered(tableResponseBody.getClustering() != null)
            .retentionConfig(getTableRetention(tableResponseBody).orElse(null))
            .historyConfig(getTableHistory(tableResponseBody).orElse(null))
            .jobExecutionProperties(getJobExecutionProperties(tableResponseBody))
            .creationTimeMs(Objects.requireNonNull(tableResponseBody.getCreationTime()));
    List<TableDataLayoutMetadata> result = new ArrayList<>();
    for (DataLayoutStrategy strategy : getDataLayoutStrategies(tableResponseBody)) {
      result.add(builder.dataLayoutStrategy(strategy).build());
    }
    return result;
  }

  private @NonNull Map<String, String> getJobExecutionProperties(
      GetTableResponseBody responseBody) {
    if (responseBody.getTableProperties() == null) {
      return Collections.emptyMap();
    }
    return responseBody.getTableProperties().entrySet().stream()
        .filter(e -> e.getKey().startsWith(MAINTENANCE_PROPERTY_PREFIX))
        .map(
            e ->
                new AbstractMap.SimpleEntry<>(
                    e.getKey().substring(MAINTENANCE_PROPERTY_PREFIX.length()), e.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private List<DataLayoutStrategy> getDataLayoutStrategies(GetTableResponseBody tableResponseBody) {
    Map<String, String> tableProps = tableResponseBody.getTableProperties();
    if (tableProps == null
        || !tableProps.containsKey(StrategiesDaoTableProps.DATA_LAYOUT_STRATEGIES_PROPERTY_KEY)) {
      return Collections.emptyList();
    }
    return StrategiesDaoTableProps.deserializeList(
        tableProps.get(StrategiesDaoTableProps.DATA_LAYOUT_STRATEGIES_PROPERTY_KEY));
  }

  protected @NonNull String getTableCreator(GetTableResponseBody responseBody) {
    return Objects.requireNonNull(responseBody.getTableCreator());
  }

  private String mapTableResponseToTableDirectoryName(GetTableResponseBody responseBody) {
    TableMetadata metadata =
        TableMetadata.builder()
            .dbName(Objects.requireNonNull(responseBody.getDatabaseId()))
            .tableName(Objects.requireNonNull(responseBody.getTableId()))
            .build();
    String location = getTable(metadata).getTableLocation();
    return new Path(Objects.requireNonNull(location)).getParent().getName();
  }

  // Below are asynchronous methods
  /**
   * Fetch all tables for the given database asynchronously using Mono.
   *
   * @param database
   * @return Mono<List<GetTableResponseBody>>
   */
  public Mono<List<GetTableResponseBody>> getAllTablesAsync(String database) {
    return Mono.fromCallable(
            () -> {
              GetAllTablesResponseBody allTablesResponseBody = getAllTables(database);
              log.debug("Got all tables: {} for database {}", allTablesResponseBody, database);
              if (allTablesResponseBody == null) {
                return Collections.<GetTableResponseBody>emptyList();
              }
              return allTablesResponseBody.getResults();
            })
        .onErrorResume(
            ex -> {
              log.error("Error while fetching tables for database {}", database, ex);
              return Mono.just(Collections.emptyList());
            })
        .subscribeOn(
            Schedulers.boundedElastic()); // Offload the blocking call to boundedElastic scheduler
  }

  /**
   * Fetch table metadata for a table asynchronously using Mono.
   *
   * @param getTableResponseBody
   * @return Mono<TableMetadata>
   */
  public Mono<TableMetadata> getTableMetadataAsync(GetTableResponseBody getTableResponseBody) {
    return Mono.fromCallable(() -> mapTableResponseToTableMetadata(getTableResponseBody))
        .flatMap(
            optionalTableMetadata -> {
              if (optionalTableMetadata.isPresent()) {
                log.debug("Got table metadata for : {}", optionalTableMetadata.get());
                return Mono.just(optionalTableMetadata.get());
              } else {
                return Mono.empty();
              }
            })
        .onErrorResume(
            ex -> {
              log.error("Error while fetching table metadata for : {}", getTableResponseBody, ex);
              return Mono.empty();
            })
        .subscribeOn(
            Schedulers.boundedElastic()); // Offload the blocking call to boundedElastic scheduler
  }

  /**
   * Fetch table data layout metadata for a table asynchronously using Mono.
   *
   * @param getTableResponseBody
   * @return Mono<List<TableDataLayoutMetadata>
   */
  public Mono<List<TableDataLayoutMetadata>> getTableDataLayoutMetadataListAsync(
      GetTableResponseBody getTableResponseBody) {
    return Mono.fromCallable(
            () -> mapTableResponseToTableDataLayoutMetadataList(getTableResponseBody))
        .flatMap(
            tableDataLayoutMetadataList -> {
              if (!tableDataLayoutMetadataList.isEmpty()) {
                log.debug("Got table data layout metadata for : {}", tableDataLayoutMetadataList);
                return Mono.just(tableDataLayoutMetadataList);
              } else {
                return Mono.empty();
              }
            })
        .onErrorResume(
            ex -> {
              log.error(
                  "Error while fetching table data layout metadata for : {}",
                  getTableResponseBody,
                  ex);
              return Mono.empty();
            })
        .subscribeOn(
            Schedulers.boundedElastic()); // Offload the blocking call to boundedElastic scheduler
  }

  /**
   * Fetch table data layout metadata for all tables in parallel using Flux.
   *
   * @param numParallelMetadataFetch
   * @return List<TableDataLayoutMetadata>
   */
  public List<TableDataLayoutMetadata> getTableDataLayoutMetadataListInParallel(
      int numParallelMetadataFetch) {
    return Flux.fromIterable(getDatabases())
        .parallel(numParallelMetadataFetch)
        .runOn(Schedulers.boundedElastic())
        .filter(databaseTableFilter::applyDatabaseName)
        .flatMap(
            database ->
                getAllTablesAsync(database)
                    .flatMapMany(Flux::fromIterable)
                    .parallel(numParallelMetadataFetch)
                    .runOn(Schedulers.boundedElastic())
                    .flatMap(this::getTableDataLayoutMetadataListAsync)
                    .flatMap(Flux::fromIterable)
                    .filter(databaseTableFilter::apply))
        .sequential()
        .collectList()
        .block();
  }
}
