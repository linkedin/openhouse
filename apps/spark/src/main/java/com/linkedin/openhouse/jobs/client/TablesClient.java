package com.linkedin.openhouse.jobs.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.datalayout.persistence.StrategiesDaoTableProps;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.jobs.util.DatabaseMetadata;
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
import com.linkedin.openhouse.tables.client.api.ViewApi;
import com.linkedin.openhouse.tables.client.model.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetAllSoftDeletedTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.client.model.GetDatabaseResponseBody;
import com.linkedin.openhouse.tables.client.model.GetSoftDeletedTableResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.GetViewResponseBody;
import com.linkedin.openhouse.tables.client.model.History;
import com.linkedin.openhouse.tables.client.model.PageGetSoftDeletedTableResponseBody;
import com.linkedin.openhouse.tables.client.model.PageGetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.Policies;
import com.linkedin.openhouse.tables.client.model.Replication;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.reactive.function.client.WebClientResponseException;
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
  private static final String METADATA_FILE_SUFFIX = ".metadata.json";
  private static final int LISTING_PAGE_SIZE = 1000;
  private static final int MAX_LISTING_ATTEMPTS = 3;
  /** Message of the 404 the tables service returns when views are turned off for a database. */
  private static final String VIEWS_DISABLED_MESSAGE = "Views are disabled";

  private static final ObjectMapper ERROR_BODY_MAPPER = new ObjectMapper();
  private static final List<String> TABLE_LOCATION_FIELD =
      Collections.singletonList("tableLocation");
  /** OpenHouse names table directories {@code <tableId>-<tableUUID>}. */
  private static final Pattern TABLE_DIRECTORY_NAME_PATTERN =
      Pattern.compile(
          "^([A-Za-z0-9_]+)-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

  private final RetryTemplate retryTemplate;
  private final TableApi tableApi;
  private final DatabaseApi databaseApi;
  private final ViewApi viewApi;
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

  protected Optional<RetentionConfig> getTableRetention(GetTableResponseBody response) {
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
            .timeZone(Optional.ofNullable(policies.getRetention().getTimeZone()))
            .build());
  }

  protected Optional<HistoryConfig> getTableHistory(GetTableResponseBody response) {
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

  protected Optional<List<ReplicationConfig>> getTableReplication(GetTableResponseBody response) {
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
   * Scans all databases in the Tables Service, converts to {@link DatabaseMetadata}, and applies
   * filters
   */
  public List<DatabaseMetadata> getDatabaseMetadataList() {
    return getDatabases().stream()
        .filter(databaseTableFilter::applyDatabaseName)
        .map(dbName -> DatabaseMetadata.builder().dbName(dbName).build())
        .collect(Collectors.toList());
  }

  /**
   * Get the table directories of the given database that no live or soft-deleted table and no view
   * uses.
   *
   * <p>Fails closed: throws if the database directory cannot be listed, if the directories in use
   * cannot be determined completely, or if none of the listed directories is in use, which means
   * the listing and the table locations disagree. Only directories named like OpenHouse table
   * directories ({@code <tableId>-<tableUUID>}) are returned.
   *
   * @param dbName database to get table directories from
   * @return a list of DirectoryMetadata that are not used by any table
   * @throws RuntimeException with the reason, if orphan directories cannot be found safely
   */
  @VisibleForTesting
  public List<DirectoryMetadata> getOrphanTableDirectories(String dbName) {
    if (!databaseTableFilter.applyDatabaseName(dbName)) {
      return Collections.emptyList();
    }
    Path dbPath = new Path(storageClient.getRootPath(), dbName);
    // List before reading table metadata, so a table created in between cannot look orphaned.
    List<DirectoryMetadata> allTableDirectories = storageClient.getSubDirectoriesWithOwners(dbPath);
    if (allTableDirectories.isEmpty()) {
      return Collections.emptyList();
    }
    Set<String> directoriesInUse;
    try {
      directoriesInUse = getTableDirectoryNamesInUse(dbName);
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "Cannot determine which table directories are in use: " + e.getMessage(), e);
    }
    if (allTableDirectories.stream()
        .noneMatch(directory -> directoriesInUse.contains(directory.getBaseName()))) {
      throw new IllegalStateException(
          String.format(
              "None of the %d directories under %s is used by a table",
              allTableDirectories.size(), dbPath));
    }
    List<DirectoryMetadata> orphanTableDirectories =
        allTableDirectories.stream()
            .filter(directory -> isTableDirectoryName(directory.getBaseName()))
            .filter(
                directory -> databaseTableFilter.applyTableDirectoryPath(directory.getBaseName()))
            .filter(directory -> !directoriesInUse.contains(directory.getBaseName()))
            .collect(Collectors.toList());
    log.info(
        "Database {}: {} directories, {} table directories in use, {} orphan table directories",
        dbName,
        allTableDirectories.size(),
        directoriesInUse.size(),
        orphanTableDirectories.size());
    return orphanTableDirectories;
  }

  /**
   * Get all orphan table directories in the corresponding file system.
   *
   * <p>A database whose orphan directories cannot be found safely is skipped, and the reason is
   * logged. Throws if the storage root is not a directory, if no databases are listed, or if every
   * database checked is skipped, since then the run cannot be trusted at all.
   *
   * @return a list of DirectoryMetadata that are not registered
   * @throws IllegalStateException if the run cannot find orphan directories in any database
   */
  public List<DirectoryMetadata> getOrphanTableDirectories() {
    Path rootPath = new Path(storageClient.getRootPath());
    if (!storageClient.isDirectory(rootPath)) {
      throw new IllegalStateException("Storage root " + rootPath + " is not a directory");
    }
    // we use getDatabases interface to avoid accidentally deleting essential directories on the
    // database level
    List<String> databases = getDatabases();
    if (databases.isEmpty()) {
      // getDatabases returns an empty list when the lookup fails, so this cannot be trusted.
      throw new IllegalStateException("No databases found");
    }
    List<DirectoryMetadata> orphanTableDirectories = new ArrayList<>();
    Map<String, String> skippedDatabases = new LinkedHashMap<>();
    int checkedDatabases = 0;
    for (String dbName : databases) {
      if (!databaseTableFilter.applyDatabaseName(dbName)) {
        continue;
      }
      checkedDatabases++;
      try {
        orphanTableDirectories.addAll(getOrphanTableDirectories(dbName));
      } catch (RuntimeException e) {
        log.error(
            "Skipping orphan directory detection for database {}: {}", dbName, e.getMessage(), e);
        skippedDatabases.put(dbName, e.getMessage());
      }
    }
    log.info(
        "Orphan directory detection: checked {} databases, skipped {} {}, found {} orphan table directories",
        checkedDatabases,
        skippedDatabases.size(),
        skippedDatabases.keySet(),
        orphanTableDirectories.size());
    if (checkedDatabases > 0 && skippedDatabases.size() == checkedDatabases) {
      throw new IllegalStateException("Every database checked was skipped: " + skippedDatabases);
    }
    return orphanTableDirectories;
  }

  /**
   * Re-checks that a directory returned by {@link #getOrphanTableDirectories()} is still orphaned,
   * right before acting on it. Looks up the live and soft-deleted tables and the view named like
   * the directory, so it does not find tables renamed since they were created; {@link
   * #getOrphanTableDirectories(String)} protects those.
   *
   * <p>Fails closed: returns false if the directory is not named like a table directory or if a
   * lookup fails.
   *
   * @param tableDirectoryPath table directory path, {@code <database path>/<tableId>-<tableUUID>}
   * @return true if no live or soft-deleted table or view named like the directory uses it
   */
  public boolean isOrphanTableDirectory(Path tableDirectoryPath) {
    String directoryName = tableDirectoryPath.getName();
    Matcher matcher = TABLE_DIRECTORY_NAME_PATTERN.matcher(directoryName);
    if (!matcher.matches() || tableDirectoryPath.getParent() == null) {
      log.error("Not a table directory: {}", tableDirectoryPath);
      return false;
    }
    String dbName = tableDirectoryPath.getParent().getName();
    String tableId = matcher.group(1);
    try {
      Optional<GetTableResponseBody> table = findTable(dbName, tableId);
      if (table.isPresent()
          && directoryName.equals(
              toTableDirectoryName(table.get().getTableLocation(), dbName, tableId))) {
        log.info("Directory {} is used by table {}.{}", tableDirectoryPath, dbName, tableId);
        return false;
      }
      if (getSoftDeletedTableDirectoryNames(dbName, tableId).contains(directoryName)) {
        log.info(
            "Directory {} is used by soft-deleted table {}.{}",
            tableDirectoryPath,
            dbName,
            tableId);
        return false;
      }
      Optional<GetViewResponseBody> view = findView(dbName, tableId);
      if (view.isPresent()
          && directoryName.equals(
              toTableDirectoryName(view.get().getMetadataLocation(), dbName, tableId))) {
        log.info("Directory {} is used by view {}.{}", tableDirectoryPath, dbName, tableId);
        return false;
      }
      return true;
    } catch (RuntimeException e) {
      log.error("Cannot verify that directory {} is orphaned", tableDirectoryPath, e);
      return false;
    }
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

  /**
   * Returns the names of the directories used by live and soft-deleted tables and by views of the
   * given database. Throws if any lookup fails or returns an incomplete answer.
   */
  private Set<String> getTableDirectoryNamesInUse(String dbName) {
    Set<String> directoryNames = getLiveTableDirectoryNames(dbName);
    if (directoryNames.isEmpty()) {
      // An empty result means the lookup is not reliable. A database holding only views is skipped
      // too.
      throw new IllegalStateException("No live tables found for database " + dbName);
    }
    directoryNames.addAll(getSoftDeletedTableDirectoryNames(dbName, null));
    // Views are stored like tables, in <viewId>-<viewUUID> directories.
    directoryNames.addAll(getViewDirectoryNames(dbName));
    // Read live tables again: a table restored while soft-deleted tables were being read is in
    // neither earlier result.
    directoryNames.addAll(getLiveTableDirectoryNames(dbName));
    return directoryNames;
  }

  private Set<String> getLiveTableDirectoryNames(String dbName) {
    List<GetTableResponseBody> tables =
        readAllPages(
            "tables of database " + dbName,
            page -> {
              GetAllTablesResponseBody response =
                  retryTemplate.execute(
                      (RetryCallback<GetAllTablesResponseBody, RuntimeException>)
                          context ->
                              tableApi
                                  .searchTablesPaginatedV2(
                                      dbName, page, LISTING_PAGE_SIZE, null, TABLE_LOCATION_FIELD)
                                  .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS)));
              PageGetTableResponseBody pageResults =
                  response == null ? null : response.getPageResults();
              return pageResults == null
                  ? null
                  : new ListingPage<>(
                      pageResults.getContent(),
                      pageResults.getTotalElements(),
                      pageResults.getTotalPages());
            },
            table -> toTableDirectoryName(table.getTableLocation(), dbName, table.getTableId()));
    Set<String> directoryNames = new HashSet<>();
    for (GetTableResponseBody table : tables) {
      directoryNames.add(
          toTableDirectoryName(table.getTableLocation(), dbName, table.getTableId()));
    }
    return directoryNames;
  }

  /**
   * Returns the table, or empty if it does not exist. Throws if the lookup fails, so that a failure
   * is not mistaken for a missing table.
   */
  private Optional<GetTableResponseBody> findTable(String dbName, String tableId) {
    GetTableResponseBody table;
    try {
      table =
          retryTemplate.execute(
              (RetryCallback<GetTableResponseBody, RuntimeException>)
                  context ->
                      tableApi
                          .getTableV1(dbName, tableId)
                          .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS)));
    } catch (WebClientResponseException.NotFound e) {
      return Optional.empty();
    }
    if (table == null) {
      throw new IllegalStateException(String.format("Cannot load table %s.%s", dbName, tableId));
    }
    return Optional.of(table);
  }

  /**
   * Returns the view, or empty if it does not exist or views are turned off for the database.
   * Throws if the lookup fails.
   */
  private Optional<GetViewResponseBody> findView(String dbName, String viewId) {
    GetViewResponseBody view;
    try {
      view =
          retryTemplate.execute(
              (RetryCallback<GetViewResponseBody, RuntimeException>)
                  context ->
                      viewApi
                          .getViewV1(dbName, viewId)
                          .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS)));
    } catch (WebClientResponseException.NotFound e) {
      return Optional.empty();
    }
    if (view == null) {
      throw new IllegalStateException(String.format("Cannot load view %s.%s", dbName, viewId));
    }
    return Optional.of(view);
  }

  /**
   * Returns the names of the directories used by soft-deleted tables of the given database,
   * optionally only those with the given table ID.
   */
  private Set<String> getSoftDeletedTableDirectoryNames(String dbName, String tableId) {
    List<GetSoftDeletedTableResponseBody> tables =
        readAllPages(
            "soft-deleted tables of database " + dbName,
            page -> {
              GetAllSoftDeletedTablesResponseBody response =
                  retryTemplate.execute(
                      (RetryCallback<GetAllSoftDeletedTablesResponseBody, RuntimeException>)
                          context ->
                              tableApi
                                  .searchSoftDeletedTablesV1(
                                      dbName, tableId, page, LISTING_PAGE_SIZE, null)
                                  .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS)));
              PageGetSoftDeletedTableResponseBody pageResults =
                  response == null ? null : response.getPageResults();
              return pageResults == null
                  ? null
                  : new ListingPage<>(
                      pageResults.getContent(),
                      pageResults.getTotalElements(),
                      pageResults.getTotalPages());
            },
            table -> toTableDirectoryName(table.getTableLocation(), dbName, table.getTableId()));
    Set<String> directoryNames = new HashSet<>();
    for (GetSoftDeletedTableResponseBody table : tables) {
      directoryNames.add(
          toTableDirectoryName(table.getTableLocation(), dbName, table.getTableId()));
    }
    return directoryNames;
  }

  /**
   * Returns the names of the directories used by views of the given database, or none if views are
   * turned off for it. The view listing gives no total to check against, so a view could be missed
   * if views are dropped between pages: the listing is read until two reads in a row agree.
   */
  private Set<String> getViewDirectoryNames(String dbName) {
    Optional<Set<String>> previous = readViewDirectoryNames(dbName);
    if (!previous.isPresent()) {
      return Collections.emptySet();
    }
    for (int attempt = 1; attempt <= MAX_LISTING_ATTEMPTS; attempt++) {
      Optional<Set<String>> current = readViewDirectoryNames(dbName);
      if (current.isPresent() && current.equals(previous)) {
        return current.get();
      }
      log.warn(
          "views of database {} changed while being read (attempt {} of {})",
          dbName,
          attempt,
          MAX_LISTING_ATTEMPTS);
      previous = current;
    }
    throw new IllegalStateException(
        "views of database " + dbName + " kept changing while being read");
  }

  /** Reads every page of the view listing. Returns empty if views are turned off. */
  private Optional<Set<String>> readViewDirectoryNames(String dbName) {
    Set<String> directoryNames = new HashSet<>();
    String pageToken = null;
    do {
      String requestedPageToken = pageToken;
      GetAllViewsResponseBody response;
      try {
        response =
            retryTemplate.execute(
                (RetryCallback<GetAllViewsResponseBody, RuntimeException>)
                    context ->
                        viewApi
                            .getAllViewsV1(dbName, requestedPageToken, LISTING_PAGE_SIZE, null)
                            .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS)));
      } catch (WebClientResponseException.NotFound e) {
        if (requestedPageToken == null && isViewsDisabled(e)) {
          return Optional.empty();
        }
        throw e;
      }
      if (response == null || response.getResults() == null) {
        throw new IllegalStateException("Cannot read views of database " + dbName);
      }
      for (GetViewResponseBody view : response.getResults()) {
        directoryNames.add(
            toTableDirectoryName(view.getMetadataLocation(), dbName, view.getViewId()));
      }
      pageToken = response.getNextPageToken();
    } while (pageToken != null);
    return Optional.of(directoryNames);
  }

  private static boolean isViewsDisabled(WebClientResponseException.NotFound e) {
    try {
      return VIEWS_DISABLED_MESSAGE.equals(
          ERROR_BODY_MAPPER.readTree(e.getResponseBodyAsString()).path("message").asText());
    } catch (JsonProcessingException parseFailure) {
      return false;
    }
  }

  /** One page of a paginated listing. */
  @Value
  private static class ListingPage<T> {
    List<T> content;
    Long totalElements;
    Integer totalPages;
  }

  /**
   * Reads every page of a listing. Rows added, removed or moved between pages shift the remaining
   * pages, so a row could be missed: a listing that changes while being read is read again, and the
   * read fails if it keeps changing.
   *
   * @param toDirectoryName returns the table directory name of a row, which no two rows share
   */
  private static <T> List<T> readAllPages(
      String listing, IntFunction<ListingPage<T>> readPage, Function<T, String> toDirectoryName) {
    for (int attempt = 1; attempt <= MAX_LISTING_ATTEMPTS; attempt++) {
      Optional<List<T>> rows = tryReadAllPages(listing, readPage, toDirectoryName);
      if (rows.isPresent()) {
        return rows.get();
      }
      log.warn(
          "{} changed while being read (attempt {} of {})", listing, attempt, MAX_LISTING_ATTEMPTS);
    }
    throw new IllegalStateException(listing + " kept changing while being read");
  }

  /** Returns empty if rows were added, removed or moved while the pages were read. */
  private static <T> Optional<List<T>> tryReadAllPages(
      String listing, IntFunction<ListingPage<T>> readPage, Function<T, String> toDirectoryName) {
    List<T> rows = new ArrayList<>();
    Long expectedTotal = null;
    int page = 0;
    int totalPages;
    do {
      ListingPage<T> pageResults = readPage.apply(page);
      if (pageResults == null
          || pageResults.getContent() == null
          || pageResults.getTotalElements() == null
          || pageResults.getTotalPages() == null) {
        throw new IllegalStateException("Cannot read " + listing);
      }
      if (expectedTotal == null) {
        expectedTotal = pageResults.getTotalElements();
      } else if (!expectedTotal.equals(pageResults.getTotalElements())) {
        return Optional.empty();
      }
      rows.addAll(pageResults.getContent());
      totalPages = pageResults.getTotalPages();
      page++;
    } while (page < totalPages);
    if (rows.size() != expectedTotal) {
      return Optional.empty();
    }
    // A matching row count does not show that a row moved to another page: one row is then read
    // twice and another is missed. Pages are sorted by table ID, so a renamed table moves, and
    // soft-deleted tables with the same ID are in no fixed order.
    Set<String> directoryNames = new HashSet<>();
    for (T row : rows) {
      String directoryName = toDirectoryName.apply(row);
      if (!directoryNames.add(directoryName)) {
        log.warn("{}: table directory {} was read twice", listing, directoryName);
        return Optional.empty();
      }
    }
    return Optional.of(rows);
  }

  /**
   * Returns the table directory name for a table metadata location. OpenHouse writes metadata.json
   * directly under the table directory.
   */
  private static String toTableDirectoryName(
      String metadataLocation, String dbName, String tableId) {
    Path tableDirectory =
        metadataLocation != null && metadataLocation.endsWith(METADATA_FILE_SUFFIX)
            ? new Path(metadataLocation).getParent()
            : null;
    if (tableDirectory == null || tableDirectory.getName().isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "Unexpected location for table %s.%s: %s", dbName, tableId, metadataLocation));
    }
    return tableDirectory.getName();
  }

  private static boolean isTableDirectoryName(String directoryName) {
    return TABLE_DIRECTORY_NAME_PATTERN.matcher(directoryName).matches();
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
