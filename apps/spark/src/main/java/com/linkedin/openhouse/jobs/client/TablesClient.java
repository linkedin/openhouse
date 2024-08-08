package com.linkedin.openhouse.jobs.client;

import com.linkedin.openhouse.jobs.util.DatabaseTableFilter;
import com.linkedin.openhouse.jobs.util.DirectoryMetadata;
import com.linkedin.openhouse.jobs.util.RetentionConfig;
import com.linkedin.openhouse.jobs.util.RetryUtil;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.tables.client.api.DatabaseApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.model.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetDatabaseResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.Policies;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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

/**
 * A read-only client for interacting with /tables service. Supports limited operations necessary
 * for reading tables metadata for scheduling.
 */
@Slf4j
@AllArgsConstructor
public class TablesClient {
  private static final int REQUEST_TIMEOUT_SECONDS = 60;
  private final RetryTemplate retryTemplate;
  private final TableApi tableApi;
  private final DatabaseApi databaseApi;
  private final DatabaseTableFilter databaseFilter;
  @VisibleForTesting private final StorageClient storageClient;

  public Optional<RetentionConfig> getTableRetention(TableMetadata tableMetadata) {
    GetTableResponseBody response = getTable(tableMetadata);
    return getTableRetention(response);
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
    String columnName = "";
    String columnPattern = "";
    if (response.getTimePartitioning() != null) {
      columnName = response.getTimePartitioning().getColumnName();
    } else {
      columnName = policies.getRetention().getColumnPattern().getColumnName();
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

  protected GetTableResponseBody getTable(TableMetadata tableMetadata) {
    return RetryUtil.executeWithRetry(
        retryTemplate,
        (RetryCallback<GetTableResponseBody, Exception>)
            context ->
                tableApi
                    .getTableV1(tableMetadata.getDbName(), tableMetadata.getTableName())
                    .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS)),
        null);
  }

  /**
   * Checks if data layout strategies can be generated on the input table.
   *
   * @param tableMetadata table metadata
   * @return true if data layout strategies can be generated on the table, false otherwise
   */
  public boolean canRunDataLayoutStrategyGeneration(TableMetadata tableMetadata) {
    GetTableResponseBody response = getTable(tableMetadata);
    if (response == null || response.getTimePartitioning() == null) {
      return false;
    }
    return isPrimaryTable(tableMetadata);
  }

  /**
   * Checks if data compaction can be executed on the input table.
   *
   * @param tableMetadata table metadata
   * @return true if the table can run data compaction, false otherwise
   */
  public boolean canRunDataCompaction(TableMetadata tableMetadata) {
    return isPrimaryTable(tableMetadata);
  }

  /**
   * Checks if expire snapshots can be executed on the input table.
   *
   * @param tableMetadata table metadata
   * @return true if the table can expire snapshots, false otherwise
   */
  public boolean canExpireSnapshots(TableMetadata tableMetadata) {
    return isPrimaryTable(tableMetadata);
  }

  /**
   * Checks if retention can be executed on the input table.
   *
   * @param tableMetadata table metadata
   * @return true if the table can run retention, false otherwise
   */
  public boolean canRunRetention(TableMetadata tableMetadata) {
    GetTableResponseBody response = getTable(tableMetadata);

    if (response == null || !isPrimaryTable(response)) {
      return false;
    }
    Optional<RetentionConfig> config = getTableRetention(response);
    return config.isPresent();
  }

  private boolean isPrimaryTable(@NonNull GetTableResponseBody response) {
    return GetTableResponseBody.TableTypeEnum.PRIMARY_TABLE == response.getTableType();
  }

  private boolean isPrimaryTable(@NonNull TableMetadata tableMetadata) {
    GetTableResponseBody response = getTable(tableMetadata);
    return response != null && isPrimaryTable(response);
  }

  public List<TableMetadata> getTables() {
    List<TableMetadata> ret = new ArrayList<>();
    for (String dbName : getDatabases()) {
      if (databaseFilter.applyDatabaseName(dbName)) {
        ret.addAll(
            RetryUtil.executeWithRetry(
                retryTemplate,
                (RetryCallback<List<TableMetadata>, Exception>)
                    context -> {
                      GetAllTablesResponseBody response =
                          tableApi
                              .searchTablesV1(dbName)
                              .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
                      return Optional.ofNullable(response.getResults())
                          .map(Collection::stream)
                          .orElseGet(Stream::empty)
                          .map(this::mapTableResponseToTableMetadata)
                          .filter(databaseFilter::apply)
                          .collect(Collectors.toList());
                    },
                Collections.emptyList()));
      }
    }
    return ret;
  }

  /**
   * For the given database name, get all registered tables
   *
   * @param dbName database name
   * @return a set of registered table names
   */
  public Set<String> getTableNamesForDbName(String dbName) {
    Set<String> tableNames = new HashSet<>();
    if (databaseFilter.applyDatabaseName(dbName)) {
      tableNames.addAll(
          RetryUtil.executeWithRetry(
              retryTemplate,
              (RetryCallback<Set<String>, Exception>)
                  context -> {
                    GetAllTablesResponseBody response =
                        tableApi
                            .searchTablesV1(dbName)
                            .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
                    return Optional.ofNullable(response.getResults())
                        .map(Collection::stream)
                        .orElseGet(Stream::empty)
                        .map(this::mapTableResponseToTableDirectoryName)
                        .filter(databaseFilter::applyTableDirectoryPath)
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
                !registeredTableDirectories.contains(directoryMetadata.getDirectoryName()))
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
      if (databaseFilter.applyDatabaseName(dbName)) {
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

  protected TableMetadata mapTableResponseToTableMetadata(GetTableResponseBody responseBody) {
    TableMetadata metadata =
        TableMetadata.builder()
            .dbName(responseBody.getDatabaseId())
            .tableName(responseBody.getTableId())
            .build();
    String creator = getTable(metadata).getTableCreator();
    return TableMetadata.builder()
        .creator(creator)
        .dbName(responseBody.getDatabaseId())
        .tableName(responseBody.getTableId())
        .build();
  }

  private String mapTableResponseToTableDirectoryName(GetTableResponseBody responseBody) {
    TableMetadata metadata =
        TableMetadata.builder()
            .dbName(responseBody.getDatabaseId())
            .tableName(responseBody.getTableId())
            .build();
    String location = getTable(metadata).getTableLocation();
    return new Path(location).getParent().getName();
  }
}
