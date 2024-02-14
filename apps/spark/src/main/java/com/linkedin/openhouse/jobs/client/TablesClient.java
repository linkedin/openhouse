package com.linkedin.openhouse.jobs.client;

import com.linkedin.openhouse.jobs.util.DatabaseTableFilter;
import com.linkedin.openhouse.jobs.util.RetentionConfig;
import com.linkedin.openhouse.jobs.util.RetryUtil;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.tables.client.api.DatabaseApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.model.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.Policies;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;

/**
 * A read-only client for interacting with /tables service. Supports limited operations necessary
 * for reading tables metadata for scheduling.
 */
@AllArgsConstructor
public class TablesClient {
  private static final int REQUEST_TIMEOUT_SECONDS = 60;
  private final RetryTemplate retryTemplate;
  private final TableApi api;
  private final DatabaseApi databaseApi;
  private final DatabaseTableFilter filter;

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
    GetTableResponseBody response =
        RetryUtil.executeWithRetry(
            retryTemplate,
            (RetryCallback<GetTableResponseBody, Exception>)
                context ->
                    api.getTableV0(tableMetadata.getDbName(), tableMetadata.getTableName())
                        .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS)),
            null);
    return response;
  }

  /**
   * Checks if expire snapshots can be executed on the input table.
   *
   * @param tableMetadata table metadata
   * @return true if the table can expire snapshots, false otherwise
   */
  public boolean canExpireSnapshots(TableMetadata tableMetadata) {
    GetTableResponseBody response = getTable(tableMetadata);
    return (response != null && isPrimaryTable(response));
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

  public List<TableMetadata> getTables() {
    List<TableMetadata> ret = new ArrayList<>();
    for (String dbName : getDatabases()) {
      if (filter.applyDatabaseName(dbName)) {
        ret.addAll(
            RetryUtil.executeWithRetry(
                retryTemplate,
                (RetryCallback<List<TableMetadata>, Exception>)
                    context -> {
                      GetAllTablesResponseBody response =
                          api.getAllTablesV0(dbName)
                              .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
                      return Optional.ofNullable(response.getResults())
                          .map(Collection::stream)
                          .orElseGet(Stream::empty)
                          .map(this::parseGetTableResponse)
                          .filter(filter::apply)
                          .collect(Collectors.toList());
                    },
                Collections.emptyList()));
      }
    }
    return ret;
  }

  public List<String> getDatabases() {
    List<String> ret = new ArrayList<>();
    ret.addAll(
        RetryUtil.executeWithRetry(
            retryTemplate,
            (RetryCallback<List<String>, Exception>)
                context -> {
                  GetAllDatabasesResponseBody response =
                      databaseApi
                          .getAllDatabasesV0()
                          .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
                  return Optional.ofNullable(response.getResults())
                      .map(Collection::stream)
                      .orElseGet(Stream::empty)
                      .map(r -> r.getDatabaseId())
                      .collect(Collectors.toList());
                },
            Collections.emptyList()));
    return ret;
  }

  protected TableMetadata parseGetTableResponse(GetTableResponseBody responseBody) {
    return TableMetadata.builder()
        .dbName(responseBody.getDatabaseId())
        .tableName(responseBody.getTableId())
        .creator(responseBody.getTableCreator())
        .build();
  }
}
