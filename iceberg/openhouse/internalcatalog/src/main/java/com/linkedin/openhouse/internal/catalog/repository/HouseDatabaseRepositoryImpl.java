package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.housetables.client.api.DatabaseApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.model.CreateDatabaseRequestBody;
import com.linkedin.openhouse.housetables.client.model.GetAllEntityResponseBodyUserTable;
import com.linkedin.openhouse.housetables.client.model.PageUserTable;
import com.linkedin.openhouse.housetables.client.model.UpdateDatabasePropertiesRequestBody;
import com.linkedin.openhouse.internal.catalog.model.DatabaseMetadata;
import com.linkedin.openhouse.internal.catalog.model.DatabasePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException.Reason;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/** Uses bounded HouseTables requests with automatic DatabaseApi transport retries disabled. */
@RequiredArgsConstructor
public class HouseDatabaseRepositoryImpl implements HouseDatabaseRepository {
  private static final Duration READ_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(60);

  private final DatabaseApi databaseApi;
  private final UserTableApi userTableApi;

  @Override
  public DatabaseMetadata create(String databaseName, Map<String, String> properties)
      throws HouseDatabaseException {
    com.linkedin.openhouse.housetables.client.model.DatabaseMetadata result =
        execute(
                () ->
                    databaseApi.createDatabase(
                        new CreateDatabaseRequestBody()
                            .databaseName(databaseName)
                            .properties(properties)),
                WRITE_TIMEOUT,
                Reason.ALREADY_EXISTS)
            .orElseThrow(() -> new HouseDatabaseException(Reason.UNKNOWN_OUTCOME));
    return new DatabaseMetadata(
        Optional.ofNullable(result.getDatabaseUuid())
            .orElseThrow(() -> new HouseDatabaseException(Reason.UNKNOWN_OUTCOME)),
        Optional.ofNullable(result.getDatabaseName())
            .orElseThrow(() -> new HouseDatabaseException(Reason.UNKNOWN_OUTCOME)),
        Map.copyOf(
            Optional.ofNullable(result.getProperties())
                .filter(propertiesMap -> propertiesMap.values().stream().noneMatch(Objects::isNull))
                .orElseThrow(() -> new HouseDatabaseException(Reason.UNKNOWN_OUTCOME))));
  }

  @Override
  public Optional<DatabaseMetadata> findByName(String databaseName) throws HouseDatabaseException {
    try {
      com.linkedin.openhouse.housetables.client.model.DatabaseMetadata result =
          execute(
                  () -> databaseApi.getDatabase(databaseName),
                  READ_TIMEOUT,
                  Reason.CONCURRENT_MODIFICATION)
              .orElseThrow(() -> new HouseDatabaseException(Reason.FAILURE));
      return Optional.of(
          new DatabaseMetadata(
              Optional.ofNullable(result.getDatabaseUuid())
                  .orElseThrow(() -> new HouseDatabaseException(Reason.FAILURE)),
              Optional.ofNullable(result.getDatabaseName())
                  .orElseThrow(() -> new HouseDatabaseException(Reason.FAILURE)),
              Map.copyOf(
                  Optional.ofNullable(result.getProperties())
                      .filter(
                          propertiesMap ->
                              propertiesMap.values().stream().noneMatch(Objects::isNull))
                      .orElseThrow(() -> new HouseDatabaseException(Reason.FAILURE)))));
    } catch (HouseDatabaseException exception) {
      if (exception.getReason() == Reason.NOT_FOUND) {
        return Optional.empty();
      }
      throw exception;
    }
  }

  @Override
  public void delete(String databaseName) throws HouseDatabaseException {
    execute(
            () -> databaseApi.deleteDatabaseWithHttpInfo(databaseName),
            WRITE_TIMEOUT,
            Reason.CONCURRENT_MODIFICATION)
        .filter(response -> response.getStatusCodeValue() == 204)
        .orElseThrow(() -> new HouseDatabaseException(Reason.UNKNOWN_OUTCOME));
  }

  @Override
  public DatabasePropertyUpdate updateProperties(
      String databaseName, Map<String, String> updates, Set<String> removals)
      throws HouseDatabaseException {
    com.linkedin.openhouse.housetables.client.model.UpdateDatabasePropertiesResponseBody result =
        execute(
                () ->
                    databaseApi.updateDatabaseProperties(
                        databaseName,
                        new UpdateDatabasePropertiesRequestBody()
                            .updates(updates)
                            .removals(removals)),
                WRITE_TIMEOUT,
                Reason.CONCURRENT_MODIFICATION)
            .orElseThrow(() -> new HouseDatabaseException(Reason.UNKNOWN_OUTCOME));
    return new DatabasePropertyUpdate(
        java.util.List.copyOf(
            Optional.ofNullable(result.getUpdated())
                .filter(keys -> keys.stream().noneMatch(Objects::isNull))
                .orElseThrow(() -> new HouseDatabaseException(Reason.UNKNOWN_OUTCOME))),
        java.util.List.copyOf(
            Optional.ofNullable(result.getRemoved())
                .filter(keys -> keys.stream().noneMatch(Objects::isNull))
                .orElseThrow(() -> new HouseDatabaseException(Reason.UNKNOWN_OUTCOME))),
        java.util.List.copyOf(
            Optional.ofNullable(result.getMissing())
                .filter(keys -> keys.stream().noneMatch(Objects::isNull))
                .orElseThrow(() -> new HouseDatabaseException(Reason.UNKNOWN_OUTCOME))));
  }

  @Override
  public boolean containsEntities(String databaseName) throws HouseDatabaseException {
    Map<String, String> filters = Collections.singletonMap("databaseId", databaseName);
    // Each query returns at most one HouseTables row, without loading Iceberg metadata files.
    try {
      Optional<GetAllEntityResponseBodyUserTable> tables =
          execute(
              () -> userTableApi.getPaginatedUserTables(filters, 0, 1, "tableId"),
              READ_TIMEOUT,
              Reason.CONCURRENT_MODIFICATION);
      if (!tables
          .map(GetAllEntityResponseBodyUserTable::getPageResults)
          .map(PageUserTable::getContent)
          .orElseThrow(() -> new HouseDatabaseException(Reason.FAILURE))
          .isEmpty()) {
        return true;
      }
      return !execute(
              () -> userTableApi.getPaginatedUserViews(filters, 0, 1, "tableId"),
              READ_TIMEOUT,
              Reason.CONCURRENT_MODIFICATION)
          .map(GetAllEntityResponseBodyUserTable::getPageResults)
          .map(PageUserTable::getContent)
          .orElseThrow(() -> new HouseDatabaseException(Reason.FAILURE))
          .isEmpty();
    } catch (HouseDatabaseException exception) {
      // Listing availability is required to establish that the namespace is empty.
      if (exception.getReason() == Reason.NOT_FOUND) {
        throw new HouseDatabaseException(Reason.UNAVAILABLE, exception);
      }
      throw exception;
    }
  }

  private <T> Optional<T> execute(
      Supplier<Mono<T>> request, Duration timeout, Reason conflictReason)
      throws HouseDatabaseException {
    Reason uncertainFailure =
        timeout.equals(WRITE_TIMEOUT) ? Reason.UNKNOWN_OUTCOME : Reason.UNAVAILABLE;
    java.util.concurrent.CompletableFuture<T> pending =
        Mono.defer(request).timeout(timeout).toFuture();
    try {
      return Optional.ofNullable(pending.get());
    } catch (InterruptedException exception) {
      pending.cancel(true);
      Thread.currentThread().interrupt();
      throw new HouseDatabaseException(uncertainFailure, exception);
    } catch (ExecutionException
        | WebClientResponseException
        | WebClientRequestException exception) {
      Throwable cause = exception instanceof ExecutionException ? exception.getCause() : exception;
      if (cause instanceof WebClientResponseException) {
        switch (((WebClientResponseException) cause).getRawStatusCode()) {
          case 400:
            throw new HouseDatabaseException(Reason.BAD_REQUEST, cause);
          case 404:
            throw new HouseDatabaseException(Reason.NOT_FOUND, cause);
          case 409:
            throw new HouseDatabaseException(conflictReason, cause);
          case 422:
            throw new HouseDatabaseException(Reason.INVALID_PROPERTIES, cause);
          case 401:
          case 403:
          case 503:
            throw new HouseDatabaseException(Reason.UNAVAILABLE, cause);
          case 502:
          case 504:
            throw new HouseDatabaseException(uncertainFailure, cause);
          default:
            throw new HouseDatabaseException(
                timeout.equals(WRITE_TIMEOUT) ? Reason.UNKNOWN_OUTCOME : Reason.FAILURE, cause);
        }
      }
      if (cause instanceof WebClientRequestException || cause instanceof TimeoutException) {
        throw new HouseDatabaseException(uncertainFailure, cause);
      }
      throw new HouseDatabaseException(
          timeout.equals(WRITE_TIMEOUT) ? Reason.UNKNOWN_OUTCOME : Reason.FAILURE, cause);
    }
  }
}
