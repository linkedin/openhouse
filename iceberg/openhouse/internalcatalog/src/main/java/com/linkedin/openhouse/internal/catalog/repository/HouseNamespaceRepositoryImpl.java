package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.housetables.client.api.NamespaceApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.model.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.client.model.GetAllEntityResponseBodyUserTable;
import com.linkedin.openhouse.housetables.client.model.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.client.model.PageUserTable;
import com.linkedin.openhouse.housetables.client.model.UpdateNamespacePropertiesRequestBody;
import com.linkedin.openhouse.housetables.client.model.UpdateNamespacePropertiesResponseBody;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceException.Reason;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/** Uses bounded HouseTables requests with automatic NamespaceApi transport retries disabled. */
@RequiredArgsConstructor
public class HouseNamespaceRepositoryImpl implements HouseNamespaceRepository {
  private static final Duration READ_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(60);

  private final NamespaceApi namespaceApi;
  private final UserTableApi userTableApi;

  @Override
  public NamespaceMetadata create(String namespaceName, Map<String, String> properties)
      throws HouseNamespaceException {
    NamespaceMetadataResponse result =
        execute(
                () ->
                    namespaceApi.createNamespace(
                        new CreateNamespaceRequestBody()
                            .namespaceName(namespaceName)
                            .properties(properties)),
                WRITE_TIMEOUT,
                Reason.ALREADY_EXISTS)
            .orElseThrow(() -> new HouseNamespaceException(Reason.UNKNOWN_OUTCOME));
    return new NamespaceMetadata(
        Optional.ofNullable(result.getNamespaceUuid())
            .orElseThrow(() -> new HouseNamespaceException(Reason.UNKNOWN_OUTCOME)),
        Optional.ofNullable(result.getNamespaceName())
            .orElseThrow(() -> new HouseNamespaceException(Reason.UNKNOWN_OUTCOME)),
        Map.copyOf(
            Optional.ofNullable(result.getProperties())
                .filter(propertiesMap -> propertiesMap.values().stream().noneMatch(Objects::isNull))
                .orElseThrow(() -> new HouseNamespaceException(Reason.UNKNOWN_OUTCOME))));
  }

  @Override
  public Optional<NamespaceMetadata> findByName(String namespaceName)
      throws HouseNamespaceException {
    try {
      NamespaceMetadataResponse result =
          execute(
                  () -> namespaceApi.getNamespace(namespaceName),
                  READ_TIMEOUT,
                  Reason.CONCURRENT_MODIFICATION)
              .orElseThrow(() -> new HouseNamespaceException(Reason.FAILURE));
      return Optional.of(
          new NamespaceMetadata(
              Optional.ofNullable(result.getNamespaceUuid())
                  .orElseThrow(() -> new HouseNamespaceException(Reason.FAILURE)),
              Optional.ofNullable(result.getNamespaceName())
                  .orElseThrow(() -> new HouseNamespaceException(Reason.FAILURE)),
              Map.copyOf(
                  Optional.ofNullable(result.getProperties())
                      .filter(
                          propertiesMap ->
                              propertiesMap.values().stream().noneMatch(Objects::isNull))
                      .orElseThrow(() -> new HouseNamespaceException(Reason.FAILURE)))));
    } catch (HouseNamespaceException exception) {
      if (exception.getReason() == Reason.NOT_FOUND) {
        return Optional.empty();
      }
      throw exception;
    }
  }

  @Override
  public void delete(String namespaceName) throws HouseNamespaceException {
    execute(
            () -> namespaceApi.deleteNamespaceWithHttpInfo(namespaceName),
            WRITE_TIMEOUT,
            Reason.CONCURRENT_MODIFICATION)
        .filter(response -> response.getStatusCodeValue() == 204)
        .orElseThrow(() -> new HouseNamespaceException(Reason.UNKNOWN_OUTCOME));
  }

  @Override
  public NamespacePropertyUpdate updateProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws HouseNamespaceException {
    UpdateNamespacePropertiesResponseBody result =
        execute(
                () ->
                    namespaceApi.updateNamespaceProperties(
                        namespaceName,
                        new UpdateNamespacePropertiesRequestBody()
                            .updates(updates)
                            .removals(removals)),
                WRITE_TIMEOUT,
                Reason.CONCURRENT_MODIFICATION)
            .orElseThrow(() -> new HouseNamespaceException(Reason.UNKNOWN_OUTCOME));
    return new NamespacePropertyUpdate(
        List.copyOf(
            Optional.ofNullable(result.getUpdated())
                .filter(keys -> keys.stream().noneMatch(Objects::isNull))
                .orElseThrow(() -> new HouseNamespaceException(Reason.UNKNOWN_OUTCOME))),
        List.copyOf(
            Optional.ofNullable(result.getRemoved())
                .filter(keys -> keys.stream().noneMatch(Objects::isNull))
                .orElseThrow(() -> new HouseNamespaceException(Reason.UNKNOWN_OUTCOME))),
        List.copyOf(
            Optional.ofNullable(result.getMissing())
                .filter(keys -> keys.stream().noneMatch(Objects::isNull))
                .orElseThrow(() -> new HouseNamespaceException(Reason.UNKNOWN_OUTCOME))));
  }

  @Override
  public boolean containsEntities(String namespaceName) throws HouseNamespaceException {
    Map<String, String> filters = Collections.singletonMap("databaseId", namespaceName);
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
          .orElseThrow(() -> new HouseNamespaceException(Reason.FAILURE))
          .isEmpty()) {
        return true;
      }
      return !execute(
              () -> userTableApi.getPaginatedUserViews(filters, 0, 1, "tableId"),
              READ_TIMEOUT,
              Reason.CONCURRENT_MODIFICATION)
          .map(GetAllEntityResponseBodyUserTable::getPageResults)
          .map(PageUserTable::getContent)
          .orElseThrow(() -> new HouseNamespaceException(Reason.FAILURE))
          .isEmpty();
    } catch (HouseNamespaceException exception) {
      // Listing availability is required to establish that the namespace is empty.
      if (exception.getReason() == Reason.NOT_FOUND) {
        throw new HouseNamespaceException(Reason.UNAVAILABLE, exception);
      }
      throw exception;
    }
  }

  private <T> Optional<T> execute(
      Supplier<Mono<T>> request, Duration timeout, Reason conflictReason)
      throws HouseNamespaceException {
    Reason uncertainFailure =
        timeout.equals(WRITE_TIMEOUT) ? Reason.UNKNOWN_OUTCOME : Reason.UNAVAILABLE;
    CompletableFuture<T> pending = Mono.defer(request).timeout(timeout).toFuture();
    try {
      return Optional.ofNullable(pending.get());
    } catch (InterruptedException exception) {
      pending.cancel(true);
      Thread.currentThread().interrupt();
      throw new HouseNamespaceException(uncertainFailure, exception);
    } catch (ExecutionException
        | WebClientResponseException
        | WebClientRequestException exception) {
      Throwable cause = exception instanceof ExecutionException ? exception.getCause() : exception;
      if (cause instanceof WebClientResponseException) {
        switch (((WebClientResponseException) cause).getRawStatusCode()) {
          case 400:
            throw new HouseNamespaceException(Reason.BAD_REQUEST, cause);
          case 404:
            throw new HouseNamespaceException(Reason.NOT_FOUND, cause);
          case 409:
            throw new HouseNamespaceException(conflictReason, cause);
          case 422:
            throw new HouseNamespaceException(Reason.INVALID_PROPERTIES, cause);
          case 401:
          case 403:
          case 503:
            throw new HouseNamespaceException(Reason.UNAVAILABLE, cause);
          case 502:
          case 504:
            throw new HouseNamespaceException(uncertainFailure, cause);
          default:
            throw new HouseNamespaceException(
                timeout.equals(WRITE_TIMEOUT) ? Reason.UNKNOWN_OUTCOME : Reason.FAILURE, cause);
        }
      }
      if (cause instanceof WebClientRequestException || cause instanceof TimeoutException) {
        throw new HouseNamespaceException(uncertainFailure, cause);
      }
      throw new HouseNamespaceException(
          timeout.equals(WRITE_TIMEOUT) ? Reason.UNKNOWN_OUTCOME : Reason.FAILURE, cause);
    }
  }
}
