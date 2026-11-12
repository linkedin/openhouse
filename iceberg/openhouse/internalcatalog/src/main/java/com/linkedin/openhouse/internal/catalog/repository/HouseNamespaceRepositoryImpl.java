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
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceAlreadyExistsException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceConcurrentModificationException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceInvalidPropertiesException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceInvalidRequestException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceMutationUncertainException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceResponseException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceUnavailableException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/** Uses bounded HouseTables requests with automatic NamespaceApi transport retries disabled. */
@RequiredArgsConstructor
public class HouseNamespaceRepositoryImpl implements HouseNamespaceRepository {
  private enum Operation {
    READ,
    MUTATION
  }

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
                Operation.MUTATION,
                HouseNamespaceAlreadyExistsException::new)
            .orElseThrow(HouseNamespaceMutationUncertainException::new);
    return new NamespaceMetadata(
        result.getNamespaceUuid(), result.getNamespaceName(), Map.copyOf(result.getProperties()));
  }

  @Override
  public Optional<NamespaceMetadata> findByName(String namespaceName)
      throws HouseNamespaceException {
    try {
      NamespaceMetadataResponse result =
          execute(
                  () -> namespaceApi.getNamespace(namespaceName),
                  READ_TIMEOUT,
                  Operation.READ,
                  HouseNamespaceConcurrentModificationException::new)
              .orElseThrow(HouseNamespaceResponseException::new);
      return Optional.of(
          new NamespaceMetadata(
              result.getNamespaceUuid(),
              result.getNamespaceName(),
              Map.copyOf(result.getProperties())));
    } catch (HouseNamespaceNotFoundException exception) {
      return Optional.empty();
    }
  }

  @Override
  public void delete(String namespaceName) throws HouseNamespaceException {
    execute(
            () -> namespaceApi.deleteNamespaceWithHttpInfo(namespaceName),
            WRITE_TIMEOUT,
            Operation.MUTATION,
            HouseNamespaceConcurrentModificationException::new)
        .filter(response -> response.getStatusCodeValue() == 204)
        .orElseThrow(HouseNamespaceMutationUncertainException::new);
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
                Operation.MUTATION,
                HouseNamespaceConcurrentModificationException::new)
            .orElseThrow(HouseNamespaceMutationUncertainException::new);
    Set<String> acknowledgedRemovals =
        Stream.concat(result.getRemoved().stream(), result.getMissing().stream())
            .collect(Collectors.toSet());
    if (!new HashSet<>(result.getUpdated()).equals(updates.keySet())
        || result.getUpdated().size() != updates.size()
        || !acknowledgedRemovals.equals(removals)
        || result.getRemoved().size() + result.getMissing().size() != acknowledgedRemovals.size()) {
      throw new HouseNamespaceMutationUncertainException();
    }
    return new NamespacePropertyUpdate(
        List.copyOf(result.getUpdated()),
        List.copyOf(result.getRemoved()),
        List.copyOf(result.getMissing()));
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
              Operation.READ,
              HouseNamespaceConcurrentModificationException::new);
      if (!tables
          .map(GetAllEntityResponseBodyUserTable::getPageResults)
          .map(PageUserTable::getContent)
          .orElseThrow(HouseNamespaceResponseException::new)
          .isEmpty()) {
        return true;
      }
      return !execute(
              () -> userTableApi.getPaginatedUserViews(filters, 0, 1, "tableId"),
              READ_TIMEOUT,
              Operation.READ,
              HouseNamespaceConcurrentModificationException::new)
          .map(GetAllEntityResponseBodyUserTable::getPageResults)
          .map(PageUserTable::getContent)
          .orElseThrow(HouseNamespaceResponseException::new)
          .isEmpty();
    } catch (HouseNamespaceNotFoundException exception) {
      // Listing availability is required to establish that the namespace is empty.
      throw new HouseNamespaceUnavailableException(exception);
    }
  }

  private <T> Optional<T> execute(
      Supplier<Mono<T>> request,
      Duration timeout,
      Operation operation,
      Function<Throwable, HouseNamespaceException> conflict)
      throws HouseNamespaceException {
    Function<Throwable, HouseNamespaceException> unavailable =
        operation == Operation.MUTATION
            ? HouseNamespaceMutationUncertainException::new
            : HouseNamespaceUnavailableException::new;
    Function<Throwable, HouseNamespaceException> unusable =
        operation == Operation.MUTATION
            ? HouseNamespaceMutationUncertainException::new
            : HouseNamespaceResponseException::new;
    CompletableFuture<T> pending = Mono.defer(request).timeout(timeout).toFuture();
    try {
      return Optional.ofNullable(pending.get());
    } catch (InterruptedException exception) {
      pending.cancel(true);
      Thread.currentThread().interrupt();
      throw unavailable.apply(exception);
    } catch (ExecutionException
        | WebClientResponseException
        | WebClientRequestException exception) {
      Throwable cause = exception instanceof ExecutionException ? exception.getCause() : exception;
      if (cause instanceof WebClientResponseException) {
        switch (((WebClientResponseException) cause).getRawStatusCode()) {
          case 400:
            throw new HouseNamespaceInvalidRequestException(cause);
          case 404:
            throw new HouseNamespaceNotFoundException(cause);
          case 409:
            throw conflict.apply(cause);
          case 422:
            throw new HouseNamespaceInvalidPropertiesException(cause);
          case 401:
          case 403:
          case 503:
            throw new HouseNamespaceUnavailableException(cause);
          case 502:
          case 504:
            throw unavailable.apply(cause);
          default:
            throw unusable.apply(cause);
        }
      }
      if (cause instanceof WebClientRequestException || cause instanceof TimeoutException) {
        throw unavailable.apply(cause);
      }
      throw unusable.apply(cause);
    }
  }
}
