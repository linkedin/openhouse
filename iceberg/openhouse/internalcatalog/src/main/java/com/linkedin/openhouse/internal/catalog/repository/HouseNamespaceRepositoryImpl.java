package com.linkedin.openhouse.internal.catalog.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
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
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.Exceptions;
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
      throws DuplicateCreationException {
    NamespaceMetadataResponse result;
    try {
      result =
          execute(
                  () ->
                      namespaceApi.createNamespace(
                          new CreateNamespaceRequestBody()
                              .namespaceName(namespaceName)
                              .properties(properties)),
                  WRITE_TIMEOUT)
              .orElseThrow(
                  () -> new IllegalStateException("Namespace metadata response is unusable"));
    } catch (WebClientResponseException exception) {
      if (exception.getRawStatusCode() == 409 && isError(exception, "AlreadyExistsException")) {
        throw new DuplicateCreationException("Namespace already exists", exception);
      }
      throw exception;
    }
    if (!namespaceName.equalsIgnoreCase(result.getNamespaceName())) {
      throw new IllegalStateException("Namespace metadata response is unusable");
    }
    return new NamespaceMetadata(
        result.getNamespaceUuid(), result.getNamespaceName(), Map.copyOf(result.getProperties()));
  }

  @Override
  public NamespaceMetadata getByName(String namespaceName) throws NotFoundException {
    NamespaceMetadataResponse result =
        executeExisting(() -> namespaceApi.getNamespace(namespaceName), READ_TIMEOUT)
            .orElseThrow(
                () -> new IllegalStateException("Namespace metadata response is unusable"));
    if (!namespaceName.equalsIgnoreCase(result.getNamespaceName())) {
      throw new IllegalStateException("Namespace metadata response is unusable");
    }
    return new NamespaceMetadata(
        result.getNamespaceUuid(), result.getNamespaceName(), Map.copyOf(result.getProperties()));
  }

  @Override
  public void delete(String namespaceName) throws NotFoundException {
    executeExisting(() -> namespaceApi.deleteNamespaceWithHttpInfo(namespaceName), WRITE_TIMEOUT)
        .filter(response -> response.getStatusCodeValue() == 204)
        .orElseThrow(() -> new IllegalStateException("Namespace deletion response is unusable"));
  }

  @Override
  public NamespacePropertyUpdate updateProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws NotFoundException {
    UpdateNamespacePropertiesResponseBody result =
        executeExisting(
                () ->
                    namespaceApi.updateNamespaceProperties(
                        namespaceName,
                        new UpdateNamespacePropertiesRequestBody()
                            .updates(updates)
                            .removals(removals)),
                WRITE_TIMEOUT)
            .orElseThrow(
                () -> new IllegalStateException("Namespace property update response is unusable"));
    Set<String> acknowledgedRemovals =
        Stream.concat(result.getRemoved().stream(), result.getMissing().stream())
            .collect(Collectors.toSet());
    if (!new HashSet<>(result.getUpdated()).equals(updates.keySet())
        || result.getUpdated().size() != updates.size()
        || !acknowledgedRemovals.equals(removals)
        || result.getRemoved().size() + result.getMissing().size() != acknowledgedRemovals.size()) {
      throw new IllegalStateException("Namespace property update response is unusable");
    }
    return new NamespacePropertyUpdate(
        List.copyOf(result.getUpdated()),
        List.copyOf(result.getRemoved()),
        List.copyOf(result.getMissing()));
  }

  @Override
  public boolean containsEntities(String namespaceName) {
    Map<String, String> filters = Collections.singletonMap("databaseId", namespaceName);
    // Each query returns at most one HouseTables row, without loading Iceberg metadata files.
    Optional<GetAllEntityResponseBodyUserTable> tables =
        execute(() -> userTableApi.getPaginatedUserTables(filters, 0, 1, "tableId"), READ_TIMEOUT);
    if (!tables
        .map(GetAllEntityResponseBodyUserTable::getPageResults)
        .map(PageUserTable::getContent)
        .orElseThrow(() -> new IllegalStateException("HouseTables listing response is unusable"))
        .isEmpty()) {
      return true;
    }
    return !execute(
            () -> userTableApi.getPaginatedUserViews(filters, 0, 1, "tableId"), READ_TIMEOUT)
        .map(GetAllEntityResponseBodyUserTable::getPageResults)
        .map(PageUserTable::getContent)
        .orElseThrow(() -> new IllegalStateException("HouseTables listing response is unusable"))
        .isEmpty();
  }

  private <T> Optional<T> executeExisting(Supplier<Mono<T>> request, Duration timeout)
      throws NotFoundException {
    try {
      return execute(request, timeout);
    } catch (WebClientResponseException exception) {
      if (exception.getRawStatusCode() == 404 && isError(exception, "NotFoundException")) {
        throw new NotFoundException("Namespace does not exist", exception);
      }
      throw exception;
    }
  }

  private boolean isError(WebClientResponseException exception, String expectedType) {
    try {
      return Optional.ofNullable(new ObjectMapper().readTree(exception.getResponseBodyAsString()))
          .filter(
              body ->
                  body.isObject()
                      && body.path("status").isTextual()
                      && HttpStatus.valueOf(exception.getRawStatusCode())
                          .name()
                          .equals(body.path("status").textValue())
                      && body.path("error").isTextual()
                      && body.path("message").isTextual()
                      && body.path("type").isTextual())
          .map(body -> expectedType.equals(body.path("type").textValue()))
          .orElse(false);
    } catch (JsonProcessingException decodingFailure) {
      exception.addSuppressed(decodingFailure);
      return false;
    }
  }

  private <T> Optional<T> execute(Supplier<Mono<T>> request, Duration timeout) {
    CompletableFuture<T> pending = Mono.defer(request).timeout(timeout).toFuture();
    try {
      return Optional.ofNullable(pending.get());
    } catch (InterruptedException exception) {
      pending.cancel(true);
      Thread.currentThread().interrupt();
      throw new IllegalStateException("HouseTables namespace operation interrupted", exception);
    } catch (ExecutionException exception) {
      throw Exceptions.propagate(exception.getCause());
    }
  }
}
