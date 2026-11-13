package com.linkedin.openhouse.internal.catalog.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.common.exception.checked.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.checked.NamespaceException;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/** Uses bounded HouseTables requests with automatic NamespaceApi transport retries disabled. */
@RequiredArgsConstructor
public class HouseNamespaceRepositoryImpl implements HouseNamespaceRepository {
  private enum Operation {
    CREATE,
    LOAD,
    DROP,
    UPDATE,
    LIST
  }

  private static final Duration READ_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(60);

  private final NamespaceApi namespaceApi;
  private final UserTableApi userTableApi;

  @Override
  public NamespaceMetadata create(String namespaceName, Map<String, String> properties)
      throws NamespaceException {
    NamespaceMetadataResponse result =
        execute(
                () ->
                    namespaceApi.createNamespace(
                        new CreateNamespaceRequestBody()
                            .namespaceName(namespaceName)
                            .properties(properties)),
                WRITE_TIMEOUT,
                Operation.CREATE)
            .orElseThrow(() -> new NamespaceException("Namespace metadata response is unusable"));
    if (!namespaceName.equalsIgnoreCase(result.getNamespaceName())) {
      throw new NamespaceException("Namespace metadata response is unusable");
    }
    return new NamespaceMetadata(
        result.getNamespaceUuid(), result.getNamespaceName(), Map.copyOf(result.getProperties()));
  }

  @Override
  public NamespaceMetadata getByName(String namespaceName) throws NamespaceException {
    NamespaceMetadataResponse result =
        execute(() -> namespaceApi.getNamespace(namespaceName), READ_TIMEOUT, Operation.LOAD)
            .orElseThrow(() -> new NamespaceException("Namespace metadata response is unusable"));
    if (!namespaceName.equalsIgnoreCase(result.getNamespaceName())) {
      throw new NamespaceException("Namespace metadata response is unusable");
    }
    return new NamespaceMetadata(
        result.getNamespaceUuid(), result.getNamespaceName(), Map.copyOf(result.getProperties()));
  }

  @Override
  public void delete(String namespaceName) throws NamespaceException {
    execute(
            () -> namespaceApi.deleteNamespaceWithHttpInfo(namespaceName),
            WRITE_TIMEOUT,
            Operation.DROP)
        .filter(response -> response.getStatusCodeValue() == 204)
        .orElseThrow(() -> new NamespaceException("Namespace deletion response is unusable"));
  }

  @Override
  public NamespacePropertyUpdate updateProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws NamespaceException {
    UpdateNamespacePropertiesResponseBody result =
        execute(
                () ->
                    namespaceApi.updateNamespaceProperties(
                        namespaceName,
                        new UpdateNamespacePropertiesRequestBody()
                            .updates(updates)
                            .removals(removals)),
                WRITE_TIMEOUT,
                Operation.UPDATE)
            .orElseThrow(
                () -> new NamespaceException("Namespace property update response is unusable"));
    Set<String> acknowledgedRemovals =
        Stream.concat(result.getRemoved().stream(), result.getMissing().stream())
            .collect(Collectors.toSet());
    if (!new HashSet<>(result.getUpdated()).equals(updates.keySet())
        || result.getUpdated().size() != updates.size()
        || !acknowledgedRemovals.equals(removals)
        || result.getRemoved().size() + result.getMissing().size() != acknowledgedRemovals.size()) {
      throw new NamespaceException("Namespace property update response is unusable");
    }
    return new NamespacePropertyUpdate(
        List.copyOf(result.getUpdated()),
        List.copyOf(result.getRemoved()),
        List.copyOf(result.getMissing()));
  }

  @Override
  public boolean containsEntities(String namespaceName) throws NamespaceException {
    Map<String, String> filters = Collections.singletonMap("databaseId", namespaceName);
    // Each query returns at most one HouseTables row, without loading Iceberg metadata files.
    Optional<GetAllEntityResponseBodyUserTable> tables =
        execute(
            () -> userTableApi.getPaginatedUserTables(filters, 0, 1, "tableId"),
            READ_TIMEOUT,
            Operation.LIST);
    if (!tables
        .map(GetAllEntityResponseBodyUserTable::getPageResults)
        .map(PageUserTable::getContent)
        .orElseThrow(() -> new NamespaceException("HouseTables listing response is unusable"))
        .isEmpty()) {
      return true;
    }
    return !execute(
            () -> userTableApi.getPaginatedUserViews(filters, 0, 1, "tableId"),
            READ_TIMEOUT,
            Operation.LIST)
        .map(GetAllEntityResponseBodyUserTable::getPageResults)
        .map(PageUserTable::getContent)
        .orElseThrow(() -> new NamespaceException("HouseTables listing response is unusable"))
        .isEmpty();
  }

  private <T> Optional<T> execute(Supplier<Mono<T>> request, Duration timeout, Operation operation)
      throws NamespaceException {
    CompletableFuture<T> pending = Mono.defer(request).timeout(timeout).toFuture();
    try {
      return Optional.ofNullable(pending.get());
    } catch (InterruptedException exception) {
      pending.cancel(true);
      Thread.currentThread().interrupt();
      throw new NamespaceException("HouseTables namespace operation interrupted", exception);
    } catch (ExecutionException
        | CancellationException
        | WebClientResponseException
        | WebClientRequestException exception) {
      Throwable cause = exception instanceof ExecutionException ? exception.getCause() : exception;
      if (cause instanceof WebClientResponseException) {
        int statusCode = ((WebClientResponseException) cause).getRawStatusCode();
        if ((statusCode == 404 && operation != Operation.CREATE && operation != Operation.LIST)
            || (statusCode == 409 && operation == Operation.CREATE)) {
          Optional<String> type = Optional.empty();
          try {
            type =
                Optional.ofNullable(
                        new ObjectMapper()
                            .readTree(
                                ((WebClientResponseException) cause).getResponseBodyAsString()))
                    .filter(
                        body ->
                            body.isObject()
                                && body.path("status").isTextual()
                                && HttpStatus.valueOf(statusCode)
                                    .name()
                                    .equals(body.path("status").textValue())
                                && body.path("error").isTextual()
                                && body.path("message").isTextual()
                                && body.path("type").isTextual())
                    .map(body -> body.path("type").textValue());
          } catch (JsonProcessingException decodingFailure) {
            cause.addSuppressed(decodingFailure);
          }
          if (statusCode == 404 && type.filter("NotFoundException"::equals).isPresent()) {
            throw new NotFoundException("Namespace does not exist", cause);
          }
          if (statusCode == 409 && type.filter("AlreadyExistsException"::equals).isPresent()) {
            throw new AlreadyExistsException("Namespace already exists", cause);
          }
        }
      }
      throw new NamespaceException("HouseTables namespace operation failed", cause);
    }
  }
}
