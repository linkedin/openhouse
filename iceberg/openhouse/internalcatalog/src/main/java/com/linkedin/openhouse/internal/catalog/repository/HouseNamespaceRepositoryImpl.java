package com.linkedin.openhouse.internal.catalog.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.housetables.client.api.NamespaceApi;
import com.linkedin.openhouse.housetables.client.model.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.client.model.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.client.model.UpdateNamespacePropertiesRequestBody;
import com.linkedin.openhouse.housetables.client.model.UpdateNamespacePropertiesResponseBody;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

/** Uses bounded HouseTables requests with automatic NamespaceApi transport retries disabled. */
@RequiredArgsConstructor
public class HouseNamespaceRepositoryImpl implements HouseNamespaceRepository {
  private static final Duration READ_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(60);

  private final NamespaceApi namespaceApi;

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
              WRITE_TIMEOUT);
    } catch (WebClientResponseException exception) {
      if (exception.getRawStatusCode() == 409 && isError(exception, "AlreadyExistsException")) {
        throw new DuplicateCreationException("Namespace already exists", exception);
      }
      throw exception;
    }
    return new NamespaceMetadata(
        result.getNamespaceUuid(), result.getNamespaceName(), Map.copyOf(result.getProperties()));
  }

  @Override
  public NamespaceMetadata getByName(String namespaceName) throws NotFoundException {
    NamespaceMetadataResponse result =
        executeExisting(() -> namespaceApi.getNamespace(namespaceName), READ_TIMEOUT);
    return new NamespaceMetadata(
        result.getNamespaceUuid(), result.getNamespaceName(), Map.copyOf(result.getProperties()));
  }

  @Override
  public void delete(String namespaceName) throws NotFoundException {
    executeExisting(() -> namespaceApi.deleteNamespace(namespaceName), WRITE_TIMEOUT);
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
                    new UpdateNamespacePropertiesRequestBody().updates(updates).removals(removals)),
            WRITE_TIMEOUT);
    return new NamespacePropertyUpdate(
        List.copyOf(result.getUpdated()),
        List.copyOf(result.getRemoved()),
        List.copyOf(result.getMissing()));
  }

  private <T> T executeExisting(Supplier<Mono<T>> request, Duration timeout)
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
      return expectedType.equals(
          new ObjectMapper()
              .readTree(exception.getResponseBodyAsString())
              .path("type")
              .textValue());
    } catch (JsonProcessingException decodingFailure) {
      exception.addSuppressed(decodingFailure);
      return false;
    }
  }

  private <T> T execute(Supplier<Mono<T>> request, Duration timeout) {
    CompletableFuture<T> pending = Mono.defer(request).timeout(timeout).toFuture();
    try {
      return pending.get();
    } catch (InterruptedException exception) {
      pending.cancel(true);
      Thread.currentThread().interrupt();
      throw new IllegalStateException("HouseTables namespace operation interrupted", exception);
    } catch (ExecutionException exception) {
      throw Exceptions.propagate(exception.getCause());
    }
  }
}
