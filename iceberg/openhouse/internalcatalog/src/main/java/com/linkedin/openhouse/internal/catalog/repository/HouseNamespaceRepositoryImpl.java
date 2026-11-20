package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.housetables.client.api.NamespaceApi;
import com.linkedin.openhouse.housetables.client.model.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.client.model.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.client.model.UpdateNamespaceRequestBody;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
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
    } catch (WebClientResponseException.Conflict exception) {
      throw new DuplicateCreationException("Namespace already exists", exception);
    }
    return metadata(result);
  }

  @Override
  public NamespaceMetadata getByName(String namespaceName) throws NotFoundException {
    NamespaceMetadataResponse result;
    try {
      result = execute(() -> namespaceApi.getNamespace(namespaceName), READ_TIMEOUT);
    } catch (WebClientResponseException.NotFound exception) {
      throw new NotFoundException("Namespace does not exist", exception);
    }
    return metadata(result);
  }

  @Override
  public void delete(String namespaceName) throws NotFoundException {
    try {
      execute(() -> namespaceApi.deleteNamespace(namespaceName), WRITE_TIMEOUT);
    } catch (WebClientResponseException.NotFound exception) {
      throw new NotFoundException("Namespace does not exist", exception);
    } catch (WebClientResponseException.Conflict exception) {
      throw new EntityConcurrentModificationException(namespaceName, exception);
    }
  }

  @Override
  public NamespaceMetadata update(String namespaceName, NamespaceMetadata document)
      throws NotFoundException {
    NamespaceMetadataResponse result;
    try {
      result =
          execute(
              () ->
                  namespaceApi.updateNamespace(
                      namespaceName,
                      new UpdateNamespaceRequestBody()
                          .namespaceUuid(document.getNamespaceUuid())
                          .namespaceName(document.getNamespaceName())
                          .properties(document.getProperties())
                          .revision(document.getRevision())),
              WRITE_TIMEOUT);
    } catch (WebClientResponseException.NotFound exception) {
      throw new NotFoundException("Namespace does not exist", exception);
    } catch (WebClientResponseException.Conflict exception) {
      throw new EntityConcurrentModificationException(namespaceName, exception);
    }
    return metadata(result);
  }

  private NamespaceMetadata metadata(NamespaceMetadataResponse response) {
    return new NamespaceMetadata(
        Objects.requireNonNull(response.getNamespaceUuid()),
        Objects.requireNonNull(response.getNamespaceName()),
        Map.copyOf(response.getProperties()),
        Objects.requireNonNull(response.getRevision()));
  }

  private <T> T execute(Supplier<Mono<T>> request, Duration timeout) {
    CompletableFuture<T> pending = Mono.defer(request).timeout(timeout).toFuture();
    try {
      return pending.get();
    } catch (InterruptedException exception) {
      pending.cancel(true);
      Thread.currentThread().interrupt();
      CancellationException cancellation =
          new CancellationException("HouseTables namespace operation interrupted");
      cancellation.initCause(exception);
      throw cancellation;
    } catch (ExecutionException exception) {
      throw Exceptions.propagate(exception.getCause());
    }
  }
}
