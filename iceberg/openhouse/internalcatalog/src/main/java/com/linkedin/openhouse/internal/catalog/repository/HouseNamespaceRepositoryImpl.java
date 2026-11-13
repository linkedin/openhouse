package com.linkedin.openhouse.internal.catalog.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.common.exception.checked.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.checked.AuthenticationRequiredException;
import com.linkedin.openhouse.common.exception.checked.DomainException;
import com.linkedin.openhouse.common.exception.checked.InvalidPropertyUpdateException;
import com.linkedin.openhouse.common.exception.checked.InvalidRequestException;
import com.linkedin.openhouse.common.exception.checked.MutationUncertainException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.common.exception.checked.PermissionDeniedException;
import com.linkedin.openhouse.common.exception.checked.ServiceFailureException;
import com.linkedin.openhouse.common.exception.checked.UnavailableException;
import com.linkedin.openhouse.common.exception.checked.VersionConflictException;
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
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
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
      throws DomainException {
    try {
      NamespaceMetadataResponse result =
          execute(
                  () ->
                      namespaceApi.createNamespace(
                          new CreateNamespaceRequestBody()
                              .namespaceName(namespaceName)
                              .properties(properties)),
                  WRITE_TIMEOUT,
                  Operation.CREATE)
              .orElseThrow(
                  () ->
                      new MutationUncertainException(
                          "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying."));
      if (!namespaceName.equalsIgnoreCase(result.getNamespaceName())) {
        throw new MutationUncertainException(
            "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying.");
      }
      return new NamespaceMetadata(
          result.getNamespaceUuid(), result.getNamespaceName(), Map.copyOf(result.getProperties()));
    } catch (InvalidRequestException
        | InvalidPropertyUpdateException
        | AuthenticationRequiredException
        | PermissionDeniedException exception) {
      throw new ServiceFailureException(
          "HouseTables rejected the service's namespace creation request.", exception);
    }
  }

  @Override
  public NamespaceMetadata getByName(String namespaceName) throws DomainException {
    try {
      NamespaceMetadataResponse result =
          execute(() -> namespaceApi.getNamespace(namespaceName), READ_TIMEOUT, Operation.LOAD)
              .orElseThrow(
                  () -> new ServiceFailureException("Namespace metadata response is unusable"));
      if (!namespaceName.equalsIgnoreCase(result.getNamespaceName())) {
        throw new ServiceFailureException("Namespace metadata response is unusable");
      }
      return new NamespaceMetadata(
          result.getNamespaceUuid(), result.getNamespaceName(), Map.copyOf(result.getProperties()));
    } catch (InvalidRequestException
        | InvalidPropertyUpdateException
        | AuthenticationRequiredException
        | PermissionDeniedException exception) {
      throw new ServiceFailureException(
          "HouseTables rejected the service's namespace lookup.", exception);
    }
  }

  @Override
  public void delete(String namespaceName) throws DomainException {
    try {
      execute(
              () -> namespaceApi.deleteNamespaceWithHttpInfo(namespaceName),
              WRITE_TIMEOUT,
              Operation.DROP)
          .filter(response -> response.getStatusCodeValue() == 204)
          .orElseThrow(
              () ->
                  new MutationUncertainException(
                      "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying."));
    } catch (InvalidRequestException
        | InvalidPropertyUpdateException
        | AuthenticationRequiredException
        | PermissionDeniedException exception) {
      throw new ServiceFailureException(
          "HouseTables rejected the service's namespace deletion request.", exception);
    }
  }

  @Override
  public NamespacePropertyUpdate updateProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws DomainException {
    try {
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
                  () ->
                      new MutationUncertainException(
                          "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying."));
      Set<String> acknowledgedRemovals =
          Stream.concat(result.getRemoved().stream(), result.getMissing().stream())
              .collect(Collectors.toSet());
      if (!new HashSet<>(result.getUpdated()).equals(updates.keySet())
          || result.getUpdated().size() != updates.size()
          || !acknowledgedRemovals.equals(removals)
          || result.getRemoved().size() + result.getMissing().size()
              != acknowledgedRemovals.size()) {
        throw new MutationUncertainException(
            "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying.");
      }
      return new NamespacePropertyUpdate(
          List.copyOf(result.getUpdated()),
          List.copyOf(result.getRemoved()),
          List.copyOf(result.getMissing()));
    } catch (InvalidRequestException
        | InvalidPropertyUpdateException
        | AuthenticationRequiredException
        | PermissionDeniedException exception) {
      throw new ServiceFailureException(
          "HouseTables rejected the service's namespace property request.", exception);
    }
  }

  @Override
  public boolean containsEntities(String namespaceName) throws DomainException {
    Map<String, String> filters = Collections.singletonMap("databaseId", namespaceName);
    // Each query returns at most one HouseTables row, without loading Iceberg metadata files.
    try {
      Optional<GetAllEntityResponseBodyUserTable> tables =
          execute(
              () -> userTableApi.getPaginatedUserTables(filters, 0, 1, "tableId"),
              READ_TIMEOUT,
              Operation.LIST);
      if (!tables
          .map(GetAllEntityResponseBodyUserTable::getPageResults)
          .map(PageUserTable::getContent)
          .orElseThrow(() -> new ServiceFailureException("Namespace metadata response is unusable"))
          .isEmpty()) {
        return true;
      }
      return !execute(
              () -> userTableApi.getPaginatedUserViews(filters, 0, 1, "tableId"),
              READ_TIMEOUT,
              Operation.LIST)
          .map(GetAllEntityResponseBodyUserTable::getPageResults)
          .map(PageUserTable::getContent)
          .orElseThrow(() -> new ServiceFailureException("Namespace metadata response is unusable"))
          .isEmpty();
    } catch (NotFoundException
        | InvalidRequestException
        | InvalidPropertyUpdateException
        | AuthenticationRequiredException
        | PermissionDeniedException exception) {
      throw new ServiceFailureException("Required HouseTables listing failed.", exception);
    }
  }

  private <T> Optional<T> execute(Supplier<Mono<T>> request, Duration timeout, Operation operation)
      throws DomainException {
    Function<Throwable, DomainException> unavailable =
        operation == Operation.LOAD || operation == Operation.LIST
            ? cause -> new UnavailableException("Namespace metadata service unavailable", cause)
            : cause ->
                new MutationUncertainException(
                    "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying.",
                    cause);
    Function<Throwable, DomainException> unusable =
        operation == Operation.LOAD || operation == Operation.LIST
            ? cause -> new ServiceFailureException("Namespace metadata response is unusable", cause)
            : cause ->
                new MutationUncertainException(
                    "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying.",
                    cause);
    CompletableFuture<T> pending = Mono.defer(request).timeout(timeout).toFuture();
    try {
      return Optional.ofNullable(pending.get());
    } catch (InterruptedException exception) {
      pending.cancel(true);
      Thread.currentThread().interrupt();
      throw unavailable.apply(exception);
    } catch (ExecutionException
        | CancellationException
        | WebClientResponseException
        | WebClientRequestException exception) {
      Throwable cause = exception instanceof ExecutionException ? exception.getCause() : exception;
      if (cause instanceof WebClientResponseException) {
        int statusCode = ((WebClientResponseException) cause).getRawStatusCode();
        Optional<String> type = Optional.empty();
        try {
          type =
              Optional.ofNullable(
                      new ObjectMapper()
                          .readTree(((WebClientResponseException) cause).getResponseBodyAsString()))
                  .filter(
                      body ->
                          body.isObject()
                              && body.path("status").isTextual()
                              && Optional.ofNullable(HttpStatus.resolve(statusCode))
                                  .map(
                                      status ->
                                          status.name().equals(body.path("status").textValue()))
                                  .orElse(false)
                              && body.path("error").isTextual()
                              && body.path("message").isTextual()
                              && body.path("type").isTextual())
                  .map(body -> body.path("type").textValue());
        } catch (JsonProcessingException decodingFailure) {
          cause.addSuppressed(decodingFailure);
        }
        if (statusCode == 400 && type.filter("InvalidRequestException"::equals).isPresent()) {
          throw new InvalidRequestException("Invalid namespace request", cause);
        }
        if (statusCode == 404
            && operation != Operation.CREATE
            && type.filter("NotFoundException"::equals).isPresent()) {
          throw new NotFoundException("Namespace does not exist", cause);
        }
        if (statusCode == 409
            && operation == Operation.CREATE
            && type.filter("AlreadyExistsException"::equals).isPresent()) {
          throw new AlreadyExistsException("Namespace already exists", cause);
        }
        if (statusCode == 409
            && operation != Operation.LIST
            && type.filter("VersionConflictException"::equals).isPresent()) {
          throw new VersionConflictException("Namespace was modified concurrently", cause);
        }
        if (statusCode == 422
            && operation == Operation.UPDATE
            && type.filter("InvalidPropertyUpdateException"::equals).isPresent()) {
          throw new InvalidPropertyUpdateException("Invalid namespace property update", cause);
        }
        if (statusCode == 401
            && type.filter("AuthenticationRequiredException"::equals).isPresent()) {
          throw new AuthenticationRequiredException("HouseTables authentication required", cause);
        }
        if (statusCode == 403 && type.filter("PermissionDeniedException"::equals).isPresent()) {
          throw new PermissionDeniedException("HouseTables access denied", cause);
        }
        if (statusCode == 500 && type.filter("ServiceFailureException"::equals).isPresent()) {
          throw new ServiceFailureException("Namespace metadata operation failed", cause);
        }
        if (statusCode == 503 && type.filter("UnavailableException"::equals).isPresent()) {
          throw new UnavailableException("Namespace metadata service unavailable", cause);
        }
        if (statusCode == 502 || statusCode == 503 || statusCode == 504) {
          throw unavailable.apply(cause);
        }
        throw unusable.apply(cause);
      }
      if (cause instanceof WebClientRequestException
          || cause instanceof TimeoutException
          || cause instanceof CancellationException) {
        throw unavailable.apply(cause);
      }
      throw unusable.apply(cause);
    }
  }
}
