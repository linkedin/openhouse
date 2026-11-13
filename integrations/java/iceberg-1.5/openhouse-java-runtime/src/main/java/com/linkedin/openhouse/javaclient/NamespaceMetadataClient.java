package com.linkedin.openhouse.javaclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.javaclient.exception.AuthenticationRequiredException;
import com.linkedin.openhouse.javaclient.exception.CatalogOperationException;
import com.linkedin.openhouse.javaclient.exception.DuplicateCreationException;
import com.linkedin.openhouse.javaclient.exception.InvalidRequestException;
import com.linkedin.openhouse.javaclient.exception.MutationUncertainException;
import com.linkedin.openhouse.javaclient.exception.NotEmptyException;
import com.linkedin.openhouse.javaclient.exception.NotFoundException;
import com.linkedin.openhouse.javaclient.exception.OperationFailedException;
import com.linkedin.openhouse.javaclient.exception.PermissionDeniedException;
import com.linkedin.openhouse.javaclient.exception.UnavailableException;
import com.linkedin.openhouse.javaclient.exception.VersionConflictException;
import com.linkedin.openhouse.tables.client.api.NamespaceApi;
import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.tables.client.model.CreateNamespaceRequest;
import com.linkedin.openhouse.tables.client.model.NamespaceResponse;
import com.linkedin.openhouse.tables.client.model.UpdateNamespacePropertiesRequest;
import com.linkedin.openhouse.tables.client.model.UpdateNamespacePropertiesResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/** Shared namespace request and validation logic for the version-specific catalog adapters. */
final class NamespaceMetadataClient {
  enum Operation {
    CREATE,
    LOAD,
    DROP,
    UPDATE
  }

  private final NamespaceApi namespaceApi;
  private final String catalogName;

  NamespaceMetadataClient(ApiClient apiClient, String catalogName) {
    this.namespaceApi = new NamespaceApi(apiClient);
    this.catalogName = catalogName;
  }

  Map<String, String> create(Namespace namespace, Map<String, String> properties)
      throws CatalogOperationException {
    validateNamespace(namespace);
    validateProperties(properties);
    ResponseEntity<NamespaceResponse> response =
        request(
            namespaceApi.createNamespaceV1WithHttpInfo(
                catalogName,
                new CreateNamespaceRequest()
                    .namespace(Arrays.asList(namespace.levels()))
                    .properties(properties)),
            200,
            60,
            Operation.CREATE);
    return metadata(response, namespace, Operation.CREATE);
  }

  Map<String, String> load(Namespace namespace) throws CatalogOperationException {
    validateNamespace(namespace);
    return metadata(
        request(
            namespaceApi.loadNamespaceMetadataV1WithHttpInfo(catalogName, namespace.level(0)),
            200,
            30,
            Operation.LOAD),
        namespace,
        Operation.LOAD);
  }

  boolean drop(Namespace namespace) throws CatalogOperationException {
    validateNamespace(namespace);
    try {
      request(
          namespaceApi.dropNamespaceV1WithHttpInfo(catalogName, namespace.level(0)),
          204,
          60,
          Operation.DROP);
      return true;
    } catch (NotFoundException failure) {
      return false;
    }
  }

  boolean update(Namespace namespace, Map<String, String> updates, Set<String> removals)
      throws CatalogOperationException {
    validateNamespace(namespace);
    validateProperties(updates);
    if (removals.stream().anyMatch(Objects::isNull)) {
      throw new InvalidRequestException("Namespace removal keys must be strings");
    }
    ResponseEntity<UpdateNamespacePropertiesResponse> response =
        request(
            namespaceApi.updateNamespacePropertiesV1WithHttpInfo(
                catalogName,
                namespace.level(0),
                new UpdateNamespacePropertiesRequest().updates(updates).removals(removals)),
            200,
            60,
            Operation.UPDATE);
    UpdateNamespacePropertiesResponse requiredResponse =
        Optional.ofNullable(response.getBody())
            .orElseThrow(() -> new MutationUncertainException("Empty namespace update response"));
    List<String> updated = requiredResponse.getUpdated();
    List<String> removed = requiredResponse.getRemoved();
    List<String> missing =
        Optional.ofNullable(requiredResponse.getMissing()).orElseGet(Collections::emptyList);
    Set<String> accountedRemovals =
        Stream.concat(removed.stream(), missing.stream()).collect(Collectors.toSet());
    if (!new HashSet<>(updated).equals(updates.keySet())
        || updated.size() != updates.size()
        || !removals.containsAll(accountedRemovals)
        || removed.size() + missing.size() != accountedRemovals.size()) {
      throw new MutationUncertainException("Namespace update response disagrees with request");
    }
    return !updated.isEmpty() || !removed.isEmpty();
  }

  Map<String, String> effectiveProperties(Table table) throws CatalogOperationException {
    String tableName =
        Optional.ofNullable(table.name())
            .orElseThrow(() -> new InvalidRequestException("Missing table name"));
    String catalogPrefix = catalogName + ".";
    if (!tableName.startsWith(catalogPrefix)) {
      throw new InvalidRequestException(
          String.format("Table name '%s' must belong to catalog '%s'", tableName, catalogName));
    }
    String[] identifier = tableName.substring(catalogPrefix.length()).split("\\.", -1);
    if (identifier.length != 2
        || !identifier[0].matches("[a-zA-Z0-9_]{1,128}")
        || !identifier[1].matches("[a-zA-Z0-9_]{1,128}")) {
      throw new InvalidRequestException(
          String.format(
              "Table name '%s' must identify a catalog-qualified namespace and table", tableName));
    }
    Map<String, String> effective = new HashMap<>();
    try {
      effective.putAll(load(Namespace.of(identifier[0])));
    } catch (NotFoundException failure) {
      // Missing optional metadata contributes an empty set of defaults.
    }
    Map<String, String> tableProperties =
        Optional.ofNullable(table.properties())
            .orElseThrow(() -> new OperationFailedException("Missing table properties"));
    validateProperties(tableProperties);
    effective.putAll(tableProperties);
    return Collections.unmodifiableMap(effective);
  }

  private static void validateNamespace(Namespace namespace) throws CatalogOperationException {
    if (namespace.length() != 1 || !namespace.level(0).matches("[a-zA-Z0-9_]{1,128}")) {
      throw new InvalidRequestException(
          String.format(
              "Namespace '%s' must contain one name of 1 to 128 letters, digits, or underscores",
              namespace));
    }
  }

  private static void validateProperties(Map<String, String> properties)
      throws CatalogOperationException {
    if (properties.entrySet().stream()
        .anyMatch(
            entry ->
                !Optional.ofNullable(entry.getKey()).isPresent()
                    || !Optional.ofNullable(entry.getValue()).isPresent())) {
      throw new InvalidRequestException("Namespace properties must have string keys and values");
    }
  }

  private static Map<String, String> metadata(
      ResponseEntity<NamespaceResponse> response, Namespace namespace, Operation operation)
      throws CatalogOperationException {
    NamespaceResponse requiredResponse =
        Optional.ofNullable(response.getBody())
            .orElseThrow(
                () ->
                    operation == Operation.LOAD
                        ? new OperationFailedException("Empty namespace response")
                        : new MutationUncertainException("Empty namespace response"));
    if (requiredResponse.getNamespace().size() != 1
        || !namespace.level(0).equalsIgnoreCase(requiredResponse.getNamespace().get(0))) {
      throw operation == Operation.LOAD
          ? new OperationFailedException("Namespace response identifies a different namespace")
          : new MutationUncertainException("Namespace response identifies a different namespace");
    }
    Map<String, String> responseProperties =
        Optional.ofNullable(requiredResponse.getProperties()).orElseGet(Collections::emptyMap);
    return Collections.unmodifiableMap(new HashMap<>(responseProperties));
  }

  /**
   * Bounds each generated request and converts asynchronous client failures to checked failures.
   */
  static <T> ResponseEntity<T> request(
      Mono<ResponseEntity<T>> request, int expectedStatus, int timeoutSeconds, Operation operation)
      throws CatalogOperationException {
    CompletableFuture<ResponseEntity<T>> pending = request.toFuture();
    try {
      ResponseEntity<T> response =
          Optional.ofNullable(pending.get(timeoutSeconds, TimeUnit.SECONDS))
              .orElseThrow(
                  () ->
                      operation == Operation.LOAD
                          ? new OperationFailedException("Empty namespace response")
                          : new MutationUncertainException("Empty namespace response"));
      if (response.getStatusCodeValue() != expectedStatus) {
        throw operation == Operation.LOAD
            ? new OperationFailedException("Unexpected namespace response status")
            : new MutationUncertainException("Unexpected namespace response status");
      }
      return response;
    } catch (InterruptedException failure) {
      pending.cancel(true);
      Thread.currentThread().interrupt();
      throw operation == Operation.LOAD
          ? new UnavailableException(failure)
          : new MutationUncertainException(failure);
    } catch (TimeoutException | CancellationException failure) {
      pending.cancel(true);
      throw operation == Operation.LOAD
          ? new UnavailableException(failure)
          : new MutationUncertainException(failure);
    } catch (ExecutionException failure) {
      Throwable cause = failure.getCause();
      int statusCode =
          cause instanceof WebClientResponseException
                  && ((WebClientResponseException) cause).getRawStatusCode() >= 400
              ? ((WebClientResponseException) cause).getRawStatusCode()
              : 0;
      Optional<String> errorType = Optional.empty();
      if (cause instanceof WebClientResponseException) {
        try {
          errorType =
              Optional.ofNullable(
                      new ObjectMapper()
                          .readTree(((WebClientResponseException) cause).getResponseBodyAsString()))
                  .map(body -> body.path("error"))
                  .filter(
                      error ->
                          error.isObject()
                              && error.path("code").isIntegralNumber()
                              && error.path("code").canConvertToInt()
                              && error.path("code").intValue() == statusCode
                              && error.path("message").isTextual()
                              && error.path("type").isTextual())
                  .map(error -> error.path("type").textValue());
        } catch (JsonProcessingException decodingFailure) {
          cause.addSuppressed(decodingFailure);
        }
      }
      if (operation == Operation.DROP
          && (statusCode == 400 || statusCode == 409)
          && errorType.filter("NamespaceNotEmptyException"::equals).isPresent()) {
        throw new NotEmptyException(cause);
      }
      if ((statusCode == 400 && errorType.filter("BadRequestException"::equals).isPresent())
          || (statusCode == 422
              && operation == Operation.UPDATE
              && errorType.filter("UnprocessableEntityException"::equals).isPresent())) {
        throw new InvalidRequestException(cause);
      }
      if (statusCode == 401 && errorType.filter("NotAuthorizedException"::equals).isPresent()) {
        throw new AuthenticationRequiredException(cause);
      }
      if (statusCode == 403 && errorType.filter("ForbiddenException"::equals).isPresent()) {
        throw new PermissionDeniedException(cause);
      }
      if (statusCode == 404
          && operation != Operation.CREATE
          && errorType.filter("NoSuchNamespaceException"::equals).isPresent()) {
        throw new NotFoundException(cause);
      }
      if (statusCode == 409
          && operation == Operation.CREATE
          && errorType.filter("AlreadyExistsException"::equals).isPresent()) {
        throw new DuplicateCreationException(cause);
      }
      if (statusCode == 409 && errorType.filter("CommitFailedException"::equals).isPresent()) {
        throw new VersionConflictException(cause);
      }
      if (statusCode == 500 && errorType.filter("ServiceFailureException"::equals).isPresent()) {
        throw new OperationFailedException(cause);
      }
      if (statusCode == 503
          && errorType.filter("ServiceUnavailableException"::equals).isPresent()) {
        throw new UnavailableException(cause);
      }
      if (operation != Operation.LOAD) {
        throw new MutationUncertainException(cause);
      }
      if (cause instanceof WebClientRequestException
          || cause instanceof TimeoutException
          || cause instanceof CancellationException
          || statusCode == 502
          || statusCode == 503
          || statusCode == 504) {
        throw new UnavailableException(cause);
      }
      throw new OperationFailedException(cause);
    }
  }
}
