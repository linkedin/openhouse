package com.linkedin.openhouse.javaclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataAlreadyExistsException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataConcurrentModificationException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataForbiddenException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataInvalidRequestException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataMutationUncertainException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataNotEmptyException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataNotFoundException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataResponseException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataUnauthenticatedException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataUnavailableException;
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
      throws NamespaceMetadataException {
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

  Map<String, String> load(Namespace namespace) throws NamespaceMetadataException {
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

  boolean drop(Namespace namespace) throws NamespaceMetadataException {
    validateNamespace(namespace);
    try {
      request(
          namespaceApi.dropNamespaceV1WithHttpInfo(catalogName, namespace.level(0)),
          204,
          60,
          Operation.DROP);
      return true;
    } catch (NamespaceMetadataNotFoundException failure) {
      return false;
    }
  }

  boolean update(Namespace namespace, Map<String, String> updates, Set<String> removals)
      throws NamespaceMetadataException {
    validateNamespace(namespace);
    validateProperties(updates);
    if (removals.stream().anyMatch(Objects::isNull)) {
      throw new NamespaceMetadataInvalidRequestException("Namespace removal keys must be strings");
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
            .orElseThrow(
                () ->
                    new NamespaceMetadataMutationUncertainException(
                        "Empty namespace update response"));
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
      throw new NamespaceMetadataMutationUncertainException(
          "Namespace update response disagrees with request");
    }
    return !updated.isEmpty() || !removed.isEmpty();
  }

  Map<String, String> effectiveProperties(Table table) throws NamespaceMetadataException {
    String tableName =
        Optional.ofNullable(table.name())
            .orElseThrow(() -> new NamespaceMetadataInvalidRequestException("Missing table name"));
    String catalogPrefix = catalogName + ".";
    if (!tableName.startsWith(catalogPrefix)) {
      throw new NamespaceMetadataInvalidRequestException(
          String.format("Table name '%s' must belong to catalog '%s'", tableName, catalogName));
    }
    String[] identifier = tableName.substring(catalogPrefix.length()).split("\\.", -1);
    if (identifier.length != 2
        || !identifier[0].matches("[a-zA-Z0-9_]{1,128}")
        || !identifier[1].matches("[a-zA-Z0-9_]{1,128}")) {
      throw new NamespaceMetadataInvalidRequestException(
          String.format(
              "Table name '%s' must identify a catalog-qualified namespace and table", tableName));
    }
    Map<String, String> effective = new HashMap<>();
    try {
      effective.putAll(load(Namespace.of(identifier[0])));
    } catch (NamespaceMetadataNotFoundException failure) {
      // Missing optional metadata contributes an empty set of defaults.
    }
    Map<String, String> tableProperties =
        Optional.ofNullable(table.properties())
            .orElseThrow(() -> new NamespaceMetadataResponseException("Missing table properties"));
    validateProperties(tableProperties);
    effective.putAll(tableProperties);
    return Collections.unmodifiableMap(effective);
  }

  private static void validateNamespace(Namespace namespace) throws NamespaceMetadataException {
    if (namespace.length() != 1 || !namespace.level(0).matches("[a-zA-Z0-9_]{1,128}")) {
      throw new NamespaceMetadataInvalidRequestException(
          String.format(
              "Namespace '%s' must contain one name of 1 to 128 letters, digits, or underscores",
              namespace));
    }
  }

  private static void validateProperties(Map<String, String> properties)
      throws NamespaceMetadataException {
    if (properties.entrySet().stream()
        .anyMatch(
            entry ->
                !Optional.ofNullable(entry.getKey()).isPresent()
                    || !Optional.ofNullable(entry.getValue()).isPresent())) {
      throw new NamespaceMetadataInvalidRequestException(
          "Namespace properties must have string keys and values");
    }
  }

  private static Map<String, String> metadata(
      ResponseEntity<NamespaceResponse> response, Namespace namespace, Operation operation)
      throws NamespaceMetadataException {
    NamespaceResponse requiredResponse =
        Optional.ofNullable(response.getBody())
            .orElseThrow(
                () ->
                    operation == Operation.LOAD
                        ? new NamespaceMetadataResponseException("Empty namespace response")
                        : new NamespaceMetadataMutationUncertainException(
                            "Empty namespace response"));
    if (requiredResponse.getNamespace().size() != 1
        || !namespace.level(0).equalsIgnoreCase(requiredResponse.getNamespace().get(0))) {
      throw operation == Operation.LOAD
          ? new NamespaceMetadataResponseException(
              "Namespace response identifies a different namespace")
          : new NamespaceMetadataMutationUncertainException(
              "Namespace response identifies a different namespace");
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
      throws NamespaceMetadataException {
    CompletableFuture<ResponseEntity<T>> pending = request.toFuture();
    try {
      ResponseEntity<T> response =
          Optional.ofNullable(pending.get(timeoutSeconds, TimeUnit.SECONDS))
              .orElseThrow(
                  () ->
                      operation == Operation.LOAD
                          ? new NamespaceMetadataResponseException("Empty namespace response")
                          : new NamespaceMetadataMutationUncertainException(
                              "Empty namespace response"));
      if (response.getStatusCodeValue() != expectedStatus) {
        throw operation == Operation.LOAD
            ? new NamespaceMetadataResponseException("Unexpected namespace response status")
            : new NamespaceMetadataMutationUncertainException(
                "Unexpected namespace response status");
      }
      return response;
    } catch (InterruptedException failure) {
      pending.cancel(true);
      Thread.currentThread().interrupt();
      throw operation == Operation.LOAD
          ? new NamespaceMetadataUnavailableException(0, failure)
          : new NamespaceMetadataMutationUncertainException(0, failure);
    } catch (TimeoutException failure) {
      pending.cancel(true);
      throw operation == Operation.LOAD
          ? new NamespaceMetadataUnavailableException(0, failure)
          : new NamespaceMetadataMutationUncertainException(0, failure);
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
                  .map(body -> body.path("error").path("type"))
                  .filter(JsonNode::isTextual)
                  .map(JsonNode::asText);
        } catch (JsonProcessingException decodingFailure) {
          cause.addSuppressed(decodingFailure);
        }
      }
      switch (statusCode) {
        case 400:
        case 422:
          throw new NamespaceMetadataInvalidRequestException(statusCode, cause);
        case 401:
          throw new NamespaceMetadataUnauthenticatedException(cause);
        case 403:
          throw new NamespaceMetadataForbiddenException(cause);
        case 404:
          throw new NamespaceMetadataNotFoundException(cause);
        case 409:
          if (operation == Operation.CREATE) {
            throw new NamespaceMetadataAlreadyExistsException(cause);
          }
          if (errorType.filter("NamespaceNotEmptyException"::equals).isPresent()) {
            throw new NamespaceMetadataNotEmptyException(cause);
          }
          if (operation == Operation.UPDATE
              || errorType.filter("CommitFailedException"::equals).isPresent()) {
            throw new NamespaceMetadataConcurrentModificationException(cause);
          }
          throw new NamespaceMetadataResponseException(statusCode, cause);
        case 503:
          throw new NamespaceMetadataUnavailableException(statusCode, cause);
        default:
          if (operation != Operation.LOAD) {
            throw new NamespaceMetadataMutationUncertainException(statusCode, cause);
          }
          if (cause instanceof WebClientRequestException
              || cause instanceof TimeoutException
              || statusCode == 502
              || statusCode == 504) {
            throw new NamespaceMetadataUnavailableException(statusCode, cause);
          }
          throw new NamespaceMetadataResponseException(statusCode, cause);
      }
    }
  }
}
