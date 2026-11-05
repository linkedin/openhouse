package com.linkedin.openhouse.javaclient;

import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataException;
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
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ValidationException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/** Shared namespace request and validation logic for the version-specific catalog adapters. */
final class NamespaceMetadataClient {
  private final NamespaceApi namespaceApi;
  private final String catalogName;

  NamespaceMetadataClient(ApiClient apiClient, String catalogName) {
    this.namespaceApi = new NamespaceApi(apiClient);
    this.catalogName = catalogName;
  }

  void create(Namespace namespace, Map<String, String> properties)
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
            60);
    metadata(response, namespace);
  }

  Map<String, String> load(Namespace namespace) throws NamespaceMetadataException {
    validateNamespace(namespace);
    return metadata(
        request(
            namespaceApi.loadNamespaceMetadataV1WithHttpInfo(catalogName, namespace.level(0)),
            200,
            30),
        namespace);
  }

  boolean drop(Namespace namespace) throws NamespaceMetadataException {
    validateNamespace(namespace);
    try {
      request(namespaceApi.dropNamespaceV1WithHttpInfo(catalogName, namespace.level(0)), 204, 60);
      return true;
    } catch (NamespaceMetadataException failure) {
      if (failure.getStatusCode() == 404) {
        return false;
      }
      throw failure;
    }
  }

  boolean update(Namespace namespace, Map<String, String> updates, Set<String> removals)
      throws NamespaceMetadataException {
    validateNamespace(namespace);
    validateProperties(updates);
    if (removals.stream().anyMatch(Objects::isNull)) {
      throw new NamespaceMetadataException(400, "Namespace removal keys must be strings");
    }
    ResponseEntity<UpdateNamespacePropertiesResponse> response =
        request(
            namespaceApi.updateNamespacePropertiesV1WithHttpInfo(
                catalogName,
                namespace.level(0),
                new UpdateNamespacePropertiesRequest().updates(updates).removals(removals)),
            200,
            60);
    UpdateNamespacePropertiesResponse requiredResponse =
        Optional.ofNullable(response.getBody())
            .orElseThrow(
                () -> new NamespaceMetadataException(0, "Empty namespace update response"));
    List<String> updated =
        Optional.ofNullable(requiredResponse.getUpdated())
            .orElseThrow(() -> new NamespaceMetadataException(0, "Missing updated keys"));
    List<String> removed =
        Optional.ofNullable(requiredResponse.getRemoved())
            .orElseThrow(() -> new NamespaceMetadataException(0, "Missing removed keys"));
    List<String> missing =
        Optional.ofNullable(requiredResponse.getMissing())
            .orElseThrow(() -> new NamespaceMetadataException(0, "Missing absent keys"));
    if (Stream.of(updated, removed, missing).flatMap(List::stream).anyMatch(Objects::isNull)) {
      throw new NamespaceMetadataException(0, "Malformed namespace update response");
    }
    Set<String> accountedRemovals =
        Stream.concat(removed.stream(), missing.stream()).collect(Collectors.toSet());
    if (!new HashSet<>(updated).equals(updates.keySet())
        || updated.size() != updates.size()
        || !removals.containsAll(accountedRemovals)
        || removed.size() + missing.size() != accountedRemovals.size()) {
      throw new NamespaceMetadataException(0, "Namespace update response disagrees with request");
    }
    return !updated.isEmpty() || !removed.isEmpty();
  }

  Map<String, String> effectiveProperties(Table table) throws NamespaceMetadataException {
    String tableName =
        Optional.ofNullable(table.name())
            .orElseThrow(() -> new NamespaceMetadataException(400, "Missing table name"));
    String catalogPrefix = catalogName + ".";
    if (!tableName.startsWith(catalogPrefix)) {
      throw new NamespaceMetadataException(400, "Table name must belong to this catalog");
    }
    String[] identifier = tableName.substring(catalogPrefix.length()).split("\\.", -1);
    if (identifier.length != 2 || !identifier[1].matches("[a-zA-Z0-9_]{1,128}")) {
      throw new NamespaceMetadataException(400, "Expected a catalog-qualified database and table");
    }
    Map<String, String> effective = new HashMap<>();
    try {
      effective.putAll(load(Namespace.of(identifier[0])));
    } catch (NamespaceMetadataException failure) {
      if (failure.getStatusCode() != 404) {
        throw failure;
      }
    }
    Map<String, String> tableProperties =
        Optional.ofNullable(table.properties())
            .orElseThrow(() -> new NamespaceMetadataException(0, "Missing table properties"));
    validateProperties(tableProperties);
    effective.putAll(tableProperties);
    return Collections.unmodifiableMap(effective);
  }

  static RuntimeException icebergFailure(NamespaceMetadataException failure, String operation) {
    switch (failure.getStatusCode()) {
      case 400:
      case 422:
        return new ValidationException(failure, "Invalid namespace request");
      case 401:
        return new NotAuthorizedException(failure, "Namespace authentication required");
      case 403:
        return new ForbiddenException(failure, "Namespace access denied");
      case 404:
        return new NoSuchNamespaceException(failure, "Namespace metadata does not exist");
      case 409:
        if ("create".equals(operation)) {
          return new AlreadyExistsException(failure, "Namespace already exists");
        }
        if ("drop".equals(operation)) {
          return new NamespaceNotEmptyException(failure, "Namespace is nonempty or changed");
        }
        return new CommitFailedException(failure, "Namespace metadata changed concurrently");
      default:
        return new RESTException(failure, "Namespace %s failed", operation);
    }
  }

  private static void validateNamespace(Namespace namespace) throws NamespaceMetadataException {
    if (namespace.length() != 1 || !namespace.level(0).matches("[a-zA-Z0-9_]{1,128}")) {
      throw new NamespaceMetadataException(
          400, "Expected one namespace containing 1 to 128 letters, digits, or underscores");
    }
  }

  private static void validateProperties(Map<String, String> properties)
      throws NamespaceMetadataException {
    if (properties.entrySet().stream()
        .anyMatch(
            entry ->
                !Optional.ofNullable(entry.getKey()).isPresent()
                    || !Optional.ofNullable(entry.getValue()).isPresent())) {
      throw new NamespaceMetadataException(
          400, "Namespace properties must have string keys and values");
    }
  }

  private static Map<String, String> metadata(
      ResponseEntity<NamespaceResponse> response, Namespace namespace)
      throws NamespaceMetadataException {
    NamespaceResponse requiredResponse =
        Optional.ofNullable(response.getBody())
            .orElseThrow(() -> new NamespaceMetadataException(0, "Empty namespace response"));
    List<String> responseNamespace =
        Optional.ofNullable(requiredResponse.getNamespace())
            .orElseThrow(() -> new NamespaceMetadataException(0, "Missing response namespace"));
    if (responseNamespace.size() != 1
        || !namespace.level(0).equalsIgnoreCase(responseNamespace.get(0))) {
      throw new NamespaceMetadataException(0, "Namespace response identifies a different database");
    }
    Map<String, String> responseProperties =
        Optional.ofNullable(requiredResponse.getProperties())
            .orElseThrow(() -> new NamespaceMetadataException(0, "Missing namespace properties"));
    validateProperties(responseProperties);
    return Collections.unmodifiableMap(new HashMap<>(responseProperties));
  }

  /**
   * Bounds each generated request and converts asynchronous client failures to checked failures.
   */
  private static <T> ResponseEntity<T> request(
      Mono<ResponseEntity<T>> request, int expectedStatus, int timeoutSeconds)
      throws NamespaceMetadataException {
    CompletableFuture<ResponseEntity<T>> pending = request.toFuture();
    try {
      ResponseEntity<T> response =
          Optional.ofNullable(pending.get(timeoutSeconds, TimeUnit.SECONDS))
              .orElseThrow(() -> new NamespaceMetadataException(0, "Empty namespace response"));
      if (response.getStatusCodeValue() != expectedStatus) {
        throw new NamespaceMetadataException(0, "Unexpected namespace response status");
      }
      return response;
    } catch (InterruptedException failure) {
      pending.cancel(true);
      Thread.currentThread().interrupt();
      throw new NamespaceMetadataException(0, "Namespace request interrupted", failure);
    } catch (TimeoutException failure) {
      pending.cancel(true);
      throw new NamespaceMetadataException(0, "Namespace request timed out", failure);
    } catch (ExecutionException failure) {
      Throwable cause = failure.getCause();
      int statusCode =
          cause instanceof WebClientResponseException
                  && ((WebClientResponseException) cause).getRawStatusCode() >= 400
              ? ((WebClientResponseException) cause).getRawStatusCode()
              : 0;
      throw new NamespaceMetadataException(statusCode, "Namespace request failed", cause);
    }
  }
}
