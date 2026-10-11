package com.linkedin.openhouse.jobs.spark.replication;

import com.linkedin.openhouse.housetables.client.model.ReplicationCheckpoint;
import com.linkedin.openhouse.housetables.client.model.ReplicationCheckpointUpdate;
import com.linkedin.openhouse.housetables.client.model.ReplicationDestination;
import com.linkedin.openhouse.housetables.client.model.ReplicationEdgeState;
import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import java.util.Collections;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient.ResponseSpec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Tables API client for replication state endpoints omitted from the generated Tables client. */
class ReferenceReplicationStateApi {
  private static final String DESTINATIONS_PATH = "/v1/replication/destinations";
  private static final String CHECKPOINTS_PATH = "/v1/replication/checkpoints";

  private final ApiClient apiClient;

  ReferenceReplicationStateApi(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  Flux<ReplicationEdgeState> getDestinationsV1(
      String sourceClusterId, String sourceTableUUID, Long sourceCreationTime) {
    MultiValueMap<String, String> queryParams = new LinkedMultiValueMap<>();
    queryParams.putAll(
        apiClient.parameterToMultiValueMap(null, "sourceClusterId", sourceClusterId));
    queryParams.putAll(
        apiClient.parameterToMultiValueMap(null, "sourceTableUUID", sourceTableUUID));
    queryParams.putAll(
        apiClient.parameterToMultiValueMap(null, "sourceCreationTime", sourceCreationTime));
    return invoke(
            DESTINATIONS_PATH,
            HttpMethod.GET,
            queryParams,
            null,
            new ParameterizedTypeReference<ReplicationEdgeState>() {})
        .bodyToFlux(new ParameterizedTypeReference<ReplicationEdgeState>() {});
  }

  Mono<ReplicationDestination> putDestinationV1(ReplicationDestination destination) {
    return invoke(
            DESTINATIONS_PATH,
            HttpMethod.PUT,
            new LinkedMultiValueMap<>(),
            destination,
            new ParameterizedTypeReference<ReplicationDestination>() {})
        .bodyToMono(new ParameterizedTypeReference<ReplicationDestination>() {});
  }

  Mono<ReplicationCheckpoint> getCheckpointV1(
      String sourceClusterId,
      String sourceTableUUID,
      Long sourceCreationTime,
      String destinationClusterId,
      String destinationTableUUID,
      Long destinationCreationTime) {
    MultiValueMap<String, String> queryParams = new LinkedMultiValueMap<>();
    queryParams.putAll(
        apiClient.parameterToMultiValueMap(null, "sourceClusterId", sourceClusterId));
    queryParams.putAll(
        apiClient.parameterToMultiValueMap(null, "sourceTableUUID", sourceTableUUID));
    queryParams.putAll(
        apiClient.parameterToMultiValueMap(null, "sourceCreationTime", sourceCreationTime));
    queryParams.putAll(
        apiClient.parameterToMultiValueMap(null, "destinationClusterId", destinationClusterId));
    queryParams.putAll(
        apiClient.parameterToMultiValueMap(null, "destinationTableUUID", destinationTableUUID));
    queryParams.putAll(
        apiClient.parameterToMultiValueMap(
            null, "destinationCreationTime", destinationCreationTime));
    return invoke(
            CHECKPOINTS_PATH,
            HttpMethod.GET,
            queryParams,
            null,
            new ParameterizedTypeReference<ReplicationCheckpoint>() {})
        .bodyToMono(new ParameterizedTypeReference<ReplicationCheckpoint>() {});
  }

  Mono<ReplicationCheckpoint> advanceCheckpointV1(ReplicationCheckpointUpdate update) {
    return invoke(
            CHECKPOINTS_PATH,
            HttpMethod.PUT,
            new LinkedMultiValueMap<>(),
            update,
            new ParameterizedTypeReference<ReplicationCheckpoint>() {})
        .bodyToMono(new ParameterizedTypeReference<ReplicationCheckpoint>() {});
  }

  private <T> ResponseSpec invoke(
      String path,
      HttpMethod method,
      MultiValueMap<String, String> queryParams,
      Object body,
      ParameterizedTypeReference<T> responseType) {
    return apiClient.invokeAPI(
        path,
        method,
        Collections.<String, Object>emptyMap(),
        queryParams,
        body,
        new HttpHeaders(),
        new LinkedMultiValueMap<>(),
        new LinkedMultiValueMap<>(),
        Collections.singletonList(MediaType.APPLICATION_JSON),
        body == null ? null : MediaType.APPLICATION_JSON,
        new String[0],
        responseType);
  }
}
