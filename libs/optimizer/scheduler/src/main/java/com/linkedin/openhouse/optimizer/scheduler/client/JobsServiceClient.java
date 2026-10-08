package com.linkedin.openhouse.optimizer.scheduler.client;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.reactive.function.client.WebClient;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * Client for the OpenHouse Jobs Service.
 *
 * <p>Submits one {@code BatchedOrphanFilesDeletionSparkApp} job per bin via {@code POST /jobs}.
 */
@Slf4j
public class JobsServiceClient {

  private static final JsonMapper MAPPER = JsonMapper.builder().build();
  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  private final WebClient webClient;
  private final String clusterId;

  public JobsServiceClient(WebClient webClient, String clusterId) {
    this.webClient = webClient;
    this.clusterId = clusterId;
  }

  /**
   * Submit a batched Spark job for the given tables.
   *
   * @param jobName human-readable name for the job
   * @param jobType operation type string (e.g. "ORPHAN_FILES_DELETION")
   * @param tableNames fully-qualified table names (db.table)
   * @param operationIds operation UUIDs — parallel to tableNames
   * @param resultsEndpoint base URL the Spark app PATCHes results back to
   * @return job ID if the submission succeeded, empty if an error occurred
   */
  public Optional<String> launch(
      String jobName,
      String jobType,
      List<String> tableNames,
      List<String> operationIds,
      String resultsEndpoint) {
    try {
      ObjectNode body = MAPPER.createObjectNode();
      body.put("jobName", jobName);
      body.put("clusterId", clusterId);

      ObjectNode jobConf = body.putObject("jobConf");
      jobConf.put("jobType", jobType);

      ArrayNode args = jobConf.putArray("args");
      args.add("--tableNames");
      args.add(String.join(",", tableNames));
      args.add("--operationIds");
      args.add(String.join(",", operationIds));
      args.add("--resultsEndpoint");
      args.add(resultsEndpoint);

      String responseBody =
          webClient
              .post()
              .uri("/jobs")
              .bodyValue(body)
              .retrieve()
              .bodyToMono(String.class)
              .timeout(TIMEOUT)
              .block();

      String jobId = MAPPER.readTree(responseBody).path("jobId").asText(null);
      return Optional.ofNullable(jobId);
    } catch (Exception e) {
      log.error("Failed to submit job '{}': {}", jobName, e.getMessage());
      return Optional.empty();
    }
  }
}
