package com.linkedin.openhouse.jobs.services.livy;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.common.exception.JobEngineException;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.jobs.services.BatchJobInfo;
import com.linkedin.openhouse.jobs.services.HouseJobHandle;
import com.linkedin.openhouse.jobs.services.JobInfo;
import io.netty.handler.timeout.ReadTimeoutException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/** Implementation of {@link HouseJobHandle} for Livy API. */
@AllArgsConstructor
@Slf4j
public class LivyJobHandle implements HouseJobHandle {
  private final WebClient client;
  private final String executionId;
  private static final MetricsReporter METRICS_REPORTER = MetricsReporter.of("jobs_handle");

  @Override
  public void cancel() {
    METRICS_REPORTER.executeWithStats(
        () -> {
          client
              .delete()
              .uri("/batches/" + executionId)
              .retrieve()
              .onStatus(
                  HttpStatus::is5xxServerError,
                  r -> Mono.error(new JobEngineException("Engine server unavailable")))
              .onStatus(
                  status -> !HttpStatus.OK.equals(status),
                  r -> Mono.error(new JobEngineException("Could not cancel job")))
              .bodyToMono(String.class)
              .onErrorMap(
                  ReadTimeoutException.class,
                  ex -> new JobEngineException("Engine server timeout", ex))
              .block();
          return null;
        },
        MetricsConstant.CANCEL);
  }

  @Override
  public JobInfo getInfo() {
    return METRICS_REPORTER.executeWithStats(
        () -> {
          String responseBody =
              client
                  .get()
                  .uri("/batches/" + executionId)
                  .retrieve()
                  .onStatus(
                      HttpStatus::is5xxServerError,
                      r -> Mono.error(new JobEngineException("Engine server unavailable")))
                  .onStatus(
                      status -> !HttpStatus.OK.equals(status),
                      r -> Mono.error(new JobEngineException("Could not get job info")))
                  .bodyToMono(String.class)
                  .block();
          JsonObject data = new Gson().fromJson(responseBody, JsonObject.class);
          log.debug(data.toString());
          return BatchJobInfo.of(data);
        },
        MetricsConstant.GET);
  }

  // for testing
  public String getExecutionId() {
    return executionId;
  }
}
