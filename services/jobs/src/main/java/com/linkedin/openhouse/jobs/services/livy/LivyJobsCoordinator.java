package com.linkedin.openhouse.jobs.services.livy;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.common.exception.JobEngineException;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.jobs.config.JobLaunchConf;
import com.linkedin.openhouse.jobs.services.HouseJobHandle;
import com.linkedin.openhouse.jobs.services.HouseJobsCoordinator;
import io.netty.handler.timeout.ReadTimeoutException;
import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

/** Implementation of Jobs Coordinator for Livy API. */
@Getter
@Slf4j
@AllArgsConstructor
public class LivyJobsCoordinator implements HouseJobsCoordinator {
  private static final int ENGINE_API_RESPONSE_TIMEOUT_SECONDS = 10;
  private static final MetricsReporter METRICS_REPORTER = MetricsReporter.of("jobs_coordinator");
  private final WebClient client;

  public LivyJobsCoordinator(String baseURL) {
    this(getDefaultWebClient(baseURL));
  }

  public LivyJobHandle submit(JobLaunchConf conf) {
    return METRICS_REPORTER.executeWithStats(
        () -> {
          String responseBody =
              client
                  .post()
                  .uri("/batches")
                  .bodyValue(createSubmitRequestBodyString(conf))
                  .retrieve()
                  .onStatus(
                      HttpStatus::is5xxServerError,
                      r -> Mono.error(new JobEngineException("Engine server unavailable")))
                  .onStatus(
                      status -> !HttpStatus.CREATED.equals(status),
                      r -> Mono.error(new JobEngineException("Could not submit job")))
                  .bodyToMono(String.class)
                  .onErrorMap(
                      ReadTimeoutException.class,
                      ex -> new JobEngineException("Engine server timeout", ex))
                  .block();
          JsonObject response = new Gson().fromJson(responseBody, JsonObject.class);
          LivyJobHandle jobHandle = new LivyJobHandle(client, response.get("id").getAsString());
          return jobHandle;
        },
        MetricsConstant.SUBMIT);
  }

  @Override
  public HouseJobHandle obtainHandle(String executionId) {
    return new LivyJobHandle(client, executionId);
  }

  public static String createSubmitRequestBodyString(JobLaunchConf conf) {
    JsonObject body = new JsonObject();
    body.addProperty("file", conf.getJarPath());
    body.addProperty("className", conf.getClassName());
    body.addProperty("proxyUser", conf.getProxyUser());
    body.add("args", new Gson().toJsonTree(conf.getArgs()).getAsJsonArray());
    body.add("jars", new Gson().toJsonTree(conf.getDependencies()).getAsJsonArray());
    body.add("conf", new Gson().toJsonTree(conf.getSparkProperties()));
    return body.toString();
  }

  private static WebClient getDefaultWebClient(String baseURL) {
    HttpClient httpClient =
        HttpClient.create()
            .responseTimeout(Duration.ofSeconds(ENGINE_API_RESPONSE_TIMEOUT_SECONDS));
    ClientHttpConnector connector = new ReactorClientHttpConnector(httpClient);
    return WebClient.builder()
        .baseUrl(baseURL)
        .clientConnector(connector)
        .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
        .build();
  }
}
