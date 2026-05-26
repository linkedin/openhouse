package com.linkedin.openhouse.jobs.spark.optimizer;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * Thin OkHttp client for the Optimizer Service. The batched Spark app calls {@link
 * #updateOperation(OperationUpdateRequest)} once per finished operation to record SUCCESS or
 * FAILED.
 *
 * <p>Errors are surfaced as {@link IOException}; the caller decides whether to retry. Per the
 * design, a missed update is recoverable — the operation row stays SCHEDULED and the Analyzer's
 * stale-timeout will re-queue it.
 *
 * <p>Construct with the {@link Config} builder to override the default timeouts.
 */
@Slf4j
public class OptimizerServiceClient implements AutoCloseable {

  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private static final String UPDATE_PATH = "/v1/optimizer/operations/update";

  private final String baseUrl;
  private final OkHttpClient httpClient;
  private final ObjectMapper objectMapper;

  public OptimizerServiceClient(String baseUrl) {
    this(Config.builder().baseUrl(baseUrl).build());
  }

  public OptimizerServiceClient(Config config) {
    this(config.getBaseUrl(), clientFor(config), new ObjectMapper());
  }

  OptimizerServiceClient(String baseUrl, OkHttpClient httpClient, ObjectMapper objectMapper) {
    this.baseUrl = stripTrailingSlash(Objects.requireNonNull(baseUrl, "baseUrl"));
    this.httpClient = httpClient;
    this.objectMapper = objectMapper;
  }

  public void updateOperation(OperationUpdateRequest body) throws IOException {
    String url = baseUrl + UPDATE_PATH;
    String json = objectMapper.writeValueAsString(body);
    Request request = new Request.Builder().url(url).post(RequestBody.create(json, JSON)).build();
    try (Response response = httpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new IOException(
            String.format(
                "Optimizer Service update failed: url=%s status=%d operationId=%s",
                url, response.code(), body.getOperationId()));
      }
      log.info(
          "Reported operation update: operationId={} status={} httpStatus={}",
          body.getOperationId(),
          body.getStatus(),
          response.code());
    }
  }

  @Override
  public void close() {
    httpClient.dispatcher().executorService().shutdown();
    httpClient.connectionPool().evictAll();
  }

  private static OkHttpClient clientFor(Config config) {
    return new OkHttpClient.Builder()
        .connectTimeout(config.getConnectTimeoutSeconds(), TimeUnit.SECONDS)
        .readTimeout(config.getReadTimeoutSeconds(), TimeUnit.SECONDS)
        .writeTimeout(config.getWriteTimeoutSeconds(), TimeUnit.SECONDS)
        .build();
  }

  private static String stripTrailingSlash(String url) {
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }

  /** Tunable transport settings. Defaults match the previous hardcoded values. */
  @lombok.Getter
  @Builder
  public static class Config {
    private final String baseUrl;
    @Builder.Default private final long connectTimeoutSeconds = 10L;
    @Builder.Default private final long readTimeoutSeconds = 30L;
    @Builder.Default private final long writeTimeoutSeconds = 30L;
  }
}
