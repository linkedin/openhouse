package com.linkedin.openhouse.jobs.spark.optimizer;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
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
 */
@Slf4j
public class OptimizerServiceClient implements AutoCloseable {

  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private static final String UPDATE_PATH = "/v1/optimizer/operations/update";

  private final String baseUrl;
  private final OkHttpClient httpClient;
  private final ObjectMapper objectMapper;

  public OptimizerServiceClient(String baseUrl) {
    this(baseUrl, defaultClient(), new ObjectMapper());
  }

  OptimizerServiceClient(String baseUrl, OkHttpClient httpClient, ObjectMapper objectMapper) {
    this.baseUrl = stripTrailingSlash(baseUrl);
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

  private static OkHttpClient defaultClient() {
    return new OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .writeTimeout(30, TimeUnit.SECONDS)
        .build();
  }

  private static String stripTrailingSlash(String url) {
    if (url == null || url.isEmpty()) {
      throw new IllegalArgumentException("Optimizer Service base URL must be non-empty");
    }
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }
}
