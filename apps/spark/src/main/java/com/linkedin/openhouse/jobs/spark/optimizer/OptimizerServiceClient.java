package com.linkedin.openhouse.jobs.spark.optimizer;

import com.linkedin.openhouse.client.ssl.OptimizerApiClientFactory;
import com.linkedin.openhouse.jobs.util.RetryUtil;
import com.linkedin.openhouse.optimizer.client.api.TableOperationsControllerApi;
import com.linkedin.openhouse.optimizer.client.invoker.ApiClient;
import com.linkedin.openhouse.optimizer.client.model.TableOperationsHistory;
import com.linkedin.openhouse.optimizer.client.model.UpdateOperationRequest;
import java.net.MalformedURLException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;

/**
 * Thin wrapper around the generated optimizer-service {@link TableOperationsControllerApi}. Mirrors
 * the structure of {@link com.linkedin.openhouse.jobs.client.JobsClient}: a hand-written facade
 * over the auto-generated client, wired with a {@link RetryTemplate} that retries 5xx with
 * exponential backoff.
 *
 * <p>The batched Spark app calls {@link #updateOperation(String, UpdateOperationRequest)} once per
 * finished operation to record SUCCESS or FAILED. Per the design, a missed update is recoverable —
 * the operation row stays {@code SCHEDULED} and the Analyzer's stale-timeout will re-queue it — so
 * this client surfaces but does not swallow failures.
 */
@Slf4j
public class OptimizerServiceClient {

  private static final int REQUEST_TIMEOUT_SECONDS = 20;

  private final RetryTemplate retryTemplate;
  private final TableOperationsControllerApi api;

  public OptimizerServiceClient(String baseUrl) {
    this(baseUrl, null);
  }

  public OptimizerServiceClient(String baseUrl, String truststoreLocation) {
    this(RetryUtil.getOptimizerApiRetryTemplate(), buildApi(baseUrl, truststoreLocation));
  }

  OptimizerServiceClient(RetryTemplate retryTemplate, TableOperationsControllerApi api) {
    this.retryTemplate = retryTemplate;
    this.api = api;
  }

  /**
   * Reports a terminal status for {@code operationId}. The path id and the body's {@code
   * operationId} must match — callers should set both via the same value.
   *
   * @return the created history record, or {@link Optional#empty()} if the call failed after
   *     retries (logged; the caller decides what to do).
   */
  public Optional<TableOperationsHistory> updateOperation(
      String operationId, UpdateOperationRequest request) {
    Objects.requireNonNull(operationId, "operationId");
    Objects.requireNonNull(request, "request");
    return Optional.ofNullable(
        RetryUtil.executeWithRetry(
            retryTemplate,
            (RetryCallback<TableOperationsHistory, Exception>)
                context ->
                    api.updateOperation(operationId, request)
                        .block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS)),
            null));
  }

  private static TableOperationsControllerApi buildApi(String baseUrl, String truststoreLocation) {
    Objects.requireNonNull(baseUrl, "baseUrl");
    try {
      ApiClient apiClient =
          OptimizerApiClientFactory.getInstance()
              .createApiClient(baseUrl, null, truststoreLocation);
      return new TableOperationsControllerApi(apiClient);
    } catch (MalformedURLException | SSLException e) {
      throw new RuntimeException(
          "Failed to construct optimizer service client for baseUrl=" + baseUrl, e);
    }
  }
}
