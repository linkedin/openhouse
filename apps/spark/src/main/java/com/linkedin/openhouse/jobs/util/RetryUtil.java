package com.linkedin.openhouse.jobs.util;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.BackOffPolicyBuilder;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.retry.support.RetryTemplateBuilder;
import org.springframework.web.reactive.function.client.WebClientResponseException;

/** Utility class for running operations with retry semantics. */
@Slf4j
public final class RetryUtil {
  private static final BackOffPolicy DEFAULT_JOBS_BACKOFF_POLICY =
      BackOffPolicyBuilder.newBuilder()
          .multiplier(2.0)
          .delay(TimeUnit.SECONDS.toMillis(5))
          .maxDelay(TimeUnit.SECONDS.toMillis(60))
          .build();

  private RetryUtil() {
    // private method of static utility class
  }

  public static RetryTemplate getJobsApiRetryTemplate() {
    RetryTemplateBuilder builder = new RetryTemplateBuilder();
    return builder
        .maxAttempts(5)
        .customBackoff(DEFAULT_JOBS_BACKOFF_POLICY)
        .retryOn(WebClientResponseException.InternalServerError.class)
        .build();
  }

  public static RetryTemplate getJobsStateApiRetryTemplate() {
    RetryTemplateBuilder builder = new RetryTemplateBuilder();
    return builder
        .maxAttempts(5)
        .customBackoff(DEFAULT_JOBS_BACKOFF_POLICY)
        .retryOn(WebClientResponseException.InternalServerError.class)
        .retryOn(WebClientResponseException.Conflict.class)
        .build();
  }

  public static RetryTemplate getTablesApiRetryTemplate() {
    RetryTemplateBuilder builder = new RetryTemplateBuilder();
    return builder
        .maxAttempts(5)
        .customBackoff(DEFAULT_JOBS_BACKOFF_POLICY)
        .retryOn(WebClientResponseException.InternalServerError.class)
        .build();
  }

  public static RetryTemplate getTrinoClientRetryTemplate() {
    RetryTemplateBuilder builder = new RetryTemplateBuilder();
    return builder
        .maxAttempts(5)
        .customBackoff(DEFAULT_JOBS_BACKOFF_POLICY)
        .retryOn(WebClientResponseException.InternalServerError.class)
        .build();
  }

  public static <T, E extends Throwable> T executeWithRetry(
      RetryTemplate retryTemplate, RetryCallback<T, E> callable, T defaultResult) {
    try {
      return retryTemplate.execute(callable);
    } catch (Throwable e) {
      log.error("Failed to execute callable", e);
      return defaultResult;
    }
  }
}
