package com.linkedin.openhouse.internal.catalog.repository;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.BackOffPolicyBuilder;

/** Common utilities for retrying service call, e.g. HTS call. */
@Slf4j
public final class HtsRetryUtils {
  private HtsRetryUtils() {
    // utilities class private ctor noop
  }

  public static final int MAX_RETRY_ATTEMPT = 3;

  public static final BackOffPolicy DEFAULT_HTS_BACKOFF_POLICY =
      BackOffPolicyBuilder.newBuilder()
          .multiplier(2.0)
          .delay(TimeUnit.SECONDS.toMillis(2))
          .maxDelay(TimeUnit.SECONDS.toMillis(30))
          .build();
}
