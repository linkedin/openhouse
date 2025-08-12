package com.linkedin.openhouse.common.metrics;

import io.opentelemetry.api.common.Attributes;
import java.util.concurrent.Callable;

/** Interface for emitting OpenTelemetry metrics. */
public interface OtelEmitter {
  <T> T executeWithStats(
      Callable<T> callable, String scope, String metricPrefix, Attributes attributes)
      throws Exception;

  void count(String scope, String metric, long count, Attributes attributes);

  void time(String scope, String metric, long amount, Attributes attributes);

  void gauge(String scope, String metric, long value, Attributes attributes);
}
