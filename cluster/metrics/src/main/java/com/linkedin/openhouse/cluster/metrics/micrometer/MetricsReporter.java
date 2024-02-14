package com.linkedin.openhouse.cluster.metrics.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

/**
 * MetricsReporter: Micrometer based class to keep a record of counters, gauges and timers. It uses
 * micrometer based global registry to create and register metrics which are pushed to prometheus.
 */
@RequiredArgsConstructor
public class MetricsReporter {
  private final MeterRegistry meterRegistry;
  private final String metricPrefix;
  private final List<Tag> commonTags;

  public static MetricsReporter of(String metricPrefix, String... commonTags) {
    return new MetricsReporter(Metrics.globalRegistry, metricPrefix, toTags(commonTags));
  }

  @SneakyThrows
  public <T> T executeWithStats(Callable<T> callable, String metric, String... tags) {
    return meterRegistry
        .timer(getFullMetricName(metric), getFullTags(tags))
        .recordCallable(callable);
  }

  public void executeWithStats(Runnable r, String metrics, String... tags) {
    meterRegistry.timer(getFullMetricName(metrics), getFullTags(tags)).record(r);
  }

  public void time(String metric, long amount, TimeUnit timeUnit, String... tags) {
    meterRegistry.timer(getFullMetricName(metric), getFullTags(tags)).record(amount, timeUnit);
  }

  public void gauge(String metric, double value, String... tags) {
    meterRegistry.gauge(getFullMetricName(metric), getFullTags(tags), value);
  }

  public void count(String metric, String... tags) {
    count(metric, 1.0, tags);
  }

  public void count(String metric, double amount, String... tags) {
    meterRegistry.counter(getFullMetricName(metric), getFullTags(tags)).increment(amount);
  }

  private String getFullMetricName(String metric) {
    return metricPrefix + "_" + metric;
  }

  private Iterable<Tag> getFullTags(String... tags) {
    List<Tag> ret = new ArrayList<>(commonTags);
    toTags(tags).forEach(ret::add);
    return ret;
  }

  private static List<Tag> toTags(String... tags) {
    List<Tag> ret = new ArrayList<>();
    for (int i = 0; i < tags.length; i += 2) {
      ret.add(Tag.of(tags[i], tags[i + 1]));
    }
    return ret;
  }
}
