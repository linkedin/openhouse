package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.common.OtelEmitter;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGaugeBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class AppsOtelEmitter implements OtelEmitter {
  private final List<OpenTelemetry> otels;
  private static volatile AppsOtelEmitter instance;

  private AppsOtelEmitter(List<OpenTelemetry> otels) {
    this.otels = otels;
  }

  public static synchronized AppsOtelEmitter getInstance() {
    if (instance == null) {
      List<OpenTelemetry> otels = new ArrayList<>();
      otels.add(DefaultOtelConfig.getOpenTelemetry());
      instance = new AppsOtelEmitter(otels);
    }
    return instance;
  }

  @Override
  public <T> T executeWithStats(
      Callable<T> callable, String scope, String metricPrefix, Attributes attributes)
      throws Exception {
    long startTime = System.currentTimeMillis();
    String submitStatus = AppConstants.SUCCESS;
    try {
      return callable.call();
    } catch (Exception e) {
      submitStatus = AppConstants.FAIL;
      throw e;
    } finally {
      for (OpenTelemetry otel : otels) {
        Meter meter = otel.getMeter(scope);
        LongCounter counter = meter.counterBuilder(metricPrefix + "_count").build();
        counter.add(1, attributes.toBuilder().put(AppConstants.STATUS, submitStatus).build());
        LongHistogram histogram =
            meter
                .histogramBuilder(metricPrefix + "_latency")
                .ofLongs()
                .setUnit(TimeUnit.MILLISECONDS.name())
                .build();
        histogram.record(System.currentTimeMillis() - startTime, attributes);
      }
    }
  }

  @Override
  public void count(String scope, String metric, long count, Attributes attributes) {
    for (OpenTelemetry otel : otels) {
      Meter meter = otel.getMeter(scope);
      LongCounter counter = meter.counterBuilder(metric).build();
      if (attributes == null) {
        counter.add(count);
      } else {
        counter.add(count, attributes);
      }
    }
  }

  @Override
  public void time(String scope, String metric, long amount, Attributes attributes) {
    for (OpenTelemetry otel : otels) {
      Meter meter = otel.getMeter(scope);
      LongHistogram histogram =
          meter.histogramBuilder(metric).ofLongs().setUnit(TimeUnit.MILLISECONDS.name()).build();
      if (attributes == null) {
        histogram.record(amount);
      } else {
        histogram.record(amount, attributes);
      }
    }
  }

  @Override
  public void gauge(String scope, String metric, long value, Attributes attributes) {
    for (OpenTelemetry otel : otels) {
      Meter meter = otel.getMeter(scope);
      LongGaugeBuilder gaugeBuilder =
          meter
              .gaugeBuilder(AppConstants.RUN_DURATION_JOB)
              .ofLongs()
              .setUnit(TimeUnit.MILLISECONDS.toString());
      if (attributes == null) {
        gaugeBuilder.buildWithCallback(measurement -> measurement.record(value));
        return;
      } else {
        gaugeBuilder.buildWithCallback(measurement -> measurement.record(value, attributes));
      }
    }
  }
}
