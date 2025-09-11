package com.linkedin.openhouse.common.metrics;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGaugeBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * OpenHouseOtelEmitter is an implementation of the OtelEmitter interface that uses OpenTelemetry
 * for emitting metrics. It supports counting, timing, and gauging metrics across multiple
 * OpenTelemetry instances.
 */
public class OpenHouseOtelEmitter implements OtelEmitter {
  protected final List<OpenTelemetry> otels;

  public OpenHouseOtelEmitter(List<OpenTelemetry> otels) {
    this.otels = otels;
  }

  @Override
  public <T> T executeWithStats(
      Callable<T> callable, String scope, String metricPrefix, Attributes attributes)
      throws Exception {
    throw new UnsupportedOperationException(
        "executeWithStats is not supported in OpenHouseOtelEmitter.");
  }

  @Override
  public synchronized void count(String scope, String metric, long count, Attributes attributes) {
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
  public synchronized void time(String scope, String metric, long amount, Attributes attributes) {
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
  public synchronized void gauge(String scope, String metric, long value, Attributes attributes) {
    for (OpenTelemetry otel : otels) {
      Meter meter = otel.getMeter(scope);
      LongGaugeBuilder gaugeBuilder =
          meter.gaugeBuilder(metric).ofLongs().setUnit(TimeUnit.MILLISECONDS.toString());
      if (attributes == null) {
        gaugeBuilder.buildWithCallback(measurement -> measurement.record(value));
        return;
      } else {
        gaugeBuilder.buildWithCallback(measurement -> measurement.record(value, attributes));
      }
    }
  }
}
