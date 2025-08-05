package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.common.metrics.OpenHouseOtelEmitter;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AppsOtelEmitter extends OpenHouseOtelEmitter {
  public AppsOtelEmitter(List<OpenTelemetry> otels) {
    super(otels);
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
}
