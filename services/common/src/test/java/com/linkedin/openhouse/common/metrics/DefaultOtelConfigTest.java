package com.linkedin.openhouse.common.metrics;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DefaultOtelConfigTest {
  @BeforeEach
  @AfterEach
  void resetGlobal() {
    GlobalOpenTelemetry.resetForTest();
  }

  @Test
  void wrappedNoopDoesNotDisableStandaloneMetrics() {
    GlobalOpenTelemetry.set(OpenTelemetry.noop());
    Assertions.assertNotSame(OpenTelemetry.noop(), GlobalOpenTelemetry.get());

    try (OpenTelemetrySdk sdk =
        Assertions.assertInstanceOf(
            OpenTelemetrySdk.class, DefaultOtelConfig.initOpenTelemetry())) {
      Assertions.assertNotSame(MeterProvider.noop(), sdk.getMeterProvider());
    }
  }

  @Test
  void reusesInstalledMeterProvider() {
    OpenTelemetry agent = Mockito.mock(OpenTelemetry.class);
    Mockito.when(agent.getMeterProvider()).thenReturn(Mockito.mock(MeterProvider.class));
    GlobalOpenTelemetry.set(agent);

    Assertions.assertSame(GlobalOpenTelemetry.get(), DefaultOtelConfig.initOpenTelemetry());
  }

  @Test
  void shutdownExportsMetricsBeforeThePeriodicInterval() {
    RecordingExporter exporter = new RecordingExporter();
    try (SdkMeterProvider provider =
        SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter).setInterval(Duration.ofHours(1)).build())
            .build()) {
      Attributes attributes =
          Attributes.builder()
              .put("operation_type", "ORPHAN_FILES_DELETION")
              .put("status", "SUCCESS")
              .build();
      provider
          .get("batched-jobs")
          .counterBuilder("operation_terminal_count")
          .build()
          .add(1, attributes);

      DefaultOtelConfig.shutdownMetrics(provider);

      Assertions.assertTrue(exporter.shutdown);
      Assertions.assertEquals(1, exporter.metrics.size());
      MetricData metric = exporter.metrics.get(0);
      Assertions.assertEquals("operation_terminal_count", metric.getName());
      Assertions.assertEquals(1, metric.getLongSumData().getPoints().size());
      Assertions.assertEquals(1, metric.getLongSumData().getPoints().iterator().next().getValue());
      Assertions.assertEquals(
          attributes, metric.getLongSumData().getPoints().iterator().next().getAttributes());
    }
  }

  private static final class RecordingExporter implements MetricExporter {
    private final List<MetricData> metrics = new CopyOnWriteArrayList<>();
    private volatile boolean shutdown;

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
      return AggregationTemporality.DELTA;
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> data) {
      metrics.addAll(data);
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      shutdown = true;
      return CompletableResultCode.ofSuccess();
    }
  }
}
