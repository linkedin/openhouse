package com.linkedin.openhouse.jobs.util;

import static io.opentelemetry.semconv.resource.attributes.ResourceAttributes.*;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Duration;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

@Slf4j
public final class DefaultOtelConfig {
  private static final int METRIC_READER_INTERVAL_MS = 1000;
  private static OpenTelemetry otel;

  private DefaultOtelConfig() {}

  public static synchronized OpenTelemetry getOpenTelemetry() {
    if (otel == null) {
      otel = initOpenTelemetry();
    }
    return otel;
  }

  private static Attributes getCommonAttributes() {
    AttributesBuilder attributesBuilder = Attributes.builder();
    String appName = System.getenv("APP_NAME");
    String clusterName = System.getenv("CLUSTER_NAME");
    if (StringUtils.isNotEmpty(appName)) {
      attributesBuilder.put(SERVICE_NAME, appName);
    }
    if (StringUtils.isNotEmpty(clusterName)) {
      attributesBuilder.put("cluster_name", clusterName);
    }
    // adding these attrs explicitly as resource_attributes are not merged automatically to Otel
    // Resource. See bug- https://github.com/open-telemetry/opentelemetry-java/issues/5238 for more
    // details
    // eg: set as ENV variable
    // OTEL_RESOURCE_ATTRIBUTES="service.name=jobs-scheduler,k8s.pod.name=<pod.name>,k8s.pod.uuid=<pod.uuid>"
    String resourceAttrs =
        StringUtils.defaultIfEmpty(System.getenv("OTEL_RESOURCE_ATTRIBUTES"), null);

    if (resourceAttrs != null && !resourceAttrs.isEmpty()) {
      Arrays.stream(resourceAttrs.split(","))
          .map(pair -> pair.split("="))
          .filter(keyValue -> keyValue.length == 2)
          .forEach(keyValue -> attributesBuilder.put(keyValue[0].trim(), keyValue[1].trim()));
    }
    return attributesBuilder.build();
  }

  /**
   * Initialize OpenTelemetry SDK with metrics exporter only which pushes metrics to
   * collector @link{COLLECTOR_ENDPOINT}
   */
  private static OpenTelemetry initOpenTelemetry() {
    log.info("initializing open-telemetry sdk");
    Resource resource = Resource.getDefault().merge(Resource.create(getCommonAttributes()));

    // Every Meter is provided from SdkMeterProvider in OpenTelemetrySdk.
    // All metrics are exported using an exporter component which exports metrics
    // proto to a collector, URL for which is set in exporter.
    // Default exporter configuration expects a collector at http://localhost:4318 for http
    // protocol.
    // This can be setup following
    // https://github.com/open-telemetry/opentelemetry-java-docs/tree/main/otlp/docker and adding
    // http protocol here:
    // https://github.com/open-telemetry/opentelemetry-java-docs/blob/main/otlp/docker/otel-collector-config-demo.yaml#L4

    String collectorEndpoint =
        StringUtils.defaultIfEmpty(System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), null);
    // temporality options available are delta or cumulative, cumulative is default
    // https://opentelemetry.io/docs/reference/specification/metrics/data-model/#temporality

    String preferredTemporality =
        StringUtils.defaultIfEmpty(
            System.getenv("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE"), null);

    OtlpHttpMetricExporter exporter;
    if (collectorEndpoint == null) {
      exporter = OtlpHttpMetricExporter.getDefault();
    } else {
      OtlpHttpMetricExporterBuilder builder = OtlpHttpMetricExporter.builder();
      builder.setEndpoint(getMetricsUrl(collectorEndpoint));
      if ("delta".equalsIgnoreCase(preferredTemporality)) {
        builder.setAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred());
      }
      exporter = builder.build();
    }

    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder()
            .setResource(resource)
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setInterval(Duration.ofMillis(METRIC_READER_INTERVAL_MS))
                    .build())
            .build();

    OpenTelemetrySdk openTelemetrySdk =
        OpenTelemetrySdk.builder().setMeterProvider(meterProvider).buildAndRegisterGlobal();

    Runtime.getRuntime()
        .addShutdownHook(new Thread(openTelemetrySdk.getSdkMeterProvider()::shutdown));
    return openTelemetrySdk;
  }

  @SuppressWarnings("checkstyle:ParameterAssignment")
  private static String getMetricsUrl(String baseUrl) {
    if (!baseUrl.endsWith("/")) {
      baseUrl += "/";
    }
    return baseUrl + "v1/metrics";
  }
}
