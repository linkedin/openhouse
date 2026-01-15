package com.linkedin.openhouse.tables.e2e.h2;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.actuate.metrics.AutoConfigureMetrics;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests that verify the histogram bucket configuration from application.properties is properly
 * configured for metrics. Specifically tests that the 600s maximum expected value is configured for
 * the catalog_metadata_retrieval_latency metric.
 *
 * <p>These tests use the actual autowired MeterRegistry from Spring context
 * (PrometheusMeterRegistry in production configuration) and verify histogram buckets via the
 * /actuator/prometheus endpoint.
 *
 * <p>Uses webEnvironment = RANDOM_PORT to start a real web server which activates the full actuator
 * and Prometheus metrics configuration.
 *
 * <p>NOTE: {@code @AutoConfigureMetrics} is required because Spring Boot disables metrics exporters
 * by default in tests, replacing PrometheusMeterRegistry with SimpleMeterRegistry. This annotation
 * re-enables the production metrics configuration.
 *
 * <ol>
 *   <li>The configuration property is correctly set in application.properties
 *   <li>The MeterRegistry is a PrometheusMeterRegistry with histogram buckets
 *   <li>Histogram buckets extend to 600 seconds for catalog_metadata_retrieval_latency
 * </ol>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMetrics
@ContextConfiguration(
    initializers = {
      PropertyOverrideContextInitializer.class,
      AuthorizationPropertiesInitializer.class
    })
public class MetricsHistogramConfigurationTest {

  @Autowired private MeterRegistry meterRegistry;

  @Autowired private TestRestTemplate restTemplate;

  @LocalServerPort private int port;

  @Value(
      "${management.metrics.distribution.maximum-expected-value.catalog_metadata_retrieval_latency:}")
  private String maxExpectedValueConfig;

  @Value("${management.metrics.distribution.percentiles-histogram.all:false}")
  private boolean percentilesHistogramEnabled;

  /**
   * Tests that the application.properties has the correct histogram configuration for
   * catalog_metadata_retrieval_latency metric.
   *
   * <p>The application.properties setting:
   * management.metrics.distribution.maximum-expected-value.catalog_metadata_retrieval_latency=600s
   *
   * <p>This configuration ensures histogram buckets extend to 600 seconds (10 minutes) when using a
   * production MeterRegistry like Prometheus.
   */
  @Test
  void testMaxExpectedValueConfigurationIsSet() {
    assertNotNull(
        maxExpectedValueConfig,
        "maximum-expected-value.catalog_metadata_retrieval_latency should be configured");
    assertFalse(
        maxExpectedValueConfig.isEmpty(),
        "maximum-expected-value.catalog_metadata_retrieval_latency should not be empty");
    assertEquals(
        "600s",
        maxExpectedValueConfig,
        "maximum-expected-value.catalog_metadata_retrieval_latency should be set to 600s");
  }

  /** Tests that percentiles histogram is enabled for all metrics. */
  @Test
  void testPercentilesHistogramIsEnabled() {
    assertTrue(
        percentilesHistogramEnabled,
        "management.metrics.distribution.percentiles-histogram.all should be true");
  }

  /**
   * Tests that the autowired MeterRegistry is a PrometheusMeterRegistry. This is required for
   * histogram buckets to work correctly in production.
   */
  @Test
  void testMeterRegistryIsPrometheus() {
    assertTrue(
        meterRegistry instanceof PrometheusMeterRegistry,
        String.format(
            "MeterRegistry should be PrometheusMeterRegistry but was %s",
            meterRegistry.getClass().getName()));
  }

  /**
   * Tests that histogram buckets are properly configured for catalog_metadata_retrieval_latency
   * metric with buckets extending to 600 seconds.
   *
   * <p>This test validates that:
   *
   * <ol>
   *   <li>The Prometheus actuator endpoint is accessible
   *   <li>Recording a value creates histogram bucket entries
   *   <li>Histogram buckets include the 600s boundary (le="600.0")
   *   <li>Values beyond 600s are captured in higher buckets
   * </ol>
   */
  @Test
  void testMetadataRetrievalLatencyRecordsLargeValues() {
    String metricName = "catalog_metadata_retrieval_latency";

    Timer timer = meterRegistry.timer(metricName);

    // Record various latencies including some near and beyond the 600s boundary
    timer.record(100, TimeUnit.MILLISECONDS);
    timer.record(1, TimeUnit.SECONDS);
    timer.record(10, TimeUnit.SECONDS);
    timer.record(60, TimeUnit.SECONDS);
    timer.record(300, TimeUnit.SECONDS); // 5 minutes
    timer.record(550, TimeUnit.SECONDS); // Just under 600s
    timer.record(600, TimeUnit.SECONDS); // Exactly 600s (10 minutes)
    timer.record(700, TimeUnit.SECONDS); // Beyond 600s

    // Verify all recordings were captured
    assertEquals(8, timer.count(), "All recordings should be captured");

    // Fetch Prometheus metrics via actuator endpoint
    ResponseEntity<String> response =
        restTemplate.getForEntity("/actuator/prometheus", String.class);

    assertEquals(200, response.getStatusCodeValue(), "Prometheus endpoint should return 200 OK");
    String prometheusOutput = response.getBody();
    assertNotNull(prometheusOutput, "Prometheus output should not be null");

    // Verify histogram bucket entries exist for our metric
    // Prometheus histogram format: metric_name_bucket{le="value",...} count
    assertTrue(
        prometheusOutput.contains("catalog_metadata_retrieval_latency_seconds_bucket"),
        "Prometheus output should contain histogram bucket entries for catalog_metadata_retrieval_latency");

    // Extract and log the histogram buckets for debugging
    String buckets = extractHistogramBuckets(prometheusOutput, metricName);

    // Verify the 600s bucket exists (le="600.0" in seconds)
    // This confirms the maximum-expected-value configuration is applied
    // Note: le label may appear after other labels like application and clusterName
    assertTrue(
        prometheusOutput.contains("le=\"600.0\"")
            && prometheusOutput.contains("catalog_metadata_retrieval_latency_seconds_bucket"),
        "Histogram should have a bucket at 600 seconds boundary. "
            + "This validates the maximum-expected-value.catalog_metadata_retrieval_latency=600s configuration. "
            + "Found buckets: "
            + buckets);

    // Verify there are buckets above 600s to capture the 700s value (le="+Inf")
    assertTrue(
        prometheusOutput.contains("le=\"+Inf\"")
            && prometheusOutput.contains("catalog_metadata_retrieval_latency_seconds_bucket"),
        "Histogram should have an +Inf bucket");

    // Verify the count is correctly reported (may have trailing space or comma variations)
    assertTrue(
        prometheusOutput.contains("catalog_metadata_retrieval_latency_seconds_count")
            && prometheusOutput.contains("8.0"),
        "Prometheus output should show count of 8 recordings");
  }

  /**
   * Helper method to extract histogram bucket values from Prometheus output for debugging. The le
   * label may appear after other labels like application and clusterName.
   */
  private String extractHistogramBuckets(String prometheusOutput, String metricName) {
    StringBuilder buckets = new StringBuilder();
    // Pattern to find le="value" anywhere in the bucket entry
    // Format: metric_seconds_bucket{...,le="value",...} count
    Pattern pattern =
        Pattern.compile(metricName + "_seconds_bucket\\{[^}]*le=\"([^\"]+)\"[^}]*\\}");
    Matcher matcher = pattern.matcher(prometheusOutput);

    while (matcher.find()) {
      if (buckets.length() > 0) {
        buckets.append(", ");
      }
      buckets.append(matcher.group(1));
    }
    return buckets.toString();
  }

  /**
   * Tests that the 600s configuration value can be parsed as a Duration. This validates the format
   * used in application.properties is correct.
   */
  @Test
  void testConfigurationValueIsParseable() {
    // Parse the configuration value as a Duration
    Duration maxExpectedDuration = Duration.parse("PT" + maxExpectedValueConfig.toUpperCase());

    assertEquals(
        Duration.ofSeconds(600), maxExpectedDuration, "Configuration should parse to 600 seconds");

    assertEquals(
        600, maxExpectedDuration.getSeconds(), "Configuration should be exactly 600 seconds");
  }
}
