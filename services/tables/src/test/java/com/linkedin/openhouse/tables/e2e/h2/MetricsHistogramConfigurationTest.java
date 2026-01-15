package com.linkedin.openhouse.tables.e2e.h2;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.CountAtBucket;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.actuate.metrics.AutoConfigureMetrics;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests that verify the histogram bucket configuration from application.properties is properly
 * configured for metrics. Specifically tests that the 600s maximum expected value is configured for
 * the catalog_metadata_retrieval_latency metric.
 *
 * <p>These tests use the actual autowired MeterRegistry from Spring context
 * (PrometheusMeterRegistry in production configuration) and verify histogram buckets by directly
 * inspecting the Timer's HistogramSnapshot.
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
@SpringBootTest
@AutoConfigureMetrics
@ContextConfiguration(
    initializers = {
      PropertyOverrideContextInitializer.class,
      AuthorizationPropertiesInitializer.class
    })
public class MetricsHistogramConfigurationTest {

  @Autowired private MeterRegistry meterRegistry;

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
   *   <li>Recording a value creates histogram bucket entries
   *   <li>Histogram buckets include a boundary at or above 600 seconds
   *   <li>Values are correctly counted
   * </ol>
   */
  @Test
  void testHistogramBucketsExtendTo600Seconds() {
    Timer timer = meterRegistry.timer("catalog_metadata_retrieval_latency");

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

    // Get histogram snapshot and inspect buckets directly
    HistogramSnapshot snapshot = timer.takeSnapshot();
    CountAtBucket[] buckets = snapshot.histogramCounts();

    assertTrue(buckets.length > 0, "Histogram should have bucket entries");

    // Find the maximum finite bucket boundary (in seconds)
    double maxBucketSeconds =
        Arrays.stream(buckets)
            .mapToDouble(b -> b.bucket(TimeUnit.SECONDS))
            .filter(b -> b != Double.POSITIVE_INFINITY)
            .max()
            .orElse(0);

    // Build bucket list string for assertion message
    String bucketList =
        Arrays.stream(buckets)
            .map(b -> String.valueOf(b.bucket(TimeUnit.SECONDS)))
            .reduce((a, b) -> a + ", " + b)
            .orElse("none");

    assertTrue(
        maxBucketSeconds >= 600.0,
        String.format(
            "Histogram should have buckets extending to at least 600s. "
                + "This validates the maximum-expected-value.catalog_metadata_retrieval_latency=600s configuration. "
                + "Found max bucket: %.1fs, all buckets: [%s]",
            maxBucketSeconds, bucketList));
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
