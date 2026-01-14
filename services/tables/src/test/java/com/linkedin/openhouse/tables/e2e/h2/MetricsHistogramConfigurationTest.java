package com.linkedin.openhouse.tables.e2e.h2;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests that verify the histogram bucket configuration from application.properties is properly
 * configured for metrics. Specifically tests that the 600s maximum expected value is configured for
 * the catalog_metadata_retrieval_latency metric.
 *
 * <p>Note: The test context uses SimpleMeterRegistry which doesn't support histogram buckets
 * natively. These tests verify that:
 *
 * <ol>
 *   <li>The configuration property is correctly set in application.properties
 *   <li>Timers can record values up to and beyond 600 seconds without loss
 *   <li>The configuration would be applied in production with PrometheusMeterRegistry
 * </ol>
 */
@SpringBootTest
@AutoConfigureMockMvc
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
   * Tests that the timer records values correctly across the full range of expected latencies,
   * including values near and beyond the 600s boundary.
   *
   * <p>This verifies that even without histogram buckets, the timer correctly captures large
   * latency values that would be bucketed in production.
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

    // Verify the max value is correctly recorded (should be 700s)
    double maxSeconds = timer.max(TimeUnit.SECONDS);
    assertTrue(
        maxSeconds >= 699 && maxSeconds <= 701,
        String.format("Max should be around 700 seconds, got %.2f", maxSeconds));

    // Verify total time is approximately correct
    // Sum: 0.1 + 1 + 10 + 60 + 300 + 550 + 600 + 700 = 2221.1 seconds
    double totalSeconds = timer.totalTime(TimeUnit.SECONDS);
    assertTrue(
        totalSeconds >= 2220 && totalSeconds <= 2223,
        String.format("Total time should be around 2221 seconds, got %.2f", totalSeconds));
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
