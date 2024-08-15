package com.linkedin.openhouse.javaclient;

import com.linkedin.openhouse.javaclient.audit.OpenHouseReportHandler;
import com.linkedin.openhouse.javaclient.audit.impl.OpenHouseReportLogPublish;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * OpenHouseMetricsReporter receives audit events from Iceberg commit and scan operations. Plugged
 * to OpenHouseCatalog by passing a property with the key metrics-reporter-impl=
 * com.linkedin.openhouse.javaclient.OpenHouseMetricsReporter property
 */
@Slf4j
public class OpenHouseMetricsReporter implements MetricsReporter {

  protected OpenHouseReportHandler<MetricsReport> auditEventHandler;
  private static final OpenHouseMetricsReporter INSTANCE = new OpenHouseMetricsReporter();

  public OpenHouseMetricsReporter() {}

  public static OpenHouseMetricsReporter instance() {
    return INSTANCE;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    log.info("Initializing custom metrics reporter for Iceberg: {}", this.getClass().getName());
    this.auditEventHandler = OpenHouseReportLogPublish.builder().build();
  }

  @Override
  public void report(MetricsReport report) {
    Preconditions.checkArgument(null != report, "Invalid metrics report: null");
    auditEventHandler.audit(report);
  }
}
