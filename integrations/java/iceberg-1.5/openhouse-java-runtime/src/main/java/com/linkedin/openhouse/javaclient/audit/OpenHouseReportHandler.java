package com.linkedin.openhouse.javaclient.audit;

/**
 * interface to handle Iceberg audit events. mandates handling commitEvent and ScanEvent for Iceberg
 * Audit events
 */
public interface OpenHouseReportHandler<MetricsReport> {
  void audit(MetricsReport metricsReport);
}
