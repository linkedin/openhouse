package com.linkedin.openhouse.javaclient.audit.impl;

import com.linkedin.openhouse.javaclient.audit.OpenHouseReportHandler;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CommitReportParser;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.ScanReportParser;

@Builder
@Slf4j
public class OpenHouseReportLogPublish implements OpenHouseReportHandler<MetricsReport> {
  @Override
  public void audit(MetricsReport metricsReport) {
    if (metricsReport instanceof ScanReport) {
      log.info("------------------------------");
      log.info(ScanReportParser.toJson((ScanReport) metricsReport));
    } else if (metricsReport instanceof CommitReport) {
      log.info("------------------------------");
      log.info(CommitReportParser.toJson((CommitReport) metricsReport));
    } else {
      log.info("Metrics Report not handled");
    }
  }
}
