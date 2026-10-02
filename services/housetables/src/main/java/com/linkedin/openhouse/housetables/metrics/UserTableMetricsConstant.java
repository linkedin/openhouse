package com.linkedin.openhouse.housetables.metrics;

/**
 * Metric names owned by House Tables' view query paths: only the four the owned query types can
 * actually emit. Existing table metric names stay in {@code services:common} for now.
 */
public final class UserTableMetricsConstant {

  private UserTableMetricsConstant() {
    // Utility class, constructor does nothing
  }

  public static final String HTS_PAGE_VIEWS_REQUEST = "hts_page_views_request";
  public static final String HTS_PAGE_VIEWS_TIME = "hts_page_views_time";
}
