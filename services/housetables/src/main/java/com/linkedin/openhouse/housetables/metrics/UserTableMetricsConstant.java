package com.linkedin.openhouse.housetables.metrics;

/**
 * Metric names owned by House Tables' view query paths.
 *
 * <p>Only the four names the two-state owned query design can actually emit are here. PR #697's
 * general-search view metrics are omitted deliberately: after validation every non-key filter is
 * null, so a general-search branch is unreachable, and {@code UserViewQuery}/{@code
 * PagedUserViewQuery} make it unrepresentable even to a direct service caller.
 *
 * <p>Existing table metric names stay in {@code services:common}; moving them is a separate
 * dependency-cleanup change.
 */
public final class UserTableMetricsConstant {

  private UserTableMetricsConstant() {
    // Utility class, constructor does nothing
  }

  public static final String HTS_LIST_VIEWS_REQUEST = "hts_list_views_request";
  public static final String HTS_LIST_VIEWS_TIME = "hts_list_views_time";
  public static final String HTS_PAGE_VIEWS_REQUEST = "hts_page_views_request";
  public static final String HTS_PAGE_VIEWS_TIME = "hts_page_views_time";
}
