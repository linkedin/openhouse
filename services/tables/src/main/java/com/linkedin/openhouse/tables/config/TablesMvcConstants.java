package com.linkedin.openhouse.tables.config;

public final class TablesMvcConstants {
  public static final String HTTP_HEADER_CLIENT_NAME = "X-Client-Name";
  /** A SYSTEM declaration is required for otherwise-authorized access under a SYSTEM_ONLY lock. */
  public static final String HTTP_HEADER_ACTION_TYPE = "X-OpenHouse-Action-Type";
  /** {@code true} lifts column-default policy checks for the request. */
  public static final String HTTP_HEADER_COLUMN_DEFAULT_POLICY_BYPASS =
      "X-OpenHouse-Dangerously-Bypass-Column-Default-Policy";
  /** Ends a rejection that {@link #HTTP_HEADER_COLUMN_DEFAULT_POLICY_BYPASS} lifts. */
  public static final String COLUMN_DEFAULT_POLICY_BYPASS_HINT =
      " To bypass column-default policy checks, set header."
          + HTTP_HEADER_COLUMN_DEFAULT_POLICY_BYPASS
          + "=true on the client catalog; incorrect reads or writes may result.";

  public static final String ACTION_TYPE_SYSTEM = "SYSTEM";

  public static final String CLIENT_NAME_DEFAULT_VALUE = "unspecified";
  public static final String METRIC_KEY_CLIENT_NAME = "client_name";

  private TablesMvcConstants() {
    // Noop
  }
}
