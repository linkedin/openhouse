package com.linkedin.openhouse.tables.config;

public final class TablesMvcConstants {
  public static final String HTTP_HEADER_CLIENT_NAME = "X-Client-Name";
  /** Declares a request as SYSTEM, which the default handler requires under a SYSTEM_ONLY lock. */
  public static final String HTTP_HEADER_ACTION_TYPE = "X-OpenHouse-Action-Type";

  public static final String ACTION_TYPE_SYSTEM = "SYSTEM";

  public static final String CLIENT_NAME_DEFAULT_VALUE = "unspecified";
  public static final String METRIC_KEY_CLIENT_NAME = "client_name";

  private TablesMvcConstants() {
    // Noop
  }
}
