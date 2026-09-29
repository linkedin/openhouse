package com.linkedin.openhouse.tables.config;

public final class TablesMvcConstants {
  public static final String HTTP_HEADER_CLIENT_NAME = "X-Client-Name";
  /** Request action-type declaration; currently carried without changing lock enforcement. */
  public static final String HTTP_HEADER_ACTION_TYPE = "X-OpenHouse-Action-Type";

  public static final String CLIENT_NAME_DEFAULT_VALUE = "unspecified";
  public static final String METRIC_KEY_CLIENT_NAME = "client_name";

  private TablesMvcConstants() {
    // Noop
  }
}
