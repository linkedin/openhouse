package com.linkedin.openhouse.tables.config;

public final class TablesMvcConstants {
  public static final String HTTP_HEADER_CLIENT_NAME = "X-Client-Name";
  /** Boolean system-action declaration; currently carried without changing lock enforcement. */
  public static final String HTTP_HEADER_SYSTEM_ACTION = "X-OpenHouse-System-Action";

  public static final String CLIENT_NAME_DEFAULT_VALUE = "unspecified";
  public static final String METRIC_KEY_CLIENT_NAME = "client_name";

  private TablesMvcConstants() {
    // Noop
  }
}
