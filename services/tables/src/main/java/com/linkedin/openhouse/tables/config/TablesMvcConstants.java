package com.linkedin.openhouse.tables.config;

import com.linkedin.openhouse.common.utils.SystemActionContext;

public final class TablesMvcConstants {
  public static final String HTTP_HEADER_CLIENT_NAME = "X-Client-Name";
  /** Required for otherwise-authorized data access while a cleanup lock is active. */
  public static final String HTTP_HEADER_SYSTEM_ACTION =
      SystemActionContext.HTTP_HEADER_SYSTEM_ACTION;

  public static final String CLIENT_NAME_DEFAULT_VALUE = "unspecified";
  public static final String METRIC_KEY_CLIENT_NAME = "client_name";

  private TablesMvcConstants() {
    // Noop
  }
}
