package com.linkedin.openhouse.tables.config;

import com.linkedin.openhouse.common.utils.ActionTypeContext;

public final class TablesMvcConstants {
  public static final String HTTP_HEADER_CLIENT_NAME = "X-Client-Name";
  /** A SYSTEM declaration is required for otherwise-authorized access under a SYSTEM_ONLY lock. */
  public static final String HTTP_HEADER_ACTION_TYPE = ActionTypeContext.HTTP_HEADER_ACTION_TYPE;

  public static final String CLIENT_NAME_DEFAULT_VALUE = "unspecified";
  public static final String METRIC_KEY_CLIENT_NAME = "client_name";

  private TablesMvcConstants() {
    // Noop
  }
}
