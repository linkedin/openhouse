package com.linkedin.openhouse.tables.config;

import java.util.Arrays;
import java.util.List;

public class TablesMvcConstants {
  public static final String HTTP_HEADER_CLIENT_NAME = "X-Client-Name";

  public static final String CLIENT_NAME_DEFAULT_VALUE = "unspecified";
  public static final String METRIC_KEY_CLIENT_NAME = "client_name";
  public static final List<String> ALLOWED_CLIENT_NAME_VALUES = Arrays.asList("trino", "spark");

  private TablesMvcConstants() {
    // Noop
  }
}
