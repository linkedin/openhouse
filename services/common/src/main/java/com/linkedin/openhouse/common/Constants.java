package com.linkedin.openhouse.common;

import java.util.Arrays;
import java.util.List;

public class Constants {
  public static final String HTTPHEADER_CLIENT_NAME = "X-Client-Name";

  public static final String CLIENT_NAME_DEFAULT_VALUE = "";
  public static final String METRIC_KEY_CLIENT_NAME = "client_name";
  public static final List<String> ALLOWED_CLIENT_NAME_VALUES = Arrays.asList("trino", "spark");

  private Constants() {
    // Noop
  }
}
