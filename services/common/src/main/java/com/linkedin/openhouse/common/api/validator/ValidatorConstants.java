package com.linkedin.openhouse.common.api.validator;

public final class ValidatorConstants {

  private ValidatorConstants() {}

  public static final String ALPHA_NUM_UNDERSCORE_REGEX = "^[a-zA-Z0-9_]+$";
  public static final String ALPHA_NUM_UNDERSCORE_ERROR_MSG =
      "Only alphanumerics and underscore supported";

  public static final String ALPHA_NUM_UNDERSCORE_REGEX_HYPHEN_ALLOW = "^[a-zA-Z0-9-_]+$";
  // supported memory format: Integer values ending with G or M
  public static final String ALPHA_NUM_UNDERSCORE_ERROR_MSG_HYPHEN_ALLOW =
      "Only alphanumerics, hyphen and underscore supported";
  public static final String SPARK_MEMORY_REGEX_ALLOW = "^(?!0[MG])(\\d+[MG])$";
  public static final String SPARK_MEMORY_ERROR_MSG_ALLOW =
      "Only positive integer values ending with G or M supported";
  public static final int MAX_ALLOWED_CLUSTERING_COLUMNS = 4;
  public static final String INITIAL_TABLE_VERSION = "INITIAL_VERSION";
  public static final String JOB_MEMORY_CONFIG = "memory";
}
