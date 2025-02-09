package com.linkedin.openhouse.common.api.validator;

public final class ValidatorConstants {

  private ValidatorConstants() {}

  public static final String ALPHA_NUM_UNDERSCORE_PATTERN_SEARCH_REGEX = "^%?[a-zA-Z0-9_]+%?$";

  public static final String ALPHA_NUM_UNDERSCORE_PATTERN_SEARCH_ERROR_MSG =
      "Only alphanumerics and underscore supported. The wildcard '%' can only be at the beginning or end of the string";

  public static final String ALPHA_NUM_UNDERSCORE_REGEX = "^[a-zA-Z0-9_]+$";
  public static final String ALPHA_NUM_UNDERSCORE_ERROR_MSG =
      "Only alphanumerics and underscore supported";

  public static final String ALPHA_NUM_UNDERSCORE_REGEX_HYPHEN_ALLOW = "^[a-zA-Z0-9-_]+$";
  // supported memory format: Integer values ending with G or M
  public static final String ALPHA_NUM_UNDERSCORE_ERROR_MSG_HYPHEN_ALLOW =
      "Only alphanumerics, hyphen and underscore supported";
  public static final int MAX_ALLOWED_CLUSTERING_COLUMNS = 4;
  public static final String INITIAL_TABLE_VERSION = "INITIAL_VERSION";
}
