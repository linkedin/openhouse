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

  /** The only view representation type accepted by the /v1 views API. */
  public static final String SQL_VIEW_REPRESENTATION_TYPE = "sql";

  /**
   * Maximum length of a view or database identifier on the /v1 views API. Mirrors the
   * {@code @Size(max = 128)} bean constraint on the request body identifiers so a path identifier
   * cannot bypass the limit the body enforces.
   */
  public static final int MAX_VIEW_IDENTIFIER_LENGTH = 128;

  /**
   * Maximum size of a single view representation's SQL text, in UTF-8 bytes. Counted in bytes
   * rather than characters because the limit protects storage and transport, and a {@code @Size}
   * bean constraint would instead count UTF-16 characters and let a multibyte payload through.
   */
  public static final int MAX_VIEW_SQL_BYTES = 256 * 1024;

  /** Maximum size of a view schema document, in UTF-8 bytes. See {@link #MAX_VIEW_SQL_BYTES}. */
  public static final int MAX_VIEW_SCHEMA_BYTES = 512 * 1024;
}
