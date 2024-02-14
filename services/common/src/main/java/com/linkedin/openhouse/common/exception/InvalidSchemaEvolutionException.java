package com.linkedin.openhouse.common.exception;

/** Invalid schema evolution detected from the underlying table format. */
public class InvalidSchemaEvolutionException extends RuntimeException {
  private String tableUri;
  private String offendingSchema;
  private String previousSchema;

  // Usually contains richer information from underlying table format
  private final Throwable cause;

  private static final String ERROR_MSG_TEMPLATE =
      "$tableUri with $offendingSchema contains invalid schema evolution"
          + " on top of current table schema: $currentSchema, "
          + "nested exception : $nested";

  public InvalidSchemaEvolutionException(
      String tableUri, String offendingSchema, String currentSchema) {
    this(
        tableUri,
        offendingSchema,
        currentSchema,
        null,
        ERROR_MSG_TEMPLATE
            .replace("$tableUri", tableUri)
            .replace("$offendingSchema", offendingSchema)
            .replace("$currentSchema", currentSchema));
  }

  public InvalidSchemaEvolutionException(
      String tableUri, String offendingSchema, String previousSchema, Throwable cause) {
    this(
        tableUri,
        offendingSchema,
        previousSchema,
        cause,
        ERROR_MSG_TEMPLATE
            .replace("$tableUri", tableUri)
            .replace("$offendingSchema", offendingSchema)
            .replace("$currentSchema", previousSchema)
            .replace("$nested", cause.getMessage()));
  }

  public InvalidSchemaEvolutionException(
      String tableUri, String offendingSchema, String previousSchema, String errorMsg) {
    this(tableUri, offendingSchema, previousSchema, null, errorMsg);
  }

  public InvalidSchemaEvolutionException(
      String tableUri,
      String offendingSchema,
      String previousSchema,
      Throwable cause,
      String errorMsg) {
    super(errorMsg);
    this.cause = cause;
    this.tableUri = tableUri;
    this.offendingSchema = offendingSchema;
    this.previousSchema = previousSchema;
  }
}
