package com.linkedin.openhouse.common.exception;

/**
 * The exception thrown when the request feature is toggled off thus not granting access or
 * modification.
 */
public class RequestedFeatureToggledOffException extends RuntimeException {
  public RequestedFeatureToggledOffException(
      String featureId, String databaseId, String tableId, Throwable throwable) {
    this(
        String.format(
            "Feature [%s] has been toggled off for the [%s.%s]", featureId, databaseId, tableId),
        throwable);
  }

  public RequestedFeatureToggledOffException(String featureId, String databaseId, String tableId) {
    this(
        String.format(
            "Feature [%s] has been toggled off for the [%s.%s]", featureId, databaseId, tableId));
  }

  public RequestedFeatureToggledOffException(String message, Throwable throwable) {
    super(message, throwable);
  }

  public RequestedFeatureToggledOffException(String message) {
    super(message);
  }
}
