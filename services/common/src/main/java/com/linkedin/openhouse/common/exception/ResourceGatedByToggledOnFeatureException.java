package com.linkedin.openhouse.common.exception;

/**
 * The exception thrown when the request feature of a resource is toggled on thus being gated access
 * or modification.
 */
public class ResourceGatedByToggledOnFeatureException extends RuntimeException {
  public ResourceGatedByToggledOnFeatureException(
      String featureId, String databaseId, String tableId, Throwable throwable) {
    this(
        String.format(
            "Feature [%s] has been toggled on for the [%s.%s]", featureId, databaseId, tableId),
        throwable);
  }

  public ResourceGatedByToggledOnFeatureException(
      String featureId, String databaseId, String tableId) {
    this(
        String.format(
            "Feature [%s] has been toggled on for the [%s.%s]", featureId, databaseId, tableId));
  }

  public ResourceGatedByToggledOnFeatureException(String message, Throwable throwable) {
    super(message, throwable);
  }

  public ResourceGatedByToggledOnFeatureException(String message) {
    super(message);
  }
}
