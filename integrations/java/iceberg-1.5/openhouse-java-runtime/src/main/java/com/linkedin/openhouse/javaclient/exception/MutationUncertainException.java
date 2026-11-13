package com.linkedin.openhouse.javaclient.exception;

public class MutationUncertainException extends CatalogOperationException {
  public MutationUncertainException(String message) {
    super(message);
  }

  public MutationUncertainException(Throwable cause) {
    super(
        "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying.",
        cause);
  }
}
