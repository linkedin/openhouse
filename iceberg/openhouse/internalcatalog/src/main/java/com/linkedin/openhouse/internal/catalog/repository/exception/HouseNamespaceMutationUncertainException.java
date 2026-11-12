package com.linkedin.openhouse.internal.catalog.repository.exception;

public class HouseNamespaceMutationUncertainException extends HouseNamespaceException {
  public HouseNamespaceMutationUncertainException() {
    super("Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying.");
  }

  public HouseNamespaceMutationUncertainException(Throwable cause) {
    super(
        "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying.",
        cause);
  }
}
