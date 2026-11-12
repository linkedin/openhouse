package com.linkedin.openhouse.javaclient.exception;

import java.util.Optional;

public class NamespaceMetadataMutationUncertainException extends NamespaceMetadataException {
  public NamespaceMetadataMutationUncertainException(String message) {
    super(0, message);
  }

  public NamespaceMetadataMutationUncertainException(int status, Throwable cause) {
    super(
        status,
        "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying.",
        cause,
        Optional.of("CommitStateUnknownException"));
  }
}
