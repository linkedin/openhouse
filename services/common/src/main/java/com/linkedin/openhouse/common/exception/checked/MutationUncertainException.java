package com.linkedin.openhouse.common.exception.checked;

public class MutationUncertainException extends DomainException {
  public MutationUncertainException(String message) {
    super(message);
  }

  public MutationUncertainException(String message, Throwable cause) {
    super(message, cause);
  }
}
