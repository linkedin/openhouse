package com.linkedin.openhouse.common.exception;

import org.springframework.http.HttpStatus;

/** A lock request that conflicts with the table's active lock. */
public class LockConflictException extends CodedApiException {
  public LockConflictException(String message) {
    super(message);
  }

  @Override
  public HttpStatus getHttpStatus() {
    return HttpStatus.CONFLICT;
  }
}
