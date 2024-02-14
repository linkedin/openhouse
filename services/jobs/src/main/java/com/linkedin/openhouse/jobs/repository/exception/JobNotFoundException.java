package com.linkedin.openhouse.jobs.repository.exception;

public class JobNotFoundException extends RuntimeException {
  public JobNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
