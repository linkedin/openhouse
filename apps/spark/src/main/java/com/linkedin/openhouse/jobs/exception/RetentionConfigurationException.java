package com.linkedin.openhouse.jobs.exception;

public class RetentionConfigurationException extends Exception {
  public RetentionConfigurationException(String message) {
    super(message);
  }

  public RetentionConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
}
