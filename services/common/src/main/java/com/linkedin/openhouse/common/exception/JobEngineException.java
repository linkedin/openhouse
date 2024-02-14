package com.linkedin.openhouse.common.exception;

/** Exception indicating failure in the jobs execution engine, e.g Livy endpoint */
public class JobEngineException extends RuntimeException {
  public JobEngineException(String message, Throwable cause) {
    super(message, cause);
  }

  public JobEngineException(String message) {
    super(message);
  }
}
