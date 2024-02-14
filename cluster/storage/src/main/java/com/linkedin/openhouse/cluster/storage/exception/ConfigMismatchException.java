package com.linkedin.openhouse.cluster.storage.exception;

/**
 * An exception thrown when there are mismatch between configs or between config and injected beans.
 */
public class ConfigMismatchException extends RuntimeException {
  public ConfigMismatchException(String errorMsg, String entityA, String entityB) {
    super(String.format("%s, since %s collides with %s", errorMsg, entityA, entityB));
  }
}
