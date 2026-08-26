package com.linkedin.openhouse.javaclient;

/**
 * Checked failure from {@link ReadBridge#from} or {@link ReadBridge#apply}. {@link
 * OpenHouseTableOperations#loadMetadata} wraps it as Iceberg's {@code Tasks.UnrecoverableException}
 * so the metadata read is not retried.
 */
class ReadBridgeException extends Exception {

  ReadBridgeException(String message, Throwable cause) {
    super(message, cause);
  }
}
