package com.linkedin.openhouse.javaclient;

/**
 * Checked failure from {@link ReadBridge#from} or {@link ReadBridge#apply}. {@link
 * OpenHouseTableOperations#loadMetadata} wraps it as Iceberg's {@code Tasks.UnrecoverableException}
 * so the metadata read is not retried.
 */
class ReadBridgeException extends Exception {

  enum Kind {
    UNUSABLE_CONFIG,
    CANNOT_BIND,
    UNUSABLE_METADATA
  }

  private final Kind kind;

  ReadBridgeException(Kind kind, String message, Throwable cause) {
    super(message, cause);
    this.kind = kind;
  }

  Kind getKind() {
    return kind;
  }

  static ReadBridgeException unusableConfig(String message, Throwable cause) {
    return new ReadBridgeException(Kind.UNUSABLE_CONFIG, message, cause);
  }

  static ReadBridgeException cannotBind(Throwable cause) {
    return new ReadBridgeException(
        Kind.CANNOT_BIND, "read-bridge: default cannot bind to column type", cause);
  }

  static ReadBridgeException unusableMetadata(String message, Throwable cause) {
    return new ReadBridgeException(Kind.UNUSABLE_METADATA, message, cause);
  }
}
