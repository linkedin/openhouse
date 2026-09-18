package com.linkedin.openhouse.javaclient.exception;

import java.util.Optional;

/** Checked failure from an OpenHouse table lock operation. */
public final class TableLockException extends Exception {
  private final Optional<Integer> statusCode;
  private final Optional<String> responseBody;

  public TableLockException(String message) {
    super(message);
    statusCode = Optional.empty();
    responseBody = Optional.empty();
  }

  public TableLockException(String message, Throwable cause) {
    super(message, cause);
    statusCode = Optional.empty();
    responseBody = Optional.empty();
  }

  public TableLockException(int statusCode, String responseBody, Throwable cause) {
    super(
        "Table lock request failed with HTTP "
            + statusCode
            + Optional.ofNullable(responseBody)
                .filter(body -> !body.isEmpty())
                .map(body -> ": " + body)
                .orElse(""),
        cause);
    this.statusCode = Optional.of(statusCode);
    this.responseBody = Optional.ofNullable(responseBody);
  }

  public Optional<Integer> getStatusCode() {
    return statusCode;
  }

  public Optional<String> getResponseBody() {
    return responseBody;
  }
}
