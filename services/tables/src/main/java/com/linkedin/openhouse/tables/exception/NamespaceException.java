package com.linkedin.openhouse.tables.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class NamespaceException extends Exception {
  private final HttpStatus status;
  private final String type;

  public NamespaceException(HttpStatus status, String type, String message) {
    super(message);
    this.status = status;
    this.type = type;
  }

  public NamespaceException(HttpStatus status, String type, String message, Throwable cause) {
    super(message, cause);
    this.status = status;
    this.type = type;
  }
}
