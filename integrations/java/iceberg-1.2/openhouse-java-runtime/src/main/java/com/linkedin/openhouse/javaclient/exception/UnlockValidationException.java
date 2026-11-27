package com.linkedin.openhouse.javaclient.exception;

/** Invalid UNLOCK input, rejected before making a catalog request. */
public class UnlockValidationException extends Exception {
  public UnlockValidationException(String message) {
    super(message);
  }
}
