package com.linkedin.openhouse.housetables.model;

/** The replacement document does not identify the stored namespace. */
public class InvalidNamespaceDocumentException extends RuntimeException {
  public InvalidNamespaceDocumentException() {
    super("Invalid namespace document");
  }
}
