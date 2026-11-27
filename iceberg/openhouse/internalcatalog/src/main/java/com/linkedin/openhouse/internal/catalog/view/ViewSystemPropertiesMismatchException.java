package com.linkedin.openhouse.internal.catalog.view;

import org.apache.iceberg.exceptions.BadRequestException;

/** Rejects an update that changes or omits server-owned view properties. */
public class ViewSystemPropertiesMismatchException extends BadRequestException {
  public ViewSystemPropertiesMismatchException() {
    super("System view properties cannot be added, changed or removed");
  }
}
