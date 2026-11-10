package com.linkedin.openhouse.internal.catalog.view;

import org.apache.iceberg.exceptions.BadRequestException;

public class ViewSystemPropertiesMismatchException extends BadRequestException {
  public ViewSystemPropertiesMismatchException() {
    super("System view properties cannot be added, changed or removed");
  }
}
