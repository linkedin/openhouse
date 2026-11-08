package com.linkedin.openhouse.internal.catalog.view;

import org.apache.iceberg.exceptions.BadRequestException;

/** A replacement's caller-supplied source dialect differs from the valid stored source dialect. */
public class ViewSourceDialectMismatchException extends BadRequestException {

  public ViewSourceDialectMismatchException(String databaseId, String viewId) {
    super(
        "Cannot replace view %s.%s: sourceDialect must match the current view", databaseId, viewId);
  }
}
