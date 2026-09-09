package com.linkedin.openhouse.internal.catalog.view.model;

/**
 * The operation a caller intends for one view commit.
 *
 * <p>Chosen explicitly by the caller and independent of the captured {@link
 * ViewCommitIntent#getBaseRow()} lookup result: a future SQL {@code CREATE OR REPLACE} caller may
 * pick either value from that same result. Version-neutral, so it loads under Iceberg 1.2.
 */
public enum ViewCommitOperation {
  CREATE,
  REPLACE
}
