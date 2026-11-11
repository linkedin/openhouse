package com.linkedin.openhouse.tables.audit.model;

/** The operation represented by a view operation audit event. */
public enum ViewOperationType {
  CREATE,
  REPLACE,
  DROP,
  /** The PUT route failed before state capture resolved whether it was a create or replace. */
  CREATE_OR_REPLACE
}
