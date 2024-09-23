package com.linkedin.openhouse.jobs.util;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

/** Metadata, table or table directory can extend from it. */
@Getter
@SuperBuilder
@EqualsAndHashCode
public abstract class Metadata {
  protected String creator;

  public abstract String getEntityName();
}
