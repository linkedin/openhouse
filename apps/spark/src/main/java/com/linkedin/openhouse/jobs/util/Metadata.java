package com.linkedin.openhouse.jobs.util;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/** Metadata, table or table directory can extend from it. */
@Getter
@SuperBuilder
@EqualsAndHashCode
@ToString
public abstract class Metadata {
  @NonNull @Builder.Default protected String creator = "";

  public abstract String getEntityName();
}
