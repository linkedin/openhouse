package com.linkedin.openhouse.jobs.util;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Metadata, table or table directory can extend from it. */
@Getter
@AllArgsConstructor
@EqualsAndHashCode
public abstract class Metadata {
  String creator;

  public abstract String getValue();
}
