package com.linkedin.openhouse.internal.catalog.view.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** A single SQL definition of a view, in one dialect, as supplied by a caller. */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class SqlViewRepresentationIntent {

  private final String sql;

  private final String dialect;
}
