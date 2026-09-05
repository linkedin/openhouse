package com.linkedin.openhouse.internal.catalog.view.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * A single SQL definition of a view, in one dialect, as supplied by a caller.
 *
 * <p>Version-neutral by construction: it names no {@code org.apache.iceberg.view.*} type so it
 * remains loadable when the runtime supplies Iceberg 1.2.
 */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class SqlViewRepresentationIntent {

  private final String sql;

  private final String dialect;
}
