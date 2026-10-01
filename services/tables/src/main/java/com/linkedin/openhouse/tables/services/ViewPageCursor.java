package com.linkedin.openhouse.tables.services;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Opaque pagination cursor state (plan &sect;8, R3): the request database and canonical sort it was
 * issued for, the fixed internal source-page size, the current source-page index, and the
 * within-page offset at which to resume.
 */
@Getter
@EqualsAndHashCode
@ToString
public class ViewPageCursor {

  private final String databaseId;

  private final String sortBy;

  private final int pageSize;

  private final int sourcePageIndex;

  private final int offset;

  public ViewPageCursor(
      String databaseId, String sortBy, int pageSize, int sourcePageIndex, int offset) {
    this.databaseId = databaseId;
    this.sortBy = sortBy;
    this.pageSize = pageSize;
    this.sourcePageIndex = sourcePageIndex;
    this.offset = offset;
  }
}
