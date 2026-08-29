package com.linkedin.openhouse.housetables.services.model;

import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * A {@link UserViewQuery} plus the paging the v1 route always supplies. A separate type rather than
 * an optional "paged mode" flag on {@link UserViewQuery}, so an unpaged call cannot carry paging
 * and a paged call cannot omit it.
 */
@EqualsAndHashCode
@ToString
public final class PagedUserViewQuery {

  private final UserViewQuery query;

  private final int page;

  private final int size;

  private final String sortBy;

  private PagedUserViewQuery(UserViewQuery query, int page, int size, String sortBy) {
    this.query = query;
    this.page = page;
    this.size = size;
    this.sortBy = sortBy;
  }

  /**
   * @param sortBy null when the caller supplied no sort, in which case the service applies the
   *     route's documented default
   */
  public static PagedUserViewQuery of(UserViewQuery query, int page, int size, String sortBy) {
    if (query == null) {
      throw new IllegalArgumentException("query is required");
    }
    return new PagedUserViewQuery(query, page, size, sortBy);
  }

  public UserViewQuery getQuery() {
    return query;
  }

  public int getPage() {
    return page;
  }

  public int getSize() {
    return size;
  }

  public Optional<String> getSortBy() {
    return Optional.ofNullable(sortBy);
  }
}
