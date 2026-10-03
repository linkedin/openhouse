package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewListResult;
import java.util.ArrayList;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

/**
 * Bridge-only opaque source-page-index + intra-page-offset cursor over the existing
 * page-number-based {@code findAllViewsByDatabaseId} route.
 *
 * <p>The HTS validator rejects any {@code sortBy} containing a comma or colon, so no multi-column
 * tie-breaker can be sent. The only supported caller sort is the public {@code viewId}, mapped to
 * the HTS {@code tableId} column; any other nonblank sort is a fixed service 400 before HTS. The
 * cursor is expressed in fixed-size source-page coordinates (not caller size or absolute row
 * index), so a changed {@code size} on a later continuation neither skips nor repeats rows, and a
 * short or empty non-terminal source page is handled without assuming full pages.
 */
public class ViewPaginationAdapter {

  /** Fixed internal source-page size, independent of the caller's requested {@code size}. */
  public static final int DEFAULT_SOURCE_PAGE_SIZE = 50;

  /** The only supported public sort field, mapped to the HTS {@code tableId} column. */
  private static final String CANONICAL_SORT = "viewId";

  private static final String HTS_SORT_PROPERTY = "tableId";

  /**
   * Bound on how many source pages a single {@link #list} call will fetch while chasing a run of
   * empty non-terminal pages, so a pathological source cannot hang the request. The call returns a
   * forward continuation token instead of blocking past this bound.
   */
  private static final int MAX_SOURCE_PAGE_ADVANCES_PER_CALL = 50;

  private final ViewPageSource source;

  private final int sourcePageSize;

  private final ViewPageTokenCodec codec = new ViewPageTokenCodec();

  public ViewPaginationAdapter(ViewPageSource source, int sourcePageSize) {
    this.source = source;
    this.sourcePageSize = sourcePageSize;
  }

  public ViewListResult list(String databaseId, String pageToken, int size, String sortBy) {
    String canonicalSort = normalizeSort(sortBy);
    ViewPageCursor cursor =
        pageToken == null
            ? new ViewPageCursor(databaseId, canonicalSort, sourcePageSize, 0, 0)
            : decodeAndValidate(pageToken, databaseId, canonicalSort);

    // Not new ArrayList<>(size): size is an uncapped caller-supplied wire value (the approved
    // contract accepts any positive int, including Integer.MAX_VALUE), but no single source page
    // ever contributes more than sourcePageSize rows, so a capacity request proportional to size
    // rather than to rows actually available can reserve enormous, mostly-wasted heap for a small
    // or empty result. Starting at the default capacity and letting the list grow (amortized O(1)
    // per add, same as any other unbounded accumulation in this codebase) scales with rows
    // actually returned instead.
    List<ViewDto> results = new ArrayList<>();
    int pageIndex = cursor.getSourcePageIndex();
    int offset = cursor.getOffset();
    int advances = 0;

    while (true) {
      Page<ViewDto> page =
          source.list(
              databaseId, PageRequest.of(pageIndex, sourcePageSize, Sort.by(HTS_SORT_PROPERTY)));
      List<ViewDto> content = page.getContent();
      int available = content.size() - offset;
      if (available > 0) {
        int take = Math.min(available, size - results.size());
        results.addAll(content.subList(offset, offset + take));
        offset += take;
      }

      if (results.size() >= size) {
        String nextToken;
        if (offset < content.size()) {
          nextToken =
              codec.encode(
                  new ViewPageCursor(databaseId, canonicalSort, sourcePageSize, pageIndex, offset));
        } else if (page.hasNext()) {
          nextToken =
              codec.encode(
                  new ViewPageCursor(
                      databaseId, canonicalSort, sourcePageSize, nextPageIndex(pageIndex), 0));
        } else {
          nextToken = null;
        }
        return ViewListResult.builder().results(results).nextPageToken(nextToken).build();
      }

      if (!page.hasNext()) {
        return ViewListResult.builder().results(results).nextPageToken(null).build();
      }

      advances++;
      if (advances >= MAX_SOURCE_PAGE_ADVANCES_PER_CALL) {
        String nextToken =
            codec.encode(
                new ViewPageCursor(
                    databaseId, canonicalSort, sourcePageSize, nextPageIndex(pageIndex), 0));
        return ViewListResult.builder().results(results).nextPageToken(nextToken).build();
      }
      pageIndex = nextPageIndex(pageIndex);
      offset = 0;
    }
  }

  private int nextPageIndex(int pageIndex) {
    if (pageIndex == Integer.MAX_VALUE) {
      throw badRequest("pageToken : source page index overflow");
    }
    return pageIndex + 1;
  }

  private String normalizeSort(String sortBy) {
    if (sortBy == null || sortBy.trim().isEmpty()) {
      return CANONICAL_SORT;
    }
    String trimmed = sortBy.trim();
    if (CANONICAL_SORT.equalsIgnoreCase(trimmed)) {
      return CANONICAL_SORT;
    }
    throw badRequest("sortBy : only viewId is supported");
  }

  private ViewPageCursor decodeAndValidate(
      String pageToken, String databaseId, String canonicalSort) {
    ViewPageCursor cursor;
    try {
      cursor = codec.decode(pageToken);
    } catch (RuntimeException e) {
      throw badRequest("pageToken : malformed");
    }
    if (!databaseId.equals(cursor.getDatabaseId())) {
      throw badRequest("pageToken : does not match databaseId");
    }
    if (!canonicalSort.equals(cursor.getSortBy())) {
      throw badRequest("pageToken : does not match sortBy");
    }
    if (cursor.getPageSize() != sourcePageSize) {
      throw badRequest("pageToken : does not match server page size");
    }
    if (cursor.getSourcePageIndex() < 0) {
      throw badRequest("pageToken : negative source page index");
    }
    if (cursor.getOffset() < 0 || cursor.getOffset() >= cursor.getPageSize()) {
      throw badRequest("pageToken : offset out of range");
    }
    return cursor;
  }

  private static ViewApiException badRequest(String message) {
    return new ViewApiException(ViewErrorCode.INVALID_VIEW_DEFINITION, message);
  }

  /** Thin seam over the real view listing, so the adapter can be unit tested without HTS. */
  public interface ViewPageSource {
    Page<ViewDto> list(String databaseId, Pageable pageable);
  }
}
