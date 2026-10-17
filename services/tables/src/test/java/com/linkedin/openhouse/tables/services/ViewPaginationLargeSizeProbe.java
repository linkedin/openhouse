package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewListResult;
import java.util.Collections;
import org.springframework.data.domain.PageImpl;

/**
 * Child-JVM entry point for {@link ViewPaginationAdapterLargeSizeTest}. It runs under a small fixed
 * heap so any allocation proportional to the requested size, rather than to returned rows, fails
 * fast inside the child instead of consuming host memory.
 */
public final class ViewPaginationLargeSizeProbe {

  static final int OK = 0;
  static final int OUT_OF_MEMORY = 3;

  private ViewPaginationLargeSizeProbe() {}

  public static void main(String[] args) {
    ViewPaginationAdapter adapter =
        new ViewPaginationAdapter(
            (databaseId, pageable) -> new PageImpl<ViewDto>(Collections.emptyList(), pageable, 0),
            ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE);
    for (String arg : args) {
      int size = Integer.parseInt(arg);
      try {
        ViewListResult result = adapter.list("db", null, size, "viewId");
        System.out.println(
            "RESULT size="
                + size
                + " rows="
                + result.getResults().size()
                + " next="
                + result.getNextPageToken());
      } catch (OutOfMemoryError e) {
        System.out.println("OOM size=" + size + " " + e.getMessage());
        System.exit(OUT_OF_MEMORY);
      }
    }
    System.exit(OK);
  }
}
