package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewListResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;

public class ViewPaginationAdapterTest {

  private static final String DATABASE_ID = "dba";

  @Test
  public void onlyViewIdSortIsAcceptedAndMappedToHtsTableId() {
    RecordingViewPageSource source =
        new RecordingViewPageSource(
            new PageImpl<>(Collections.emptyList(), PageRequest.of(0, 2), 0));
    ViewPaginationAdapter adapter = new ViewPaginationAdapter(source, 2);

    adapter.list(DATABASE_ID, null, 1, " VIEWID ");

    assertEquals("tableId", source.lastPageable.getSort().iterator().next().getProperty());
    assertBadRequest(() -> adapter.list(DATABASE_ID, null, 1, "lastModifiedTime"));
    assertBadRequest(() -> adapter.list(DATABASE_ID, null, 1, "doesNotExist"));
  }

  @Test
  public void changedClientSizesDoNotSkipOrRepeatRows() {
    RecordingViewPageSource source =
        new RecordingViewPageSource(
            page(0, 2, true, view("a"), view("b")), page(1, 2, false, view("c"), view("d")));
    ViewPaginationAdapter adapter = new ViewPaginationAdapter(source, 2);

    ViewListResult first = adapter.list(DATABASE_ID, null, 1, "viewId");
    ViewListResult second = adapter.list(DATABASE_ID, first.getNextPageToken(), 3, "viewId");

    assertEquals(Collections.singletonList(view("a")), first.getResults());
    assertEquals(Arrays.asList(view("b"), view("c"), view("d")), second.getResults());
    assertNull(
        second.getNextPageToken(),
        "The source is terminal after d, even though the second request changed size.");
    assertEquals(
        Arrays.asList("dba:0:2:tableId", "dba:0:2:tableId", "dba:1:2:tableId"),
        source.requests,
        "The adapter must serialize changed caller sizes as fixed-P HTS page requests.");
  }

  @Test
  public void emptyNonTerminalSourcePageAdvancesWithoutAssumingFullOffsets() {
    RecordingViewPageSource source =
        new RecordingViewPageSource(
            new PageImpl<>(Collections.emptyList(), PageRequest.of(0, 2), 3),
            page(1, 2, false, view("c")));
    ViewPaginationAdapter adapter = new ViewPaginationAdapter(source, 2);

    ViewListResult result = adapter.list(DATABASE_ID, null, 1, "viewId");

    assertEquals(Collections.singletonList(view("c")), result.getResults());
    assertNull(result.getNextPageToken());
  }

  @Test
  public void shortNonEmptyNonTerminalSourcePageContinuesFromNextSourcePage() {
    RecordingViewPageSource source =
        new RecordingViewPageSource(
            new PageImpl<>(Collections.singletonList(view("a")), PageRequest.of(0, 2), 4),
            page(1, 2, false, view("b"), view("c")));
    ViewPaginationAdapter adapter = new ViewPaginationAdapter(source, 2);

    ViewListResult result = adapter.list(DATABASE_ID, null, 3, "viewId");

    assertEquals(Arrays.asList(view("a"), view("b"), view("c")), result.getResults());
    assertNull(result.getNextPageToken());
  }

  @Test
  public void terminalEmptySourcePageReturnsEmptyTerminalResult() {
    RecordingViewPageSource source =
        new RecordingViewPageSource(
            new PageImpl<>(Collections.emptyList(), PageRequest.of(0, 2), 0));
    ViewPaginationAdapter adapter = new ViewPaginationAdapter(source, 2);

    ViewListResult result = adapter.list(DATABASE_ID, null, 3, "viewId");

    assertEquals(Collections.emptyList(), result.getResults());
    assertNull(result.getNextPageToken());
    assertEquals(1, source.calls);
  }

  @Test
  public void tokenDatabaseAndSortMustMatchRequest() {
    RecordingViewPageSource source =
        new RecordingViewPageSource(
            new PageImpl<>(Collections.emptyList(), PageRequest.of(0, 2), 0));
    ViewPaginationAdapter adapter = new ViewPaginationAdapter(source, 2);
    ViewPageTokenCodec codec = new ViewPageTokenCodec();
    String databaseMismatch = codec.encode(new ViewPageCursor("other_db", "viewId", 2, 0, 0));
    String sortMismatch = codec.encode(new ViewPageCursor(DATABASE_ID, "otherSort", 2, 0, 0));
    String pageSizeMismatch = codec.encode(new ViewPageCursor(DATABASE_ID, "viewId", 3, 0, 0));
    String negativePage = codec.encode(new ViewPageCursor(DATABASE_ID, "viewId", 2, -1, 0));
    String negativeOffset = codec.encode(new ViewPageCursor(DATABASE_ID, "viewId", 2, 0, -1));
    String outOfRangeOffset = codec.encode(new ViewPageCursor(DATABASE_ID, "viewId", 2, 0, 2));
    String valid = codec.encode(new ViewPageCursor(DATABASE_ID, "viewId", 2, 0, 0));
    String tampered = valid.substring(0, valid.length() - 1) + "x";

    assertBadRequest(() -> adapter.list(DATABASE_ID, databaseMismatch, 1, "viewId"));
    assertBadRequest(() -> adapter.list(DATABASE_ID, sortMismatch, 1, "viewId"));
    assertBadRequest(() -> adapter.list(DATABASE_ID, pageSizeMismatch, 1, "viewId"));
    assertBadRequest(() -> adapter.list(DATABASE_ID, negativePage, 1, "viewId"));
    assertBadRequest(() -> adapter.list(DATABASE_ID, negativeOffset, 1, "viewId"));
    assertBadRequest(() -> adapter.list(DATABASE_ID, outOfRangeOffset, 1, "viewId"));
    assertBadRequest(() -> adapter.list(DATABASE_ID, tampered, 1, "viewId"));
    assertBadRequest(() -> adapter.list(DATABASE_ID, "not-a-token", 1, "viewId"));

    assertEquals(0, source.calls);
  }

  @Test
  public void validRepeatedEmptyNonTerminalPagesReturnForwardContinuationWithoutHanging() {
    RecordingViewPageSource source = RecordingViewPageSource.alwaysEmptyNonTerminal();
    ViewPaginationAdapter adapter = new ViewPaginationAdapter(source, 2);

    ViewListResult result =
        assertTimeoutPreemptively(
            Duration.ofSeconds(1), () -> adapter.list(DATABASE_ID, null, 1, "viewId"));

    assertEquals(Collections.emptyList(), result.getResults());
    org.junit.jupiter.api.Assertions.assertNotNull(result.getNextPageToken());
    org.junit.jupiter.api.Assertions.assertTrue(source.calls > 0);
    org.junit.jupiter.api.Assertions.assertTrue(
        source.lastPageable.getPageNumber() > 0,
        "A valid empty nonterminal sequence must advance the source cursor before returning.");
    ViewPageCursor cursor = new ViewPageTokenCodec().decode(result.getNextPageToken());
    org.junit.jupiter.api.Assertions.assertTrue(
        cursor.getSourcePageIndex() > 0,
        "Returned continuation must point at an advanced source page, not back to page 0.");
    int callsAfterFirst = source.calls;
    assertTimeoutPreemptively(
        Duration.ofSeconds(1),
        () -> adapter.list(DATABASE_ID, result.getNextPageToken(), 1, "viewId"));
    org.junit.jupiter.api.Assertions.assertTrue(source.calls > callsAfterFirst);
  }

  @Test
  public void sourcePageOverflowWhileAdvancingFailsSafely() {
    RecordingViewPageSource source =
        new RecordingViewPageSource(
            new PageImpl<>(
                Collections.singletonList(view("tail")),
                PageRequest.of(Integer.MAX_VALUE, 2),
                Long.MAX_VALUE));
    ViewPaginationAdapter adapter = new ViewPaginationAdapter(source, 2);
    String nearOverflow =
        new ViewPageTokenCodec()
            .encode(new ViewPageCursor(DATABASE_ID, "viewId", 2, Integer.MAX_VALUE, 1));

    assertBadRequest(() -> adapter.list(DATABASE_ID, nearOverflow, 2, "viewId"));
    assertEquals(
        1,
        source.calls,
        "The adapter may inspect the current page but must fail before overflowing to the next.");
  }

  @Test
  public void unsupportedSortFailsBeforeFetchingBackendPages() {
    RecordingViewPageSource source =
        new RecordingViewPageSource(
            new PageImpl<>(Collections.emptyList(), PageRequest.of(0, 2), 0));
    ViewPaginationAdapter adapter = new ViewPaginationAdapter(source, 2);

    assertBadRequest(() -> adapter.list(DATABASE_ID, null, 1, "creationTime"));

    assertEquals(0, source.calls, "Unsupported sorts must be rejected before reaching HTS.");
  }

  private static ViewDto view(String id) {
    return ViewDto.builder().databaseId(DATABASE_ID).viewId(id).build();
  }

  private static Page<ViewDto> page(int index, int size, boolean hasNext, ViewDto... views) {
    int total = hasNext ? (index + 2) * size : index * size + views.length;
    return new PageImpl<>(Arrays.asList(views), PageRequest.of(index, size), total);
  }

  private static void assertBadRequest(Runnable action) {
    ViewApiException thrown = assertThrows(ViewApiException.class, action::run);
    assertEquals(HttpStatus.BAD_REQUEST, thrown.getHttpStatus());
  }

  private static class RecordingViewPageSource implements ViewPaginationAdapter.ViewPageSource {
    private final Map<Integer, Page<ViewDto>> pagesByIndex = new HashMap<>();
    private final boolean synthesizeEmptyNonTerminal;
    private final boolean alwaysEmptyNonTerminal;
    private final List<String> requests = new ArrayList<>();
    private int calls;
    private org.springframework.data.domain.Pageable lastPageable;

    @SafeVarargs
    private RecordingViewPageSource(Page<ViewDto>... pages) {
      this(false, false, pages);
    }

    @SafeVarargs
    private RecordingViewPageSource(boolean synthesizeEmptyNonTerminal, Page<ViewDto>... pages) {
      this(synthesizeEmptyNonTerminal, false, pages);
    }

    @SafeVarargs
    private RecordingViewPageSource(
        boolean synthesizeEmptyNonTerminal,
        boolean alwaysEmptyNonTerminal,
        Page<ViewDto>... pages) {
      this.synthesizeEmptyNonTerminal = synthesizeEmptyNonTerminal;
      this.alwaysEmptyNonTerminal = alwaysEmptyNonTerminal;
      for (Page<ViewDto> page : pages) {
        pagesByIndex.put(page.getPageable().getPageNumber(), page);
      }
    }

    private static RecordingViewPageSource emptyNonTerminalPages(int count) {
      Page<ViewDto>[] pages = new Page[count];
      for (int i = 0; i < count; i++) {
        pages[i] = new PageImpl<>(Collections.emptyList(), PageRequest.of(i, 2), count * 2L);
      }
      return new RecordingViewPageSource(true, pages);
    }

    private static RecordingViewPageSource alwaysEmptyNonTerminal() {
      return new RecordingViewPageSource(false, true);
    }

    @Override
    public Page<ViewDto> list(
        String databaseId, org.springframework.data.domain.Pageable pageable) {
      calls++;
      this.lastPageable = pageable;
      requests.add(
          databaseId
              + ":"
              + pageable.getPageNumber()
              + ":"
              + pageable.getPageSize()
              + ":"
              + pageable.getSort().iterator().next().getProperty());
      if (alwaysEmptyNonTerminal) {
        if (calls > 100) {
          throw new AssertionError("Adapter did not bound repeated empty-page progress");
        }
        return new PageImpl<>(
            Collections.emptyList(), PageRequest.of(pageable.getPageNumber(), 2), Long.MAX_VALUE);
      }
      Page<ViewDto> page = pagesByIndex.get(pageable.getPageNumber());
      if (page == null) {
        throw new AssertionError("Unexpected backend page request: " + pageable);
      }
      assertEquals(2, pageable.getPageSize(), "The bridge must use a fixed source page size.");
      assertEquals("tableId", pageable.getSort().iterator().next().getProperty());
      return page;
    }
  }
}
