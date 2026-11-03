package com.linkedin.openhouse.javaclient;

import static org.mockito.Mockito.*;

import com.linkedin.openhouse.gen.tables.client.api.ViewApi;
import com.linkedin.openhouse.gen.tables.client.model.GetAllViewsResponseBody;
import com.linkedin.openhouse.gen.tables.client.model.GetViewResponseBody;
import com.linkedin.openhouse.javaclient.exception.WebClientRequestWithMessageException;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientRequestException;
import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientResponseException;
import com.linkedin.openhouse.relocated.reactor.core.publisher.Mono;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for {@link OpenHouseCatalog}'s {@link ViewApi}-backed {@code listViews} / {@code
 * dropView}: the views-disabled gates, namespace validation, the {@code listViews} pagination walk,
 * {@code dropView}'s drop-if-exists (404) handling, and the error wrapping that keeps a service
 * failure from surfacing as a bare, message-less stack trace. {@link ViewApi} is mocked and
 * injected so the catalog logic runs without a service.
 */
public class OpenHouseCatalogViewsTest {

  private static final Namespace DB = Namespace.of("db");

  private OpenHouseCatalog catalog(ViewApi viewApi, boolean viewsEnabled) {
    OpenHouseCatalog catalog = new OpenHouseCatalog();
    setField(catalog, "viewApi", viewApi);
    setField(catalog, "viewsEnabled", viewsEnabled);
    return catalog;
  }

  private static void setField(Object target, String name, Object value) {
    try {
      Field field = OpenHouseCatalog.class.getDeclaredField(name);
      field.setAccessible(true);
      field.set(target, value);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to set " + name, e);
    }
  }

  private static GetViewResponseBody view(String viewId) {
    GetViewResponseBody view = mock(GetViewResponseBody.class);
    when(view.getViewId()).thenReturn(viewId);
    return view;
  }

  private static GetAllViewsResponseBody page(List<GetViewResponseBody> results, String nextToken) {
    GetAllViewsResponseBody page = mock(GetAllViewsResponseBody.class);
    when(page.getResults()).thenReturn(results);
    when(page.getNextPageToken()).thenReturn(nextToken);
    return page;
  }

  // ---- listViews ----

  @Test
  public void testListViewsReturnsEmptyWhenViewsDisabled() {
    ViewApi viewApi = mock(ViewApi.class);
    Assertions.assertTrue(catalog(viewApi, false).listViews(DB).isEmpty());
    verifyNoInteractions(viewApi);
  }

  static Stream<Arguments> invalidListNamespaces() {
    return Stream.of(
        Arguments.of("empty namespace", Namespace.empty()),
        Arguments.of("multi-level namespace", Namespace.of("db", "schema")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidListNamespaces")
  public void testListViewsRejectsInvalidNamespace(String description, Namespace namespace) {
    OpenHouseCatalog catalog = catalog(mock(ViewApi.class), true);
    Assertions.assertThrows(ValidationException.class, () -> catalog.listViews(namespace));
  }

  @Test
  public void testListViewsPaginatesAndAggregatesAcrossPages() {
    // Build pages before stubbing viewApi: page()/view() stub their own mocks, and Mockito forbids
    // a nested when() inside an unfinished when(viewApi...).
    GetAllViewsResponseBody firstPage = page(Arrays.asList(view("v1"), view("v2")), "token2");
    GetAllViewsResponseBody secondPage = page(List.of(view("v3")), null);
    ViewApi viewApi = mock(ViewApi.class);
    when(viewApi.getAllViewsV1("db", null, null, null)).thenReturn(Mono.just(firstPage));
    when(viewApi.getAllViewsV1("db", "token2", null, null)).thenReturn(Mono.just(secondPage));

    List<TableIdentifier> views = catalog(viewApi, true).listViews(DB);

    Assertions.assertEquals(
        Arrays.asList(
            TableIdentifier.of(DB, "v1"),
            TableIdentifier.of(DB, "v2"),
            TableIdentifier.of(DB, "v3")),
        views);
    // null size -> server default page size; loop follows nextPageToken until it is empty.
    verify(viewApi).getAllViewsV1("db", null, null, null);
    verify(viewApi).getAllViewsV1("db", "token2", null, null);
  }

  static Stream<Arguments> listServiceErrors() {
    return Stream.of(
        Arguments.of(
            "response error -> response message",
            Mono.<GetAllViewsResponseBody>error(
                mock(WebClientResponseException.InternalServerError.class)),
            WebClientResponseWithMessageException.class),
        Arguments.of(
            "request error -> request message",
            Mono.<GetAllViewsResponseBody>error(mock(WebClientRequestException.class)),
            WebClientRequestWithMessageException.class));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("listServiceErrors")
  public void testListViewsSurfacesServiceErrors(
      String description,
      Mono<GetAllViewsResponseBody> response,
      Class<? extends Throwable> expected) {
    ViewApi viewApi = mock(ViewApi.class);
    when(viewApi.getAllViewsV1("db", null, null, null)).thenReturn(response);
    OpenHouseCatalog catalog = catalog(viewApi, true);
    Assertions.assertThrows(expected, () -> catalog.listViews(DB));
  }

  // ---- dropView ----

  @Test
  public void testDropViewReturnsFalseWhenViewsDisabled() {
    ViewApi viewApi = mock(ViewApi.class);
    Assertions.assertFalse(catalog(viewApi, false).dropView(TableIdentifier.of(DB, "v")));
    verifyNoInteractions(viewApi);
  }

  @Test
  public void testDropViewRejectsMultiLevelNamespace() {
    OpenHouseCatalog catalog = catalog(mock(ViewApi.class), true);
    Assertions.assertThrows(
        ValidationException.class,
        () -> catalog.dropView(TableIdentifier.of(Namespace.of("db", "schema"), "v")));
  }

  @Test
  public void testDropViewReturnsTrueOnSuccess() {
    ViewApi viewApi = mock(ViewApi.class);
    when(viewApi.deleteViewV1("db", "v")).thenReturn(Mono.empty());
    Assertions.assertTrue(catalog(viewApi, true).dropView(TableIdentifier.of(DB, "v")));
    verify(viewApi).deleteViewV1("db", "v");
  }

  @Test
  public void testDropViewReturnsFalseWhenAbsent() {
    ViewApi viewApi = mock(ViewApi.class);
    // 404 -> drop-if-exists no-op: view already gone, so drop returns false, not an error.
    when(viewApi.deleteViewV1("db", "v"))
        .thenReturn(Mono.error(mock(WebClientResponseException.NotFound.class)));
    Assertions.assertFalse(catalog(viewApi, true).dropView(TableIdentifier.of(DB, "v")));
  }
}
