package com.linkedin.openhouse.tables.mock.audit;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletMapping;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.MappingMatch;
import org.junit.jupiter.params.provider.Arguments;
import org.springframework.mock.web.MockHttpServletMapping;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.util.ServletRequestPathUtils;

/**
 * Raw request paths and whether Spring MVC routes each to the view collection or the view item
 * controller templates ({@code /v1/databases/{databaseId}/views[/{viewId}]}) under the service's
 * default path matching: single per-segment decoding, matrix parameters ignored, case-sensitive
 * literals, and an optional trailing separator. Shared by both view audit redactor tests so the URI
 * (collection only) and payload (collection or item) scopes are classified from one table.
 */
final class ViewAuditRouteCases {

  private ViewAuditRouteCases() {}

  /** Arguments: context path, raw request URI, is the view collection, is a view route. */
  static Stream<Arguments> routes() {
    return Stream.of(
        // View collection.
        route("", "/v1/databases/db/views", true, true),
        route("", "/v1/databases/db/views/", true, true),
        route("", "/v1/databases/db/%76iews", true, true),
        route("", "/v1/databases/db/%76iews/", true, true),
        route("", "/v1/databases/db/views;mode=x", true, true),
        route("", "/v1/d%61tabases/db/views", true, true),
        route("/ctx", "/ctx/v1/databases/db/views", true, true),
        route("/ctx", "/ctx/v1/databases/db/%76iews/", true, true),
        // View item.
        route("", "/v1/databases/db/views/v", false, true),
        route("", "/v1/databases/db/views/v/", false, true),
        route("", "/v1/databases/db/%76iews/v", false, true),
        route("", "/v1/databases/db/%76iews/v/", false, true),
        route("", "/v1/databases/db/views/v;mode=x", false, true),
        route("/ctx", "/ctx/v1/databases/db/%76iews/v", false, true),
        // Not a view route.
        route("", "/v1/databases/db/tables", false, false),
        route("", "/v1/databases/db/tables/t", false, false),
        route("", "/v1/databases/db/%74ables", false, false),
        route("", "/v1/databases/db/%74ables/t", false, false),
        route("", "/v2/databases/db/views", false, false),
        route("", "/other/v1/databases/db/views", false, false),
        route("", "/v1/databases/db/VIEWS", false, false),
        route("", "/v1/databases/db/viewsExtra", false, false),
        route("", "/v1/databases/views", false, false),
        route("", "/v1/databases/db/views/v/extra", false, false),
        route("", "/v1/databases/db/%2576iews", false, false),
        route("", "/v1/databases/db%2Fviews", false, false),
        route("", "/v1/databases/db/views%2Fv", false, false),
        route("", "/v1/databases/db/views//", false, false),
        route("/ctx", "/ctx/v1/databases/db/tables", false, false));
  }

  private static Arguments route(
      String contextPath, String rawRequestUri, boolean collection, boolean viewRoute) {
    return Arguments.of(contextPath, rawRequestUri, collection, viewRoute);
  }

  /** A standalone request, or one whose path MVC's DispatcherServlet has parsed and cached. */
  static MockHttpServletRequest request(String contextPath, String rawRequestUri, boolean cached) {
    MockHttpServletRequest request = new MockHttpServletRequest("GET", rawRequestUri);
    request.setContextPath(contextPath);
    request.setRequestURI(rawRequestUri);
    if (cached) {
      ServletRequestPathUtils.parseAndCache(request);
    }
    return request;
  }

  /**
   * A DispatcherServlet mapped under {@code /api/*}: MVC's cached path is within that servlet
   * mapping, so {@code /api/v1/databases/db/views} is routed as the view collection.
   */
  static HttpServletRequest servletPrefixedCollectionRequest() {
    MockHttpServletRequest base = new MockHttpServletRequest("GET", "/api/v1/databases/db/views");
    base.setServletPath("/api");
    base.setPathInfo("/v1/databases/db/views");
    HttpServletRequest request =
        new HttpServletRequestWrapper(base) {
          @Override
          public HttpServletMapping getHttpServletMapping() {
            return new MockHttpServletMapping(
                "/v1/databases/db/views", "/api/*", "dispatcher", MappingMatch.PATH);
          }
        };
    ServletRequestPathUtils.parseAndCache(request);
    return request;
  }

  static Set<String> attributeNames(HttpServletRequest request) {
    return new HashSet<>(Collections.list(request.getAttributeNames()));
  }
}
