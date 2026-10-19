package com.linkedin.openhouse.tables.audit;

import javax.servlet.http.HttpServletRequest;
import org.springframework.http.server.PathContainer;
import org.springframework.http.server.RequestPath;
import org.springframework.web.util.ServletRequestPathUtils;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;

/**
 * Classifies a request as a view route using the same path-pattern matching Spring MVC applies when
 * it dispatches to {@code ViewsController}, so the audit redactors cannot decline a path (a
 * trailing separator, a percent-encoded literal segment, matrix parameters) that the controller
 * accepts. Matching is per decoded path segment, never against the raw URI string.
 *
 * <p>The path is the one MVC already parsed and cached on the request when present, which reuses
 * MVC's {@link RequestPath} application-path handling instead of reimplementing it. An uncached
 * request (a direct predicate call) is parsed without caching anything on it, from the raw URI and
 * context path. A malformed percent encoding surfaces as an {@link IllegalArgumentException}, which
 * the audit aspect already treats as a failure; it is not caught or defaulted here.
 *
 * <p>Classification only: no caller-visible path, query or payload is rewritten here.
 */
final class ViewRequestRouteMatcher {

  /** Matches the two view routes declared by {@code ViewsController}. */
  private static final PathPatternParser PARSER = new PathPatternParser();

  private static final PathPattern COLLECTION = PARSER.parse("/v1/databases/{databaseId}/views");

  private static final PathPattern ITEM = PARSER.parse("/v1/databases/{databaseId}/views/{viewId}");

  private ViewRequestRouteMatcher() {}

  /** Whether the request targets the view collection route. */
  static boolean isViewCollection(HttpServletRequest request) {
    PathContainer path = pathWithinApplication(request);
    return path != null && COLLECTION.matches(path);
  }

  /** Whether the request targets the view collection or a view item route. */
  static boolean isViewCollectionOrItem(HttpServletRequest request) {
    PathContainer path = pathWithinApplication(request);
    return path != null && (COLLECTION.matches(path) || ITEM.matches(path));
  }

  private static PathContainer pathWithinApplication(HttpServletRequest request) {
    if (ServletRequestPathUtils.hasParsedRequestPath(request)) {
      return ServletRequestPathUtils.getParsedRequestPath(request).pathWithinApplication();
    }
    String uri = request.getRequestURI();
    return uri == null
        ? null
        : RequestPath.parse(uri, request.getContextPath()).pathWithinApplication();
  }
}
