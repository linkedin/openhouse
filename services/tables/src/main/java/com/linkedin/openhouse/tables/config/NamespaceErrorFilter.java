package com.linkedin.openhouse.tables.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.tables.api.spec.namespace.NamespaceErrorResponse;
import com.linkedin.openhouse.tables.api.spec.namespace.NamespaceErrorResponse.NamespaceError;
import com.linkedin.openhouse.tables.exception.NamespaceExceptionHandler;
import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;

/** Renders namespace errors produced before MVC, including token-interceptor rejections. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@RequiredArgsConstructor
public class NamespaceErrorFilter extends OncePerRequestFilter {
  private final ObjectMapper objectMapper;

  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    return !request
        .getRequestURI()
        .substring(request.getContextPath().length())
        .matches("/v1/[^/]+/namespaces(?:/[^/]+(?:/properties)?)?/?");
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    ContentCachingResponseWrapper wrapped =
        new ContentCachingResponseWrapper(response) {
          @Override
          public void sendError(int status) {
            setStatus(status);
          }

          @Override
          public void sendError(int status, String message) {
            setStatus(status);
          }
        };
    chain.doFilter(request, wrapped);
    if (wrapped.getStatus() >= 400
        && !Boolean.TRUE.equals(
            request.getAttribute(NamespaceExceptionHandler.RENDERED_ERROR_ATTRIBUTE))) {
      wrapped.resetBuffer();
      wrapped.setContentType(MediaType.APPLICATION_JSON_VALUE);
      objectMapper.writeValue(
          wrapped.getOutputStream(),
          new NamespaceErrorResponse(
              new NamespaceError(
                  "Namespace request failed",
                  wrapped.getStatus() == 401
                      ? "NotAuthorizedException"
                      : wrapped.getStatus() == 403 ? "ForbiddenException" : "RESTException",
                  wrapped.getStatus())));
    }
    wrapped.copyBodyToResponse();
  }
}
