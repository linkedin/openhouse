package com.linkedin.openhouse.common.audit;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;

/**
 * Filter to cache request body in ContentCachingRequestWrapper. Reason to do this is because the
 * request body is a stream that can be only consumed once, and it's already consumed by the
 * handler, but we need this information for auditing purpose.
 */
@Component
public class CachingRequestBodyFilter extends OncePerRequestFilter {

  public static final String ATTRIBUTE_START_TIME = "startTime";

  private static Set<String> exclusionPattern = new HashSet<>(Arrays.asList("/actuator/"));

  /** Wrap the request with ContentCachingRequestWrapper so that request body can be read later. */
  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
      throws ServletException, IOException {
    request.setAttribute(ATTRIBUTE_START_TIME, Instant.now());
    ContentCachingRequestWrapper wrappedRequest = new ContentCachingRequestWrapper(request);
    filterChain.doFilter(wrappedRequest, response);
  }

  @Override
  public boolean shouldNotFilter(HttpServletRequest request) {
    return exclusionPattern.stream()
        .anyMatch(pattern -> request.getRequestURI().startsWith(pattern));
  }
}
