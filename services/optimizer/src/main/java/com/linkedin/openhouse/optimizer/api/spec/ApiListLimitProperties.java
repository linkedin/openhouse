package com.linkedin.openhouse.optimizer.api.spec;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/**
 * Bounds on list-style endpoints. Default applies when the caller omits {@code limit}; max is the
 * hard cap — requests beyond it are rejected (400) rather than silently clamped, so callers don't
 * get partial results without knowing.
 */
@ConfigurationProperties("optimizer.api.list")
@Getter
@Setter
public class ApiListLimitProperties {
  private int defaultLimit = 10;
  private int maxLimit = 1000;

  /**
   * Resolve a caller-supplied {@code limit} against this configuration. Null falls back to {@link
   * #defaultLimit}; out-of-range values throw {@link ResponseStatusException} carrying HTTP 400.
   */
  public int validateAndResolve(Integer limit) {
    int effective = (limit == null) ? defaultLimit : limit;
    if (effective < 1 || effective > maxLimit) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST,
          "limit must be between 1 and " + maxLimit + " (got " + effective + ")");
    }
    return effective;
  }
}
