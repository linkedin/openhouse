package com.linkedin.openhouse.optimizer.api.error;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Uniform error response body returned by every optimizer endpoint on a non-2xx status.
 *
 * <p>Shape:
 *
 * <ul>
 *   <li>{@code code} — machine-readable identifier (e.g. {@code OPERATION_NOT_FOUND}).
 *   <li>{@code message} — human-readable explanation.
 *   <li>{@code path} — the request URI that triggered the error.
 * </ul>
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ApiError {
  private String code;
  private String message;
  private String path;
}
