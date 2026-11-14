package com.linkedin.openhouse.housetables.api.spec.response;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Value;
import org.springframework.http.HttpStatus;

/** Semantic acknowledgements for namespace requests, including transaction completion. */
@Value
public class NamespaceErrorResponse {
  @Schema(required = true, description = "HTTP status.")
  HttpStatus status;

  @Schema(required = true, description = "HTTP failure phrase.")
  String error;

  @Schema(required = true, description = "Safe description of the rejected operation.")
  String message;

  @Schema(required = true, description = "Stable namespace failure type.")
  String type;
}
