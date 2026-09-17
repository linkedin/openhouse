package com.linkedin.openhouse.common.api.spec;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import org.springframework.http.HttpStatus;

/** A common response body for errors in controller. */
@Builder
@Value
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ErrorResponseBody {
  @Schema(description = "HTTP status code", example = "400")
  HttpStatus status;

  @Schema(description = "HTTP failure phrase", example = "Bad Request")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  String error;

  @Schema(
      description = "Useful message about the error",
      example = "databaseId: does not match with RequestBody;")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  String message;

  @Schema(
      description = "Stable machine-readable error code",
      example = "COLUMN_DEFAULT_INVALID_VALUE")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  String code;

  @Schema(description = "Server-generated identifier for support and log correlation")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  String requestId;

  @Schema(description = "Whether this failure is transient and the request may be retried later")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  Boolean retryable;

  @Schema(
      description = "Actual Error stacktrace containing the root cause of the error",
      example = "Stacktrace from downstream service that caused the error")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  String stacktrace;

  @Schema(
      description = "Actual root cause of the error",
      example = "Root cause of failure from downstream service")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  String cause;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
