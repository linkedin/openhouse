package com.linkedin.openhouse.common.api.spec;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import org.springframework.http.HttpStatus;

/** A common response body for errors in controller. */
@Builder
@Value
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
