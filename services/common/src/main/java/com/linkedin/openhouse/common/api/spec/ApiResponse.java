package com.linkedin.openhouse.common.api.spec;

import lombok.Builder;
import lombok.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;

/**
 * POJO for ApiResponse containing HTTP status, responseBody JSON, and HTTP headers. This POJO is
 * sufficient for the REST layer to forward the response back to the client without having to
 * transform further.
 *
 * @param <T> Class Type of the ResponseBody
 */
@Builder
@Value
public class ApiResponse<T> {
  /** HTTP Status to returned to the Client. */
  HttpStatus httpStatus;

  /** Response Body JSON as sent *to* the client as part of Response payload */
  T responseBody;

  /** HTTP Headers as sent *to* the client as part of headers. */
  HttpHeaders httpHeaders;
}
