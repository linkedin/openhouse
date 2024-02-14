package com.linkedin.openhouse.common.api.spec;

import lombok.Builder;
import lombok.Value;
import org.springframework.http.HttpHeaders;

/**
 * POJO for ApiRequest containing requestBody JSON, and HTTP headers.
 *
 * @param <T> Class Type of the RequestBody
 */
@Builder
@Value
public class ApiRequest<T> {

  /** Request Body JSON as sent *by* the client as part of Request payload. */
  T requestBody;

  /** HTTP Headers as sent by the client as part of headers. */
  HttpHeaders httpHeaders;
}
