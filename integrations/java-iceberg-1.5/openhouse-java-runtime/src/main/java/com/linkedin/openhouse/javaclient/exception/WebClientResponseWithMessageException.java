package com.linkedin.openhouse.javaclient.exception;

import java.nio.charset.Charset;
import org.springframework.web.reactive.function.client.WebClientResponseException;

/**
 * {@link WebClientResponseException} doesnt display server response in its error message.
 *
 * <p>In this class we override the {@link #getMessage()} behavior, to append server response if it
 * exists. It doesn't change status code (ex: forbidden) or status int (ex. 403)
 */
public class WebClientResponseWithMessageException extends WebClientResponseException {
  public WebClientResponseWithMessageException(WebClientResponseException e) {
    super(
        e.getRawStatusCode(),
        e.getStatusText(),
        e.getHeaders(),
        e.getResponseBodyAsByteArray(),
        Charset.defaultCharset());
  }

  @Override
  public String getMessage() {
    return getResponseBodyAsString().isEmpty()
        ? super.getMessage()
        : String.format("%s , %s", super.getMessage(), getResponseBodyAsString());
  }
}
