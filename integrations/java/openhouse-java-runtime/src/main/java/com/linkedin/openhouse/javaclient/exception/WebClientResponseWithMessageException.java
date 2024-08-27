package com.linkedin.openhouse.javaclient.exception;

import java.nio.charset.Charset;
import org.springframework.web.reactive.function.client.WebClientResponseException;

/**
 * {@link WebClientResponseException} doesnt display server response in its error message.
 *
 * <p>In this class we override the {@link #getMessage()} behavior, to append server response if it
 * exists. It doesn't change status code (ex: forbidden) or status int (ex. 403)
 *
 * <p>An exception thrown in Openhouse clients to indicate an error in a tables API request, acting
 * as a wrapper around WebClientResponseException.
 */
public class WebClientResponseWithMessageException extends WebClientWithMessageException {
  private final WebClientResponseException responseException;

  /**
   * We want the response exception to go through the legacy constructor in {@link
   * WebClientResponseException} because the HttpRequest value is explicitly set to null in order to
   * have the message rewritten to not expose unnecessary data.
   */
  private WebClientResponseException createWebClientResponseException(
      WebClientResponseException e) {
    return new WebClientResponseException(
        e.getRawStatusCode(),
        e.getStatusText(),
        e.getHeaders(),
        e.getResponseBodyAsByteArray(),
        Charset.defaultCharset());
  }

  public WebClientResponseWithMessageException(WebClientResponseException exception) {
    this.responseException = createWebClientResponseException(exception);
  }

  @Override
  public String getMessage() {
    return responseException.getResponseBodyAsString().isEmpty()
        ? responseException.getMessage()
        : String.format(
            "%s , %s", responseException.getMessage(), responseException.getResponseBodyAsString());
  }

  public int getStatusCode() {
    return responseException.getRawStatusCode();
  }
}
