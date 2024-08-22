package com.linkedin.openhouse.javaclient.exception;

import org.springframework.web.reactive.function.client.WebClientRequestException;

/**
 * An exception thrown in Openhouse clients to indicate an error in a tables API request, acting as
 * a wrapper around WebClientRequestException.
 */
public class WebClientRequestWithMessageException extends WebClientWithMessageException {
  private final WebClientRequestException requestException;

  public WebClientRequestWithMessageException(WebClientRequestException requestException) {
    this.requestException = requestException;
  }

  @Override
  public String getMessage() {
    return requestException.getMessage();
  }
}
