package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.CodedApiException;
import org.springframework.http.HttpStatus;

/**
 * {@link ClientCapabilityGate} refused a request to a table because the client does not declare a
 * capability the table requires. Maps to 422.
 */
public class ClientCapabilityMissingException extends CodedApiException {

  public ClientCapabilityMissingException(String message) {
    super(message);
  }

  @Override
  public HttpStatus getHttpStatus() {
    return HttpStatus.UNPROCESSABLE_ENTITY;
  }
}
