package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.CodedApiException;
import org.springframework.http.HttpStatus;

/**
 * {@link ColumnDefaultClientGate} refused a request to a table opted into column defaults because
 * the client is not a compatible Spark release. Maps to 422.
 */
public class ColumnDefaultClientRejectedException extends CodedApiException {

  public ColumnDefaultClientRejectedException(String message) {
    super(message);
  }

  @Override
  public HttpStatus getHttpStatus() {
    return HttpStatus.UNPROCESSABLE_ENTITY;
  }
}
