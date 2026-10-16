package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.CodedApiException;
import org.springframework.http.HttpStatus;

/**
 * {@link ColumnDefaultClientGate} refused a request to a table opted into column defaults: the
 * client is not an admitted, compatible release, or the server has no valid minimum configured.
 * Maps to 422.
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
