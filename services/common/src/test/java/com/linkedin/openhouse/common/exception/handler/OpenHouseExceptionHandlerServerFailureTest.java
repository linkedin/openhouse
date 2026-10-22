package com.linkedin.openhouse.common.exception.handler;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.common.exception.DependencyUnavailableException;
import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * Failures the request did not cause are server errors, never client errors, and their responses do
 * not expose server internals.
 */
public class OpenHouseExceptionHandlerServerFailureTest {

  private final OpenHouseExceptionHandler handler = new OpenHouseExceptionHandler();

  @Test
  public void tableConfigUnavailableIsServerErrorWithoutInternals() {
    TableConfigUnavailableException failure =
        new TableConfigUnavailableException(
            "config for db.tbl is unusable", new IllegalStateException("bad stored default"));

    ResponseEntity<ErrorResponseBody> response =
        handler.handleTableConfigUnavailableException(failure);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(response.getBody().getMessage()).isEqualTo("config for db.tbl is unusable");
    assertThat(response.getBody().getCause()).isNull();
    assertThat(response.getBody().getStacktrace()).isNull();
  }

  @Test
  public void dependencyUnavailableIsServiceUnavailableWithoutInternals() {
    DependencyUnavailableException failure =
        new DependencyUnavailableException(
            "HouseTables could not answer", new IllegalStateException("connection refused"));

    ResponseEntity<ErrorResponseBody> response =
        handler.handleDependencyUnavailableException(failure);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    assertThat(response.getBody().getMessage()).isEqualTo("HouseTables could not answer");
    assertThat(response.getBody().getCause()).isNull();
    assertThat(response.getBody().getStacktrace()).isNull();
  }
}
