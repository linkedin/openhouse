package com.linkedin.openhouse.tables.mock.service;

import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.services.ViewsDisabledService;
import com.linkedin.openhouse.tables.services.ViewsService;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.HttpStatus;

/**
 * Freezes the only behaviour the stub service has: every operation reports that views are disabled.
 *
 * <p>Plain instantiation rather than a Spring context: the bean has no collaborators, so a context
 * would only slow the test down without exercising anything extra.
 */
public class ViewsDisabledServiceTest {

  /**
   * Deliberately duplicated rather than referenced from the production constant, which is
   * package-private. Restating the literal is the point: this is the frozen, redacted message that
   * reaches the error body and the service audit event, so a change to it must break a test.
   */
  private static final String EXPECTED_MESSAGE = "Views are disabled";

  private static final String ACTING_PRINCIPAL = "DUMMY_ANONYMOUS_USER";

  /** One entry per {@link ViewsService} method, so a new method cannot silently skip the gate. */
  private static Stream<Arguments> allServiceOperations() {
    return Stream.of(
        Arguments.of(
            "getView",
            (ServiceOperation)
                service ->
                    service.getView(
                        ViewModelConstants.DATABASE_ID,
                        ViewModelConstants.VIEW_ID,
                        ACTING_PRINCIPAL)),
        Arguments.of(
            "getAllViews",
            (ServiceOperation)
                service ->
                    service.getAllViews(
                        ViewModelConstants.DATABASE_ID, 0, 50, null, ACTING_PRINCIPAL)),
        Arguments.of(
            "putView",
            (ServiceOperation)
                service ->
                    service.putView(
                        ViewModelConstants.createRequestWithoutBaseVersion(),
                        ACTING_PRINCIPAL,
                        true)),
        Arguments.of(
            "deleteView",
            (ServiceOperation)
                service ->
                    service.deleteView(
                        ViewModelConstants.DATABASE_ID,
                        ViewModelConstants.VIEW_ID,
                        ACTING_PRINCIPAL)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("allServiceOperations")
  public void everyOperationReportsViewsDisabled(String operationName, ServiceOperation operation) {
    ViewsDisabledService service = new ViewsDisabledService();

    ViewApiException exception =
        Assertions.assertThrows(ViewApiException.class, () -> operation.run(service));

    Assertions.assertEquals(
        ViewErrorCode.VIEWS_DISABLED,
        exception.getErrorCode(),
        "The stub must report the designed disabled code, not a generic failure: an uncoded"
            + " unchecked exception would surface as a 500 with a stack trace instead.");
    Assertions.assertEquals(HttpStatus.NOT_FOUND, exception.getHttpStatus());
    Assertions.assertEquals(EXPECTED_MESSAGE, exception.getMessage());
  }

  /** Invokes one {@link ViewsService} method; needed because the four have different shapes. */
  @FunctionalInterface
  interface ServiceOperation {
    void run(ViewsService service);
  }
}
