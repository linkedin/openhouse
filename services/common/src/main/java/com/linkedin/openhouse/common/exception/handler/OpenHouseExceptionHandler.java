package com.linkedin.openhouse.common.exception.handler;

import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.InvalidSchemaEvolutionException;
import com.linkedin.openhouse.common.exception.JobEngineException;
import com.linkedin.openhouse.common.exception.JobStateConflictException;
import com.linkedin.openhouse.common.exception.NoSuchEntityException;
import com.linkedin.openhouse.common.exception.NoSuchJobException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.common.exception.OpenHouseCommitStateUnknownException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.ResourceGatedByToggledOnFeatureException;
import com.linkedin.openhouse.common.exception.UnprocessableEntityException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import io.swagger.v3.oas.annotations.Hidden;
import java.util.Arrays;
import java.util.NoSuchElementException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.NoHandlerFoundException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Centralize the error handling for controllers. TODO: Making exception class un-bind to the entity
 * type. TODO: Integrate with validation error to capture bad requests exception. TODO: Test 409
 * conflicts one repository is ready to throw that error.
 */
@Slf4j
@ControllerAdvice
public class OpenHouseExceptionHandler extends ResponseEntityExceptionHandler {
  private static final String CONFLICT_MSG_TMPL =
      "Entity with key[%s] is modified by another process already, nested exception message: %s";
  private static final String COMMIT_STATE_UNKNOWN_TMPL =
      "Table with key[%s] isn't ack'ed by server about the commit status, nested exception: %s";

  private static final String CAUSE_NOT_AVAILABLE = "Not Available";

  private static final int STACKTRACE_MAX_WIDTH = 6000;

  /**
   * Each method needs to be annotated with {@link Hidden} or every methods in advisee controllers
   * will display these status even though they are not annotated by corresponding {@link
   * io.swagger.v3.oas.annotations.responses.ApiResponse}
   */
  @Hidden
  @ExceptionHandler({
    NoSuchUserTableException.class,
    NoSuchJobException.class,
    NoSuchEntityException.class
  })
  protected ResponseEntity<ErrorResponseBody> handleEntityNotFound(
      NoSuchElementException noSuchElementException) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.NOT_FOUND)
            .error(HttpStatus.NOT_FOUND.getReasonPhrase())
            .message(noSuchElementException.getMessage())
            .stacktrace(getAbbreviatedStackTrace(noSuchElementException))
            .cause(getExceptionCause(noSuchElementException))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  /**
   * Customize the behavior of handling {@link ResourceGatedByToggledOnFeatureException}, so that it
   * doesn't result in the default {@link OpenHouseCommitStateUnknownException} which leads to 5xx
   * error. This exception tells clients that the resource they are requesting to manipulate are
   * protected by feature-toggle for the time being, thus the request manipulation cannot be
   * fulfilled by the server.
   */
  @Hidden
  @ExceptionHandler({ResourceGatedByToggledOnFeatureException.class})
  protected ResponseEntity<ErrorResponseBody> handleToggleException(
      ResourceGatedByToggledOnFeatureException resourceGatedByToggledOnFeatureException) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.METHOD_NOT_ALLOWED)
            .error(HttpStatus.METHOD_NOT_ALLOWED.getReasonPhrase())
            .message(resourceGatedByToggledOnFeatureException.getMessage())
            .stacktrace(getAbbreviatedStackTrace(resourceGatedByToggledOnFeatureException))
            .cause(getExceptionCause(resourceGatedByToggledOnFeatureException))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  /**
   * To customize behavior of handling {@link NoHandlerFoundException} one cannot rely on {@link
   * ExceptionHandler} as that causes ambiguity with {@link ResponseEntityExceptionHandler} and
   * failed bean initialization. Instead overriding the default implementation of {@link
   * ResponseEntityExceptionHandler#handleNoHandlerFoundException} can achieve the customization.
   */
  @Override
  protected ResponseEntity<Object> handleNoHandlerFoundException(
      NoHandlerFoundException ex, HttpHeaders headers, HttpStatus status, WebRequest request) {

    String errorMsg =
        String.format(
            "The combination of the method [%s] and Path [%s] cannot be resolved by server. "
                + "Please check and making sure you are using the right version of API.",
            ((ServletWebRequest) request).getHttpMethod(), request.getDescription(false));
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.BAD_REQUEST)
            .error(HttpStatus.BAD_REQUEST.getReasonPhrase())
            .message(errorMsg)
            .stacktrace(getAbbreviatedStackTrace(ex))
            .cause(getExceptionCause(ex))
            .build();
    return new ResponseEntity<>(errorResponseBody, errorResponseBody.getStatus());
  }

  @Hidden
  @ExceptionHandler(EntityConcurrentModificationException.class)
  protected ResponseEntity<ErrorResponseBody> handleEntityConflict(
      EntityConcurrentModificationException cme) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.CONFLICT)
            .error(HttpStatus.CONFLICT.getReasonPhrase())
            .message(
                String.format(CONFLICT_MSG_TMPL, cme.getEntityId(), cme.getCause().getMessage()))
            .stacktrace(getAbbreviatedStackTrace(cme))
            .cause(getExceptionCause(cme))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  @Hidden
  @ExceptionHandler(OpenHouseCommitStateUnknownException.class)
  protected ResponseEntity<ErrorResponseBody> handleTableCommitStateUnknown(
      OpenHouseCommitStateUnknownException e) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.SERVICE_UNAVAILABLE)
            .error(HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase())
            .message(
                String.format(COMMIT_STATE_UNKNOWN_TMPL, e.getTableId(), e.getCause().getMessage()))
            .stacktrace(getAbbreviatedStackTrace(e))
            .cause(getExceptionCause(e))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  @Hidden
  @ExceptionHandler(JobStateConflictException.class)
  protected ResponseEntity<ErrorResponseBody> handleJobStateConflict(
      JobStateConflictException cme) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.CONFLICT)
            .error(HttpStatus.CONFLICT.getReasonPhrase())
            .message(cme.getMessage())
            .stacktrace(getAbbreviatedStackTrace(cme))
            .cause(getExceptionCause(cme))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  @Hidden
  @ExceptionHandler(RequestValidationFailureException.class)
  protected ResponseEntity<ErrorResponseBody> handleBadRequest(
      RequestValidationFailureException validationException) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.BAD_REQUEST)
            .error(HttpStatus.BAD_REQUEST.getReasonPhrase())
            .message(validationException.getMessage())
            .stacktrace(getAbbreviatedStackTrace(validationException))
            .cause(getExceptionCause(validationException))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  @Hidden
  @ExceptionHandler(UnprocessableEntityException.class)
  protected ResponseEntity<ErrorResponseBody> handleUnprocessableEntity(
      UnprocessableEntityException unprocessableEntityException) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.UNPROCESSABLE_ENTITY)
            .error(HttpStatus.UNPROCESSABLE_ENTITY.getReasonPhrase())
            .message(unprocessableEntityException.getMessage())
            .stacktrace(getAbbreviatedStackTrace(unprocessableEntityException))
            .cause(getExceptionCause(unprocessableEntityException))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  @Hidden
  @ExceptionHandler(AlreadyExistsException.class)
  protected ResponseEntity<ErrorResponseBody> handleEntityExists(
      AlreadyExistsException alreadyExistsException) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.CONFLICT)
            .error(HttpStatus.CONFLICT.getReasonPhrase())
            .message(alreadyExistsException.getMessage())
            .stacktrace(getAbbreviatedStackTrace(alreadyExistsException))
            .cause(getExceptionCause(alreadyExistsException))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  /**
   * Note the difference between {@link
   * OpenHouseExceptionHandler#handleEntityExists(com.linkedin.openhouse.common.exception.AlreadyExistsException)
   * }
   */
  @Hidden
  @ExceptionHandler(org.apache.iceberg.exceptions.AlreadyExistsException.class)
  protected ResponseEntity<ErrorResponseBody> handleEntityExistsConcurrentException(
      org.apache.iceberg.exceptions.AlreadyExistsException alreadyExistsException) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.CONFLICT)
            .error(HttpStatus.CONFLICT.getReasonPhrase())
            .message(alreadyExistsException.getMessage())
            .stacktrace(getAbbreviatedStackTrace(alreadyExistsException))
            .cause(getExceptionCause(alreadyExistsException))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  @Hidden
  @ExceptionHandler(InvalidSchemaEvolutionException.class)
  protected ResponseEntity<ErrorResponseBody> handleInvalidSchemaEvolution(
      InvalidSchemaEvolutionException invalidSchemaEvolutionException) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.BAD_REQUEST)
            .error(HttpStatus.BAD_REQUEST.getReasonPhrase())
            .message(invalidSchemaEvolutionException.getMessage())
            .stacktrace(getAbbreviatedStackTrace(invalidSchemaEvolutionException))
            .cause(getExceptionCause(invalidSchemaEvolutionException))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  @Hidden
  @ExceptionHandler(JobEngineException.class)
  protected ResponseEntity<ErrorResponseBody> handleJobEngineFailure(
      JobEngineException jobEngineException) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.SERVICE_UNAVAILABLE)
            .error(HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase())
            .message(jobEngineException.getMessage())
            .stacktrace(getAbbreviatedStackTrace(jobEngineException))
            .cause(getExceptionCause(jobEngineException))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  @Hidden
  @ExceptionHandler(UnsupportedClientOperationException.class)
  protected ResponseEntity<ErrorResponseBody> handleUnsupportedClientOperationException(
      UnsupportedClientOperationException unsupportedClientOperationException) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.BAD_REQUEST)
            .error(HttpStatus.BAD_REQUEST.getReasonPhrase())
            .message(unsupportedClientOperationException.getMessage())
            .stacktrace(getAbbreviatedStackTrace(unsupportedClientOperationException))
            .cause(getExceptionCause(unsupportedClientOperationException))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  @Hidden
  @ExceptionHandler(AccessDeniedException.class)
  protected ResponseEntity<ErrorResponseBody> handleAccessDeniedException(
      AccessDeniedException accessDeniedException) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.FORBIDDEN)
            .error(HttpStatus.FORBIDDEN.getReasonPhrase())
            .message(accessDeniedException.getMessage())
            .stacktrace(getAbbreviatedStackTrace(accessDeniedException))
            .cause(getExceptionCause(accessDeniedException))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  @Hidden
  @ExceptionHandler(IllegalStateException.class)
  protected ResponseEntity<ErrorResponseBody> handleIllegalStateException(
      IllegalStateException illegalStateException) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .error(HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase())
            .message(illegalStateException.getMessage())
            .stacktrace(getAbbreviatedStackTrace(illegalStateException))
            .cause(getExceptionCause(illegalStateException))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  @Hidden
  @ExceptionHandler(AuthorizationServiceException.class)
  protected ResponseEntity<ErrorResponseBody> handleAuthzServiceFailure(
      AuthorizationServiceException authorizationServiceException) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.SERVICE_UNAVAILABLE)
            .error(HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase())
            .message(authorizationServiceException.getMessage())
            .stacktrace(getAbbreviatedStackTrace(authorizationServiceException))
            .cause(getExceptionCause(authorizationServiceException))
            .build();
    return buildResponseEntity(errorResponseBody);
  }

  /**
   * Springboot fails for incompatible request enums without any useful message. This controller
   * advice changes the response to a useful error message.
   */
  @Override
  public ResponseEntity<Object> handleHttpMessageNotReadable(
      HttpMessageNotReadableException exception,
      HttpHeaders headers,
      HttpStatus status,
      WebRequest request) {
    String genericMessage = "Unacceptable JSON " + exception.getMessage();
    String errorDetails = genericMessage;

    if (exception.getCause() instanceof InvalidFormatException) {
      InvalidFormatException ifx = (InvalidFormatException) exception.getCause();
      if (ifx.getTargetType() != null && ifx.getTargetType().isEnum()) {
        errorDetails =
            String.format(
                "Invalid enum value: '%s' for the field: '%s'. The value must be one of: %s.",
                ifx.getValue(),
                ifx.getPath().get(ifx.getPath().size() - 1).getFieldName(),
                Arrays.toString(ifx.getTargetType().getEnumConstants()));
      }
    }
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.BAD_REQUEST)
            .error(HttpStatus.BAD_REQUEST.getReasonPhrase())
            .message(errorDetails)
            .stacktrace(getAbbreviatedStackTrace(exception))
            .cause(getExceptionCause(exception))
            .build();
    return handleExceptionInternal(
        exception, errorResponseBody, headers, HttpStatus.BAD_REQUEST, request);
  }

  /**
   * Handles all other exceptions either not defined above or are runtime exceptions. With this
   * method, we handle all exceptions occurred in the controller properly limiting our response path
   * to be two: either success, or failed, but no exception.
   */
  @Hidden
  @ExceptionHandler(Exception.class)
  protected ResponseEntity<ErrorResponseBody> handleGenericException(Exception exception) {
    ErrorResponseBody errorResponseBody =
        ErrorResponseBody.builder()
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .error(HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase())
            .message(exception.toString())
            .stacktrace(getAbbreviatedStackTrace(exception))
            .cause(getExceptionCause(exception))
            .build();
    log.error("Exception thrown by internal server:\n", exception);
    return buildResponseEntity(errorResponseBody);
  }

  private ResponseEntity<ErrorResponseBody> buildResponseEntity(
      ErrorResponseBody errorResponseBody) {
    return new ResponseEntity<>(errorResponseBody, errorResponseBody.getStatus());
  }

  /**
   * Gets reduced size stacktrace. Abbreviated upto defaulted max width. Extracts partial stacktrace
   * by skipping in between contents. Returns empty or partial stacktrace if there is an exception.
   *
   * @param exception
   * @return String
   */
  private String getAbbreviatedStackTrace(Throwable exception) {
    String stackTrace = ExceptionUtils.getStackTrace(exception);
    if (StringUtils.isEmpty(stackTrace)) {
      return null;
    }
    // Return the complete stacktrace if size is max width (i.e. 6000)
    if (stackTrace.length() <= STACKTRACE_MAX_WIDTH) {
      return stackTrace;
    }
    StringBuilder builder = new StringBuilder();
    // Extract the first level stacktrace with max width of 1500 so that we get better view of top
    // level stacktrace
    builder.append(StringUtils.abbreviate(stackTrace, 0, 1500));
    // Extract minimal stacktrace from the middle levels
    int width = 600;
    // skip every 2000 characters
    int skipLength = 2000;
    // Start the next scan from 6000
    int startOffset = 6000;
    // So far 1500 is extracted from top level
    int abbreviatedLength = 1500;
    // Flag to track the deepest level
    boolean isDeepestLevel = false;
    while (startOffset + width <= stackTrace.length()) {
      try {
        builder.append(StringUtils.abbreviate(stackTrace, startOffset, width));
        abbreviatedLength += width;
        if (isDeepestLevel || abbreviatedLength == STACKTRACE_MAX_WIDTH) {
          break;
        }
        // Extract the deepest level of stacktrace from the remaining stacktrace
        if (startOffset + width + skipLength > stackTrace.length()) {
          // reset the skip length to width which is already extracted in this scan so that next
          // offset can be started
          // without skipping characters
          skipLength = width;
          // Determine the min characters that can be extracted
          width =
              Math.min(
                  stackTrace.length() - (startOffset + width),
                  STACKTRACE_MAX_WIDTH - abbreviatedLength);
          isDeepestLevel = true;
        }
        // Start the next start offset
        startOffset += skipLength;
      } catch (IllegalArgumentException ex) {
        return builder.toString();
      }
    }
    return builder.toString();
  }

  private String getExceptionCause(Throwable exception) {
    return exception.getCause() != null ? exception.getCause().getMessage() : CAUSE_NOT_AVAILABLE;
  }
}
