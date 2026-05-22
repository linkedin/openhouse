package com.linkedin.openhouse.optimizer.api.error;

import com.linkedin.openhouse.optimizer.api.controller.TableOperationsController;
import com.linkedin.openhouse.optimizer.api.controller.TableOperationsHistoryController;
import com.linkedin.openhouse.optimizer.api.controller.TableStatsController;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

/**
 * Scoped to the optimizer REST controllers. Two cases only: pass through any {@link
 * ResponseStatusException} that a controller threw, and convert any other uncaught exception into a
 * 500. Framework-level 4xx responses (missing query param, malformed body, etc.) are left to
 * Spring's defaults — this advice intentionally does not blanket every possible exception type.
 */
@Slf4j
@RestControllerAdvice(
    assignableTypes = {
      TableOperationsController.class,
      TableOperationsHistoryController.class,
      TableStatsController.class
    })
public class GlobalExceptionHandler {

  @ExceptionHandler(ResponseStatusException.class)
  public ResponseEntity<ApiError> handleResponseStatus(
      ResponseStatusException e, HttpServletRequest req) {
    HttpStatus status = HttpStatus.resolve(e.getStatus().value());
    if (status == null) {
      status = HttpStatus.INTERNAL_SERVER_ERROR;
    }
    String message = e.getReason() == null ? status.getReasonPhrase() : e.getReason();
    return ResponseEntity.status(status)
        .body(
            ApiError.builder()
                .code(status.name())
                .message(message)
                .path(req.getRequestURI())
                .build());
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ApiError> handleUncaught(Exception e, HttpServletRequest req) {
    log.warn(String.format("Unhandled exception on %s", req.getRequestURI()), e);
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
        .body(
            ApiError.builder()
                .code("INTERNAL_ERROR")
                .message("An unexpected error occurred")
                .path(req.getRequestURI())
                .build());
  }
}
