package com.linkedin.openhouse.tables.controller;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException.Origin;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException.Reason;
import io.swagger.v3.oas.annotations.Hidden;
import java.util.UUID;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Keeps checked, pre-commit column-default failures out of the common catch-all handler. */
@Slf4j
@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice(assignableTypes = {TablesController.class, IcebergSnapshotsController.class})
public class ColumnDefaultExceptionHandler {
  private static final Pattern IDENTIFIER = Pattern.compile("[a-zA-Z0-9_.-]{1,256}");
  private static final Pattern TYPE =
      Pattern.compile(
          "(?i)(boolean|bool|int|integer|long|float|double|string|bytes|binary|fixed|decimal|"
              + "date|time|timestamp|timestamptz|uuid|enum|record|struct|array|list|map|union|null)"
              + "(\\([0-9]{1,10}(, ?[0-9]{1,10})?\\)|\\[[0-9]{1,10}\\]|"
              + "\\((date|time-(millis|micros)|((local-)?timestamp)-(millis|micros|nanos)|"
              + "uuid|decimal|duration)\\))?");

  @Hidden
  @ExceptionHandler(ColumnDefaultException.class)
  public ResponseEntity<ErrorResponseBody> handleColumnDefault(ColumnDefaultException failure) {
    String requestId = UUID.randomUUID().toString();
    HttpStatus status = status(failure);
    log.error(
        "Column-default write rejected: requestId={} reason={} origin={} table={}.{} fieldId={} column={} sourceType={} targetType={}",
        requestId,
        failure.getReason(),
        failure.getOrigin(),
        failure.getDatabaseId(),
        failure.getTableId(),
        failure.getFieldId(),
        failure.getColumnPath(),
        failure.getSourceType(),
        failure.getTargetType(),
        failure);
    ErrorResponseBody body =
        ErrorResponseBody.builder()
            .status(status)
            .error(status.getReasonPhrase())
            .message(message(failure, requestId))
            .code("COLUMN_DEFAULT_" + failure.getReason().name())
            .requestId(requestId)
            .retryable(failure.getReason() == Reason.UNAVAILABLE)
            .build();
    return new ResponseEntity<>(body, status);
  }

  private static HttpStatus status(ColumnDefaultException failure) {
    switch (failure.getReason()) {
      case UNAVAILABLE:
        return HttpStatus.SERVICE_UNAVAILABLE;
      case INTERNAL:
        return HttpStatus.INTERNAL_SERVER_ERROR;
      case REMOVED:
      case REWRITE:
        return HttpStatus.BAD_REQUEST;
      case INVALID_SCHEMA:
      case INVALID_VALUE:
      case TYPE_MISMATCH:
      case OUT_OF_RANGE:
        return failure.getOrigin() == Origin.INCOMING
            ? HttpStatus.BAD_REQUEST
            : HttpStatus.INTERNAL_SERVER_ERROR;
      default:
        throw new IllegalStateException("Unmapped column-default reason: " + failure.getReason());
    }
  }

  private static String message(ColumnDefaultException failure, String requestId) {
    String context = context(failure);
    switch (failure.getReason()) {
      case UNAVAILABLE:
        return "Write rejected before commit"
            + context
            + ": column-default metadata or configuration is temporarily unavailable. Retry later.";
      case INTERNAL:
        return "Write rejected before commit"
            + context
            + ": an internal error prevented column-default validation. Contact support with requestId "
            + requestId
            + "; retrying unchanged is not recommended.";
      case REMOVED:
        return "Write rejected before commit"
            + context
            + ": an existing column default was omitted. Use a column-default-aware client"
            + " and preserve existing defaults; removing them is not supported.";
      case REWRITE:
        return "Write rejected before commit"
            + context
            + ": overwrite or replace did not preserve an existing column default. Use a"
            + " column-default-aware writer that preserves existing defaults while rewriting data.";
      case INVALID_SCHEMA:
      case INVALID_VALUE:
      case TYPE_MISMATCH:
      case OUT_OF_RANGE:
        String issue = validationIssue(failure.getReason());
        if (failure.getOrigin() == Origin.INCOMING) {
          return "Write rejected before commit"
              + context
              + ": "
              + issue
              + ". Correct the incoming schema or column default before submitting again.";
        }
        if (failure.getOrigin() == Origin.STORED) {
          return "Write rejected before commit"
              + context
              + ": stored column-default metadata needs repair ("
              + issue
              + "). Contact the table owner or support with requestId "
              + requestId
              + "; retrying the same write will not repair the metadata.";
        }
        return "Write rejected before commit"
            + context
            + ": column-default validation failed ("
            + issue
            + "). Contact support with requestId "
            + requestId
            + " to identify the metadata that needs correction before submitting again.";
      default:
        throw new IllegalStateException("Unmapped column-default reason: " + failure.getReason());
    }
  }

  private static String validationIssue(Reason reason) {
    switch (reason) {
      case INVALID_SCHEMA:
        return "the schema cannot be used to validate column defaults";
      case INVALID_VALUE:
        return "a column default is not a valid value";
      case TYPE_MISMATCH:
        return "a column default is incompatible with the column type";
      case OUT_OF_RANGE:
        return "a column default is outside the supported range of its column type";
      default:
        throw new IllegalStateException("Not a column-default validation reason: " + reason);
    }
  }

  /** Include identifiers and canonical type labels only, never raw schemas, values or locations. */
  private static String context(ColumnDefaultException failure) {
    StringBuilder result = new StringBuilder();
    if (safe(failure.getDatabaseId(), IDENTIFIER) && safe(failure.getTableId(), IDENTIFIER)) {
      result
          .append(" for table ")
          .append(failure.getDatabaseId())
          .append('.')
          .append(failure.getTableId());
    }
    if (safe(failure.getColumnPath(), IDENTIFIER)) {
      result.append(", column ").append(failure.getColumnPath());
    }
    if (failure.getFieldId() != null) {
      result.append(" (field ID ").append(failure.getFieldId()).append(')');
    }
    if (safe(failure.getSourceType(), TYPE)) {
      result.append(", source type ").append(failure.getSourceType());
    }
    if (safe(failure.getTargetType(), TYPE)) {
      result.append(", target type ").append(failure.getTargetType());
    }
    return result.toString();
  }

  private static boolean safe(String value, Pattern pattern) {
    return value != null && value.length() <= 256 && pattern.matcher(value).matches();
  }
}
