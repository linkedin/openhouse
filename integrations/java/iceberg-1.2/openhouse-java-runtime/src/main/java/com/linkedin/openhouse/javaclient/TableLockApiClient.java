package com.linkedin.openhouse.javaclient;

import com.linkedin.openhouse.javaclient.exception.TableLockException;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.model.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.LockState;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

final class TableLockApiClient {
  /**
   * Reasons a caller may name in SQL. LEGACY identifies the unqualified lock that {@code LOCK
   * TABLE} and {@code UNLOCK TABLE} manage, so callers reach it by omitting {@code WITH REASON}.
   */
  private static final String SUPPORTED_REASONS =
      Arrays.stream(CreateUpdateLockRequestBody.ReasonEnum.values())
          .filter(reason -> reason != CreateUpdateLockRequestBody.ReasonEnum.LEGACY)
          .map(CreateUpdateLockRequestBody.ReasonEnum::getValue)
          .collect(Collectors.joining(", "));

  private TableLockApiClient() {}

  static void lockTable(
      TableApi tableApi,
      TableIdentifier tableIdentifier,
      Optional<String> reason,
      Optional<String> message)
      throws TableLockException {
    validateTableIdentifier(tableIdentifier);
    if (message.isPresent() && !reason.isPresent()) {
      throw new TableLockException("A table lock message requires a structured reason");
    }
    if (message.filter(value -> value.trim().isEmpty()).isPresent()) {
      throw new TableLockException("A table lock message must contain non-whitespace text");
    }

    CreateUpdateLockRequestBody requestBody =
        new CreateUpdateLockRequestBody()
            .locked(true)
            .reason(CreateUpdateLockRequestBody.ReasonEnum.LEGACY)
            .creationTime(System.currentTimeMillis())
            .expirationInDays(0);
    message.ifPresent(requestBody::message);
    if (reason.isPresent()) {
      requestBody
          .reason(toStructuredLockReason(reason.get()))
          .expectedTableUUID(
              Optional.ofNullable(getTable(tableApi, tableIdentifier).getTableUUID())
                  .orElseThrow(
                      () ->
                          new TableLockException(
                              "Tables service response omitted the table UUID for "
                                  + tableIdentifier)));
    }

    executeRequest(
        () ->
            tableApi.createLockV1(
                tableIdentifier.namespace().toString(), tableIdentifier.name(), requestBody));
  }

  static void unlockTable(
      TableApi tableApi, TableIdentifier tableIdentifier, Optional<String> reason)
      throws TableLockException {
    validateTableIdentifier(tableIdentifier);
    if (!reason.isPresent()) {
      executeRequest(
          () ->
              tableApi.deleteLockV1(
                  tableIdentifier.namespace().toString(), tableIdentifier.name()));
      return;
    }
    String requestedReason = toStructuredLockReason(reason.get()).getValue();

    Optional<LockState> lockState =
        Optional.ofNullable(getTable(tableApi, tableIdentifier).getPolicies())
            .map(policies -> policies.getLockState())
            .filter(lock -> Boolean.TRUE.equals(lock.getLocked()));
    if (!lockState.isPresent()) {
      return;
    }

    LockState.ReasonEnum currentReason =
        Optional.ofNullable(lockState.get().getReason())
            .orElseThrow(
                () ->
                    new TableLockException(
                        "Table "
                            + tableIdentifier
                            + " reports a lock reason this client version does not recognize"));
    if (currentReason == LockState.ReasonEnum.LEGACY) {
      throw new TableLockException(
          "Table " + tableIdentifier + " has a legacy lock; use UNLOCK TABLE without WITH REASON");
    }
    if (!requestedReason.equals(currentReason.getValue())) {
      throw new TableLockException(
          "Table "
              + tableIdentifier
              + " is locked with reason "
              + currentReason.getValue()
              + ", not "
              + requestedReason);
    }

    String tableUUID =
        Optional.ofNullable(lockState.get().getTableUUID())
            .orElseThrow(
                () ->
                    new TableLockException(
                        "Lock with reason "
                            + currentReason.getValue()
                            + " on "
                            + tableIdentifier
                            + " omitted its table UUID"));
    String lockOwner =
        Optional.ofNullable(lockState.get().getLockOwner())
            .orElseThrow(
                () ->
                    new TableLockException(
                        "Lock with reason "
                            + currentReason.getValue()
                            + " on "
                            + tableIdentifier
                            + " omitted its lock owner"));
    executeRequest(
        () ->
            tableApi.deleteLockByReasonV1(
                tableIdentifier.namespace().toString(),
                tableIdentifier.name(),
                requestedReason,
                tableUUID,
                lockOwner));
  }

  private static void validateTableIdentifier(TableIdentifier tableIdentifier)
      throws TableLockException {
    if (tableIdentifier.namespace().levels().length > 1) {
      throw new TableLockException(
          "Input namespace has more than one levels "
              + String.join(".", tableIdentifier.namespace().levels()));
    }
  }

  /**
   * Resolves the reason a caller named in SQL. The generated enum returns null for a value it does
   * not know, and LEGACY belongs to the unqualified commands, so both resolve to a rejection.
   */
  private static CreateUpdateLockRequestBody.ReasonEnum toStructuredLockReason(String reason)
      throws TableLockException {
    return Optional.ofNullable(
            CreateUpdateLockRequestBody.ReasonEnum.fromValue(reason.toUpperCase(Locale.ROOT)))
        .filter(value -> value != CreateUpdateLockRequestBody.ReasonEnum.LEGACY)
        .orElseThrow(
            () ->
                new TableLockException(
                    "Unsupported table lock reason '"
                        + reason
                        + "'. Supported reasons: "
                        + SUPPORTED_REASONS));
  }

  private static GetTableResponseBody getTable(TableApi tableApi, TableIdentifier tableIdentifier)
      throws TableLockException {
    return executeRequest(
            () ->
                tableApi.getTableV1(tableIdentifier.namespace().toString(), tableIdentifier.name()))
        .orElseThrow(
            () ->
                new TableLockException(
                    "Tables service returned an empty response for " + tableIdentifier));
  }

  private static <T> Optional<T> executeRequest(Supplier<Mono<T>> request)
      throws TableLockException {
    try {
      return request.get().blockOptional();
    } catch (WebClientResponseException exception) {
      throw new TableLockException(
          exception.getStatusCode().value(), exception.getResponseBodyAsString(), exception);
    } catch (WebClientRequestException exception) {
      throw new TableLockException(
          "Table lock request failed before receiving a response", exception);
    }
  }
}
