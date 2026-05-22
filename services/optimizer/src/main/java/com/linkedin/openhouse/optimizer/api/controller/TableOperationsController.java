package com.linkedin.openhouse.optimizer.api.controller;

import com.linkedin.openhouse.optimizer.api.spec.OperationStatus;
import com.linkedin.openhouse.optimizer.api.spec.OperationType;
import com.linkedin.openhouse.optimizer.api.spec.TableOperations;
import com.linkedin.openhouse.optimizer.api.spec.TableOperationsHistory;
import com.linkedin.openhouse.optimizer.api.spec.UpdateOperationRequest;
import com.linkedin.openhouse.optimizer.service.OptimizerDataService;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** REST controller for {@code table_operations}. */
@RestController
@RequestMapping("/v1/optimizer/operations")
@RequiredArgsConstructor
public class TableOperationsController {

  private final OptimizerDataService service;

  /**
   * Report an update to an operation. {@code id} comes from the URL; the body's {@code operationId}
   * must match (the controller rejects mismatched requests with 400). The backend looks up the
   * operation row, writes a history entry with the operation's table metadata, and returns 201
   * Created with the history row, or 404 if the operation does not exist.
   */
  @PostMapping("/{id}/update")
  public ResponseEntity<TableOperationsHistory> updateOperation(
      @PathVariable String id, @RequestBody UpdateOperationRequest request) {
    if (!StringUtils.hasText(request.getOperationId())) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "operationId is required");
    }
    if (!Objects.equals(id, request.getOperationId())) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST,
          String.format(
              "operationId in body (%s) does not match path id (%s)",
              request.getOperationId(), id));
    }
    if (request.getStatus() == null) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "status is required");
    }
    return service
        .updateOperation(id, request.getStatus().toModel())
        .map(
            history ->
                ResponseEntity.status(HttpStatus.CREATED)
                    .body(TableOperationsHistory.fromModel(history)))
        .orElseThrow(
            () ->
                new ResponseStatusException(
                    HttpStatus.NOT_FOUND, String.format("no operation with id %s", id)));
  }

  /** Fetch a single operation row by its ID, regardless of status. Returns 404 if not found. */
  @GetMapping("/{id}")
  public ResponseEntity<TableOperations> getTableOperation(@PathVariable String id) {
    return service
        .getTableOperation(id)
        .map(TableOperations::fromModel)
        .map(ResponseEntity::ok)
        .orElseThrow(
            () ->
                new ResponseStatusException(
                    HttpStatus.NOT_FOUND, String.format("no operation with id %s", id)));
  }

  /**
   * List operations matching the given filters, capped at {@code limit} rows. Every filter is
   * optional; {@code limit} is required so callers always state how much they want back.
   */
  @GetMapping
  public ResponseEntity<List<TableOperations>> listTableOperations(
      @RequestParam(required = false) OperationType operationType,
      @RequestParam(required = false) OperationStatus status,
      @RequestParam(required = false) String databaseName,
      @RequestParam(required = false) String tableName,
      @RequestParam(required = false) String tableUuid,
      @RequestParam int limit) {
    List<TableOperations> result =
        service
            .listTableOperations(
                Optional.ofNullable(operationType).map(OperationType::toModel),
                Optional.ofNullable(status).map(OperationStatus::toModel),
                Optional.ofNullable(databaseName),
                Optional.ofNullable(tableName),
                Optional.ofNullable(tableUuid),
                limit)
            .stream()
            .map(TableOperations::fromModel)
            .collect(Collectors.toList());
    return ResponseEntity.ok(result);
  }
}
