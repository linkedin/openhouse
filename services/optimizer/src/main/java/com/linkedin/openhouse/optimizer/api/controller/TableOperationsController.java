package com.linkedin.openhouse.optimizer.api.controller;

import com.linkedin.openhouse.optimizer.api.spec.CompleteOperationRequest;
import com.linkedin.openhouse.optimizer.api.spec.OperationStatus;
import com.linkedin.openhouse.optimizer.api.spec.OperationType;
import com.linkedin.openhouse.optimizer.api.spec.TableOperations;
import com.linkedin.openhouse.optimizer.api.spec.TableOperationsHistory;
import com.linkedin.openhouse.optimizer.service.OptimizerDataService;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** REST controller for {@code table_operations}. */
@RestController
@RequestMapping("/v1/optimizer/operations")
@RequiredArgsConstructor
public class TableOperationsController {

  private final OptimizerDataService service;

  /**
   * Report that an operation has completed. {@code id} is the operation's UUID; the body carries
   * the terminal status and any per-operation metrics or error details. The backend looks up the
   * operation row, writes a history entry with the operation's table metadata plus the supplied
   * metrics, and returns 201 Created with the history row, or 404 if the operation does not exist.
   */
  @PostMapping("/{id}/complete")
  public ResponseEntity<TableOperationsHistory> completeOperation(
      @PathVariable String id, @RequestBody CompleteOperationRequest request) {
    return service
        .completeOperation(
            id,
            request.getStatus() == null ? null : request.getStatus().toModel(),
            request.getOrphanFilesDeleted(),
            request.getOrphanBytesDeleted(),
            request.getErrorMessage(),
            request.getErrorType())
        .map(
            history ->
                ResponseEntity.status(HttpStatus.CREATED)
                    .body(TableOperationsHistory.fromModel(history)))
        .orElse(ResponseEntity.notFound().build());
  }

  /** Fetch a single operation row by its ID, regardless of status. Returns 404 if not found. */
  @GetMapping("/{id}")
  public ResponseEntity<TableOperations> getTableOperation(@PathVariable String id) {
    return service
        .getTableOperation(id)
        .map(TableOperations::fromModel)
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
  }

  /**
   * List operations matching the given filters. All parameters are optional — omit all to return
   * every row.
   */
  @GetMapping
  public ResponseEntity<List<TableOperations>> listTableOperations(
      @RequestParam(required = false) OperationType operationType,
      @RequestParam(required = false) OperationStatus status,
      @RequestParam(required = false) String databaseName,
      @RequestParam(required = false) String tableName,
      @RequestParam(required = false) String tableUuid) {
    List<TableOperations> result =
        service
            .listTableOperations(
                Optional.ofNullable(operationType).map(OperationType::toModel),
                Optional.ofNullable(status).map(OperationStatus::toModel),
                Optional.ofNullable(databaseName),
                Optional.ofNullable(tableName),
                Optional.ofNullable(tableUuid))
            .stream()
            .map(TableOperations::fromModel)
            .collect(Collectors.toList());
    return ResponseEntity.ok(result);
  }
}
