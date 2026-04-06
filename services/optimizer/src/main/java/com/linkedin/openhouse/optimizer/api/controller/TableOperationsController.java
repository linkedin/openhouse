package com.linkedin.openhouse.optimizer.api.controller;

import com.linkedin.openhouse.optimizer.api.model.CompleteOperationRequest;
import com.linkedin.openhouse.optimizer.api.model.OperationStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsDto;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.service.OptimizerDataService;
import java.util.List;
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
@RequestMapping("/v1/table-operations")
@RequiredArgsConstructor
public class TableOperationsController {

  private final OptimizerDataService service;

  /**
   * Report that an operation has completed. The backend looks up the operation row, writes a
   * history entry with the operation's table metadata and the supplied result. Returns 201 Created
   * with the history row, or 404 if the operation does not exist.
   */
  @PostMapping("/{id}/complete")
  public ResponseEntity<TableOperationsHistoryDto> completeOperation(
      @PathVariable String id, @RequestBody CompleteOperationRequest request) {
    return service
        .completeOperation(id, request)
        .map(dto -> ResponseEntity.status(HttpStatus.CREATED).body(dto))
        .orElse(ResponseEntity.notFound().build());
  }

  /** Fetch a single operation row by its ID, regardless of status. Returns 404 if not found. */
  @GetMapping("/{id}")
  public ResponseEntity<TableOperationsDto> getTableOperation(@PathVariable String id) {
    return service
        .getTableOperation(id)
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
  }

  /**
   * List operations matching the given filters. All parameters are optional — omit all to return
   * every row.
   */
  @GetMapping
  public ResponseEntity<List<TableOperationsDto>> listTableOperations(
      @RequestParam(required = false) OperationType operationType,
      @RequestParam(required = false) OperationStatus status,
      @RequestParam(required = false) String databaseName,
      @RequestParam(required = false) String tableName,
      @RequestParam(required = false) String tableUuid) {
    return ResponseEntity.ok(
        service.listTableOperations(operationType, status, databaseName, tableName, tableUuid));
  }
}
