package com.linkedin.openhouse.optimizer.api.controller;

import com.linkedin.openhouse.optimizer.api.spec.CompleteOperationRequestDto;
import com.linkedin.openhouse.optimizer.api.spec.OperationStatusDto;
import com.linkedin.openhouse.optimizer.api.spec.OperationTypeDto;
import com.linkedin.openhouse.optimizer.api.spec.TableOperationsDto;
import com.linkedin.openhouse.optimizer.api.spec.TableOperationsHistoryDto;
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
   * Report that an operation has completed. The body carries the {@code operationId} the caller is
   * completing along with its terminal status. The backend looks up the operation row, writes a
   * history entry with the operation's table metadata, and returns 201 Created with the history
   * row, or 404 if the operation does not exist.
   */
  @PostMapping("/complete")
  public ResponseEntity<TableOperationsHistoryDto> completeOperation(
      @RequestBody CompleteOperationRequestDto request) {
    return service
        .completeOperation(
            request.getOperationId(),
            request.getStatus() == null ? null : request.getStatus().toModel())
        .map(
            history ->
                ResponseEntity.status(HttpStatus.CREATED)
                    .body(TableOperationsHistoryDto.fromModel(history)))
        .orElse(ResponseEntity.notFound().build());
  }

  /** Fetch a single operation row by its ID, regardless of status. Returns 404 if not found. */
  @GetMapping("/{id}")
  public ResponseEntity<TableOperationsDto> getTableOperation(@PathVariable String id) {
    return service
        .getTableOperation(id)
        .map(TableOperationsDto::fromModel)
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
  }

  /**
   * List operations matching the given filters. All parameters are optional — omit all to return
   * every row.
   */
  @GetMapping
  public ResponseEntity<List<TableOperationsDto>> listTableOperations(
      @RequestParam(required = false) OperationTypeDto operationType,
      @RequestParam(required = false) OperationStatusDto status,
      @RequestParam(required = false) String databaseName,
      @RequestParam(required = false) String tableName,
      @RequestParam(required = false) String tableUuid) {
    List<TableOperationsDto> result =
        service
            .listTableOperations(
                Optional.ofNullable(operationType).map(OperationTypeDto::toModel),
                Optional.ofNullable(status).map(OperationStatusDto::toModel),
                Optional.ofNullable(databaseName),
                Optional.ofNullable(tableName),
                Optional.ofNullable(tableUuid))
            .stream()
            .map(TableOperationsDto::fromModel)
            .collect(Collectors.toList());
    return ResponseEntity.ok(result);
  }
}
