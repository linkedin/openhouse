package com.linkedin.openhouse.optimizer.api.controller;

import com.linkedin.openhouse.optimizer.api.model.OperationHistoryStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.service.OptimizerDataService;
import java.time.Instant;
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

/** REST controller for {@code table_operations_history}. */
@RestController
@RequestMapping("/v1/table-operations-history")
@RequiredArgsConstructor
public class TableOperationsHistoryController {

  private final OptimizerDataService service;

  /** Append a completed-job result. Called by the SparkJob after each run (success or failure). */
  @PostMapping
  public ResponseEntity<TableOperationsHistoryDto> appendHistory(
      @RequestBody TableOperationsHistoryDto dto) {
    return ResponseEntity.status(HttpStatus.CREATED).body(service.appendHistory(dto));
  }

  /** Return the most recent history for a table, newest first, up to {@code limit} rows. */
  @GetMapping("/{tableUuid}")
  public ResponseEntity<List<TableOperationsHistoryDto>> getHistory(
      @PathVariable String tableUuid, @RequestParam(defaultValue = "100") int limit) {
    return ResponseEntity.ok(service.getHistory(tableUuid, limit));
  }

  /**
   * List history rows matching the given filters, ordered newest first. All parameters are optional
   * — omit all to return every row up to {@code limit}.
   */
  @GetMapping
  public ResponseEntity<List<TableOperationsHistoryDto>> listHistory(
      @RequestParam(required = false) String databaseName,
      @RequestParam(required = false) String tableName,
      @RequestParam(required = false) String tableUuid,
      @RequestParam(required = false) OperationType operationType,
      @RequestParam(required = false) OperationHistoryStatus status,
      @RequestParam(required = false) Instant since,
      @RequestParam(required = false) Instant until,
      @RequestParam(defaultValue = "100") int limit) {
    return ResponseEntity.ok(
        service.listHistory(
            databaseName, tableName, tableUuid, operationType, status, since, until, limit));
  }
}
