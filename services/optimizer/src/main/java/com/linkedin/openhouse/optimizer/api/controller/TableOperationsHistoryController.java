package com.linkedin.openhouse.optimizer.api.controller;

import com.linkedin.openhouse.optimizer.api.spec.ApiListLimitProperties;
import com.linkedin.openhouse.optimizer.api.spec.TableOperationsHistory;
import com.linkedin.openhouse.optimizer.service.OptimizerDataService;
import java.util.List;
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

/** REST controller for {@code table_operations_history}. */
@RestController
@RequestMapping("/v1/optimizer/operations-history")
@RequiredArgsConstructor
public class TableOperationsHistoryController {

  private final OptimizerDataService service;
  private final ApiListLimitProperties limits;

  /** Append a completed-job result. Called by the SparkJob after each run (success or failure). */
  @PostMapping
  public ResponseEntity<TableOperationsHistory> appendHistory(
      @RequestBody TableOperationsHistory dto) {
    return ResponseEntity.status(HttpStatus.CREATED)
        .body(TableOperationsHistory.fromModel(service.appendHistory(dto.toModel())));
  }

  /**
   * Return the most recent history for a table, newest first. {@code limit} defaults to {@code
   * optimizer.api.list.default-limit} (10) and is rejected with 400 outside {@code [1,
   * optimizer.api.list.max-limit]} (1000).
   */
  @GetMapping("/{tableUuid}")
  public ResponseEntity<List<TableOperationsHistory>> getHistory(
      @PathVariable String tableUuid, @RequestParam(required = false) Integer limit) {
    int effectiveLimit = limits.validateAndResolve(limit);
    List<TableOperationsHistory> result =
        service.getHistory(tableUuid, effectiveLimit).stream()
            .map(TableOperationsHistory::fromModel)
            .collect(Collectors.toList());
    return ResponseEntity.ok(result);
  }
}
