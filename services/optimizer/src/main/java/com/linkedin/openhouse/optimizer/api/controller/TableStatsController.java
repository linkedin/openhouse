package com.linkedin.openhouse.optimizer.api.controller;

import com.linkedin.openhouse.optimizer.api.spec.ApiListLimitProperties;
import com.linkedin.openhouse.optimizer.api.spec.TableStats;
import com.linkedin.openhouse.optimizer.api.spec.TableStatsHistory;
import com.linkedin.openhouse.optimizer.api.spec.UpsertTableStatsRequest;
import com.linkedin.openhouse.optimizer.service.OptimizerDataService;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** REST controller for managing per-table stats in the optimizer DB. */
@RestController
@RequestMapping("/v1/optimizer/stats")
@RequiredArgsConstructor
public class TableStatsController {

  private final OptimizerDataService service;
  private final ApiListLimitProperties limits;

  /**
   * Create or overwrite the stats row for {@code tableUuid}. Called by the Tables Service on every
   * Iceberg commit. Idempotent.
   */
  @PutMapping("/{tableUuid}")
  public ResponseEntity<TableStats> upsertTableStats(
      @PathVariable String tableUuid, @RequestBody UpsertTableStatsRequest request) {
    return ResponseEntity.ok(
        TableStats.fromModel(service.upsertTableStats(request.toModel(tableUuid))));
  }

  /** Fetch the stats row for {@code tableUuid}. Returns 404 if no stats have been written yet. */
  @GetMapping("/{tableUuid}")
  public ResponseEntity<TableStats> getTableStats(@PathVariable String tableUuid) {
    return service
        .getTableStats(tableUuid)
        .map(TableStats::fromModel)
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
  }

  /**
   * List stats rows matching the given filters. Every filter is optional. {@code limit} defaults to
   * {@code optimizer.api.list.default-limit} (10) and is rejected with 400 outside {@code [1,
   * optimizer.api.list.max-limit]} (1000).
   */
  @GetMapping
  public ResponseEntity<List<TableStats>> listTableStats(
      @RequestParam(required = false) String databaseName,
      @RequestParam(required = false) String tableName,
      @RequestParam(required = false) String tableUuid,
      @RequestParam(required = false) Integer limit) {
    int effectiveLimit = limits.validateAndResolve(limit);
    List<TableStats> result =
        service
            .listTableStats(
                Optional.ofNullable(databaseName),
                Optional.ofNullable(tableName),
                Optional.ofNullable(tableUuid),
                effectiveLimit)
            .stream()
            .map(TableStats::fromModel)
            .collect(Collectors.toList());
    return ResponseEntity.ok(result);
  }

  /**
   * Return per-commit stats history for {@code tableUuid}, newest first. Optional {@code since}
   * filter (inclusive). {@code limit} defaults to {@code optimizer.api.list.default-limit} (10) and
   * is rejected with 400 outside {@code [1, optimizer.api.list.max-limit]} (1000).
   */
  @GetMapping("/{tableUuid}/history")
  public ResponseEntity<List<TableStatsHistory>> getStatsHistory(
      @PathVariable String tableUuid,
      @RequestParam(required = false) Instant since,
      @RequestParam(required = false) Integer limit) {
    int effectiveLimit = limits.validateAndResolve(limit);
    List<TableStatsHistory> result =
        service.getStatsHistory(tableUuid, Optional.ofNullable(since), effectiveLimit).stream()
            .map(TableStatsHistory::fromModel)
            .collect(Collectors.toList());
    return ResponseEntity.ok(result);
  }
}
