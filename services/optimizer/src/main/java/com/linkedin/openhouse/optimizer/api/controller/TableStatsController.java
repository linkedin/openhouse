package com.linkedin.openhouse.optimizer.api.controller;

import com.linkedin.openhouse.optimizer.api.spec.TableStats;
import com.linkedin.openhouse.optimizer.api.spec.TableStatsHistory;
import com.linkedin.openhouse.optimizer.api.spec.UpsertTableStatsRequest;
import com.linkedin.openhouse.optimizer.service.OptimizerDataService;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** REST controller for managing per-table stats in the optimizer DB. */
@RestController
@RequestMapping("/v1/optimizer/stats")
@RequiredArgsConstructor
public class TableStatsController {

  private final OptimizerDataService service;

  /**
   * Create or overwrite the stats row for {@code tableUuid}. Called by the Tables Service on every
   * Iceberg commit. Idempotent.
   */
  @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "Stats PUT: OK")})
  @PutMapping("/{tableUuid}")
  public ResponseEntity<TableStats> upsertTableStats(
      @PathVariable String tableUuid, @RequestBody UpsertTableStatsRequest request) {
    return ResponseEntity.ok(
        TableStats.fromModel(service.upsertTableStats(request.toModel(tableUuid))));
  }

  /** Fetch the stats row for {@code tableUuid}. Returns 404 if no stats have been written yet. */
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Stats GET: OK"),
        @ApiResponse(responseCode = "404", description = "Stats GET: NOT_FOUND")
      })
  @GetMapping("/{tableUuid}")
  public ResponseEntity<TableStats> getTableStats(@PathVariable String tableUuid) {
    return service
        .getTableStats(tableUuid)
        .map(TableStats::fromModel)
        .map(ResponseEntity::ok)
        .orElseThrow(
            () ->
                new ResponseStatusException(
                    HttpStatus.NOT_FOUND, String.format("no stats for tableUuid %s", tableUuid)));
  }

  /**
   * List stats rows matching the given filters, capped at {@code limit} rows. Every filter is
   * optional; {@code limit} is required so callers always state how much they want back.
   */
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Stats SEARCH: OK"),
        @ApiResponse(responseCode = "400", description = "Stats SEARCH: BAD_REQUEST")
      })
  @GetMapping
  public ResponseEntity<List<TableStats>> listTableStats(
      @RequestParam(required = false) String databaseName,
      @RequestParam(required = false) String tableName,
      @RequestParam(required = false) String tableUuid,
      @RequestParam int limit) {
    List<TableStats> result =
        service
            .listTableStats(
                Optional.ofNullable(databaseName),
                Optional.ofNullable(tableName),
                Optional.ofNullable(tableUuid),
                limit)
            .stream()
            .map(TableStats::fromModel)
            .collect(Collectors.toList());
    return ResponseEntity.ok(result);
  }

  /**
   * Return per-commit stats history for {@code tableUuid}, newest first, capped at {@code limit}
   * rows. Optional {@code since} filter (inclusive). {@code limit} is required.
   */
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "StatsHistory GET: OK"),
        @ApiResponse(responseCode = "400", description = "StatsHistory GET: BAD_REQUEST")
      })
  @GetMapping("/{tableUuid}/history")
  public ResponseEntity<List<TableStatsHistory>> getStatsHistory(
      @PathVariable String tableUuid,
      @RequestParam(required = false) Instant since,
      @RequestParam int limit) {
    List<TableStatsHistory> result =
        service.getStatsHistory(tableUuid, Optional.ofNullable(since), limit).stream()
            .map(TableStatsHistory::fromModel)
            .collect(Collectors.toList());
    return ResponseEntity.ok(result);
  }
}
