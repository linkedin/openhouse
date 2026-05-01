package com.linkedin.openhouse.optimizer.api.controller;

import com.linkedin.openhouse.optimizer.api.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.service.OptimizerDataService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Name-keyed read endpoints for human/analyst convenience. UUID-keyed endpoints elsewhere remain
 * the canonical path for machine callers, since drop-and-recreate of a table produces a new
 * optimizer identity that a name-only lookup would conflate with the dropped table.
 */
@RestController
@RequestMapping("/v1/optimizer/databases/{databaseName}/tables/{tableName}")
@RequiredArgsConstructor
public class TableByNameController {

  private final OptimizerDataService service;

  /** Operation history for a table by (database, table) name, newest first. */
  @GetMapping("/operations-history")
  public ResponseEntity<List<TableOperationsHistoryDto>> getOperationsHistoryByName(
      @PathVariable String databaseName,
      @PathVariable String tableName,
      @RequestParam(defaultValue = "100") int limit) {
    return ResponseEntity.ok(
        service.listHistory(databaseName, tableName, null, null, null, null, null, limit));
  }
}
