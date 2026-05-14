package com.linkedin.openhouse.optimizer.api.controller;

import com.linkedin.openhouse.optimizer.api.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.model.mapper.ApiModelMapper;
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
  private final ApiModelMapper apiMapper;

  /** Append a completed-job result. Called by the SparkJob after each run (success or failure). */
  @PostMapping
  public ResponseEntity<TableOperationsHistoryDto> appendHistory(
      @RequestBody TableOperationsHistoryDto dto) {
    return ResponseEntity.status(HttpStatus.CREATED)
        .body(apiMapper.toDto(service.appendHistory(apiMapper.toHistory(dto))));
  }

  /** Return the most recent history for a table, newest first, up to {@code limit} rows. */
  @GetMapping("/{tableUuid}")
  public ResponseEntity<List<TableOperationsHistoryDto>> getHistory(
      @PathVariable String tableUuid, @RequestParam(defaultValue = "100") int limit) {
    List<TableOperationsHistoryDto> result =
        service.getHistory(tableUuid, limit).stream()
            .map(apiMapper::toDto)
            .collect(Collectors.toList());
    return ResponseEntity.ok(result);
  }
}
