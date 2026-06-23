package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.jobs.client.TablesClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Shapes the already-ranked, already-capped partition strategies (selected in Trino by {@code
 * DloPartitionStrategiesReader}) into one partition-scope {@link TableDataLayoutMetadata} per
 * table, keeping only tables that are explicitly opted into partition-scope compaction.
 */
@Slf4j
public final class DataLayoutPartitionUtil {

  private DataLayoutPartitionUtil() {}

  /**
   * @param selectedStrategiesByTable map of fully-qualified table name to that table's selected
   *     partition strategies (already ranked and capped by the Trino query)
   * @param tablesClient used to check the explicit partition-scope opt-in property per table
   * @return one partition-scope metadata per opted-in table that has at least one selected strategy
   */
  public static List<TableDataLayoutMetadata> toPartitionTableDataLayoutMetadata(
      Map<String, List<DataLayoutStrategy>> selectedStrategiesByTable, TablesClient tablesClient) {
    List<TableDataLayoutMetadata> result = new ArrayList<>();
    for (Map.Entry<String, List<DataLayoutStrategy>> entry : selectedStrategiesByTable.entrySet()) {
      String fqtn = entry.getKey();
      List<DataLayoutStrategy> strategies = entry.getValue();
      if (strategies.isEmpty()) {
        continue;
      }
      String[] tokens = fqtn.split("\\.", 2);
      if (tokens.length != 2) {
        log.warn("Skipping malformed fqtn {} from partition strategies", fqtn);
        continue;
      }
      String dbName = tokens[0];
      String tableName = tokens[1];
      if (!tablesClient.isPartitionScopeEnabled(dbName, tableName)) {
        log.info("Skipping {} - not opted into partition-scope compaction", fqtn);
        continue;
      }
      double totalCost = strategies.stream().mapToDouble(DataLayoutStrategy::getCost).sum();
      log.info(
          "Selected {} partition strategies for {} (estimated compute cost {} GB-hr)",
          strategies.size(),
          fqtn,
          totalCost);
      result.add(
          TableDataLayoutMetadata.builder()
              .dbName(dbName)
              .tableName(tableName)
              .isPrimary(true)
              .isPartitionScope(true)
              .dataLayoutStrategies(strategies)
              .build());
    }
    return result;
  }
}
