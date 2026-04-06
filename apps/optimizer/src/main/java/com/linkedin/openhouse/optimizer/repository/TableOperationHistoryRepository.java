package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.entity.TableOperationHistoryRow;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Repository for reading {@code table_operations_history} in the Analyzer. */
public interface TableOperationHistoryRepository
    extends JpaRepository<TableOperationHistoryRow, String> {

  /**
   * Returns all history rows for an operation type, newest first. Loaded once per analysis run and
   * grouped in memory by {@code tableUuid} to eliminate per-table N+1 queries in the circuit
   * breaker check.
   */
  @Query(
      "SELECT r FROM TableOperationHistoryRow r "
          + "WHERE r.operationType = :opType "
          + "ORDER BY r.submittedAt DESC")
  List<TableOperationHistoryRow> findAllByOperationType(@Param("opType") String operationType);
}
