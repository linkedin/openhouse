package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.TableOperationsHistoryRow;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Repository for reading {@code table_operations_history}. */
public interface TableOperationsHistoryRepository
    extends JpaRepository<TableOperationsHistoryRow, String> {

  /**
   * Return history rows for a single {@code tableUuid}, newest first. {@code pageable} is required;
   * callers pick the row cap (default limit lives in {@code optimizer.repo.default-limit}).
   */
  @Query(
      "SELECT r FROM TableOperationsHistoryRow r "
          + "WHERE r.tableUuid = :tableUuid "
          + "ORDER BY r.completedAt DESC")
  List<TableOperationsHistoryRow> find(@Param("tableUuid") String tableUuid, Pageable pageable);

  /**
   * Return the most-recent history row per {@code (table_uuid, operation_type)}, filtered to a
   * single operation type. Used by the analyzer to evaluate cadence without materializing every
   * historical row.
   *
   * <p>The correlated subquery is portable across MySQL and H2 (MySQL mode). Backed by index {@code
   * idx_toph_optype_uuid_completed (operation_type, table_uuid, completed_at)} on {@code
   * table_operations_history}, the subquery becomes an index-only lookup per outer row.
   *
   * <p>Ties on {@code completed_at} for the same {@code (table_uuid, operation_type)} return all
   * tied rows; callers should dedupe in memory.
   */
  @Query(
      "SELECT r FROM TableOperationsHistoryRow r "
          + "WHERE r.operationType = :operationType "
          + "AND r.completedAt = ("
          + "  SELECT MAX(r2.completedAt) FROM TableOperationsHistoryRow r2 "
          + "  WHERE r2.tableUuid = r.tableUuid AND r2.operationType = r.operationType)")
  List<TableOperationsHistoryRow> findLatest(
      @Param("operationType") OperationType operationType, Pageable pageable);
}
