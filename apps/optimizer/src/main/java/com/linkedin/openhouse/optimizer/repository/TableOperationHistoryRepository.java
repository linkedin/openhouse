package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.entity.TableOperationHistoryRow;
import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Repository for reading {@code table_operations_history} in the Analyzer. */
public interface TableOperationHistoryRepository
    extends JpaRepository<TableOperationHistoryRow, String> {

  /**
   * Return history rows matching the given filters, ordered by {@code completedAt} descending.
   * Every parameter is optional — pass {@code null} to skip that filter.
   */
  @Query(
      "SELECT r FROM TableOperationHistoryRow r "
          + "WHERE (:operationType IS NULL OR r.operationType = :operationType) "
          + "AND (:tableUuid IS NULL OR r.tableUuid = :tableUuid) "
          + "AND (:status IS NULL OR r.status = :status) "
          + "AND (:since IS NULL OR r.completedAt >= :since) "
          + "ORDER BY r.completedAt DESC")
  List<TableOperationHistoryRow> find(
      @Param("operationType") String operationType,
      @Param("tableUuid") String tableUuid,
      @Param("status") String status,
      @Param("since") Instant since,
      Pageable pageable);

  /**
   * Return the most-recent history row per {@code (table_uuid, operation_type)}, filtered to a
   * single operation type. Used by the Analyzer to evaluate cadence without materializing every
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
      "SELECT r FROM TableOperationHistoryRow r "
          + "WHERE r.operationType = :operationType "
          + "AND r.completedAt = ("
          + "  SELECT MAX(r2.completedAt) FROM TableOperationHistoryRow r2 "
          + "  WHERE r2.tableUuid = r.tableUuid AND r2.operationType = r.operationType)")
  List<TableOperationHistoryRow> findLatestPerTable(@Param("operationType") String operationType);
}
