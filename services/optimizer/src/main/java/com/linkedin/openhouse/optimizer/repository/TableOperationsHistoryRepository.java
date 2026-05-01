package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.api.model.OperationHistoryStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
import com.linkedin.openhouse.optimizer.entity.TableOperationsHistoryRow;
import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * Repository for {@link TableOperationsHistoryRow}. Append-only; PK is the UUID set by the caller
 * (same UUID as the originating {@code table_operations.id}).
 */
@Repository
public interface TableOperationsHistoryRepository
    extends JpaRepository<TableOperationsHistoryRow, String> {

  /**
   * Return history rows matching the given filters, ordered by {@code completedAt} descending.
   * Every parameter is optional — pass {@code null} to skip that filter.
   */
  @Query(
      "SELECT r FROM TableOperationsHistoryRow r "
          + "WHERE (:databaseName IS NULL OR r.databaseName = :databaseName) "
          + "AND (:tableName IS NULL OR r.tableName = :tableName) "
          + "AND (:tableUuid IS NULL OR r.tableUuid = :tableUuid) "
          + "AND (:operationType IS NULL OR r.operationType = :operationType) "
          + "AND (:status IS NULL OR r.status = :status) "
          + "AND (:since IS NULL OR r.completedAt >= :since) "
          + "AND (:until IS NULL OR r.completedAt <= :until) "
          + "ORDER BY r.completedAt DESC")
  List<TableOperationsHistoryRow> find(
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName,
      @Param("tableUuid") String tableUuid,
      @Param("operationType") OperationType operationType,
      @Param("status") OperationHistoryStatus status,
      @Param("since") Instant since,
      @Param("until") Instant until,
      Pageable pageable);
}
