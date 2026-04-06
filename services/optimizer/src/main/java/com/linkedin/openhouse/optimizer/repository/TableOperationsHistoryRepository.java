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
 * Repository for {@link TableOperationsHistoryRow}. Append-only; PK is auto-increment {@code id}.
 */
@Repository
public interface TableOperationsHistoryRepository
    extends JpaRepository<TableOperationsHistoryRow, Long> {

  /**
   * Return the most recent history rows for a table UUID, newest first, up to {@code limit} rows.
   *
   * @param tableUuid the stable table UUID
   * @param limit maximum number of rows to return
   * @return history rows ordered by {@code submitted_at} descending
   */
  @Query(
      value =
          "SELECT * FROM table_operations_history "
              + "WHERE table_uuid = :tableUuid "
              + "ORDER BY submitted_at DESC LIMIT :limit",
      nativeQuery = true)
  List<TableOperationsHistoryRow> find(
      @Param("tableUuid") String tableUuid, @Param("limit") int limit);

  /**
   * Return history rows matching the given filters, ordered by {@code submittedAt} descending.
   * Every parameter is optional — pass {@code null} to skip that filter.
   */
  @Query(
      "SELECT r FROM TableOperationsHistoryRow r "
          + "WHERE (:databaseName IS NULL OR r.databaseName = :databaseName) "
          + "AND (:tableName IS NULL OR r.tableName = :tableName) "
          + "AND (:tableUuid IS NULL OR r.tableUuid = :tableUuid) "
          + "AND (:operationType IS NULL OR r.operationType = :operationType) "
          + "AND (:status IS NULL OR r.status = :status) "
          + "AND (:since IS NULL OR r.submittedAt >= :since) "
          + "AND (:until IS NULL OR r.submittedAt <= :until) "
          + "ORDER BY r.submittedAt DESC")
  List<TableOperationsHistoryRow> findFiltered(
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName,
      @Param("tableUuid") String tableUuid,
      @Param("operationType") OperationType operationType,
      @Param("status") OperationHistoryStatus status,
      @Param("since") Instant since,
      @Param("until") Instant until,
      Pageable pageable);
}
