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
   * Return history rows matching the given filters, ordered by {@code submittedAt} descending.
   * Every parameter is optional — pass {@code null} to skip that filter.
   */
  @Query(
      "SELECT r FROM TableOperationHistoryRow r "
          + "WHERE (:operationType IS NULL OR r.operationType = :operationType) "
          + "AND (:tableUuid IS NULL OR r.tableUuid = :tableUuid) "
          + "AND (:status IS NULL OR r.status = :status) "
          + "AND (:since IS NULL OR r.submittedAt >= :since) "
          + "ORDER BY r.submittedAt DESC")
  List<TableOperationHistoryRow> find(
      @Param("operationType") String operationType,
      @Param("tableUuid") String tableUuid,
      @Param("status") String status,
      @Param("since") Instant since,
      Pageable pageable);
}
