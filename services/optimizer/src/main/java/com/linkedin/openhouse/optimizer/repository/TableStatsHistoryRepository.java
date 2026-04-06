package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.entity.TableStatsHistoryRow;
import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Append-only repository for per-commit stats history rows. */
public interface TableStatsHistoryRepository extends JpaRepository<TableStatsHistoryRow, Long> {

  /**
   * Return history rows for a table, newest first. Pass {@code null} for {@code since} to skip the
   * time filter.
   *
   * @param tableUuid the stable table UUID
   * @param since inclusive lower bound on recorded_at; {@code null} to skip
   * @param pageable use {@code PageRequest.of(0, limit)} to cap results
   */
  @Query(
      "SELECT r FROM TableStatsHistoryRow r "
          + "WHERE r.tableUuid = :tableUuid "
          + "AND (:since IS NULL OR r.recordedAt >= :since) "
          + "ORDER BY r.recordedAt DESC")
  List<TableStatsHistoryRow> find(
      @Param("tableUuid") String tableUuid, @Param("since") Instant since, Pageable pageable);
}
