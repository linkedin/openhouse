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
   * Return history rows for a table, newest first.
   *
   * @param tableUuid the stable table UUID
   * @param pageable use {@code PageRequest.of(0, limit)} to cap results
   */
  @Query(
      "SELECT r FROM TableStatsHistoryRow r "
          + "WHERE r.tableUuid = :tableUuid "
          + "ORDER BY r.recordedAt DESC")
  List<TableStatsHistoryRow> findByTableUuid(
      @Param("tableUuid") String tableUuid, Pageable pageable);

  /**
   * Return history rows for a table recorded at or after {@code since}, newest first.
   *
   * @param tableUuid the stable table UUID
   * @param since inclusive lower bound on recorded_at
   * @param pageable use {@code PageRequest.of(0, limit)} to cap results
   */
  @Query(
      "SELECT r FROM TableStatsHistoryRow r "
          + "WHERE r.tableUuid = :tableUuid "
          + "AND r.recordedAt >= :since "
          + "ORDER BY r.recordedAt DESC")
  List<TableStatsHistoryRow> findByTableUuidSince(
      @Param("tableUuid") String tableUuid, @Param("since") Instant since, Pageable pageable);
}
