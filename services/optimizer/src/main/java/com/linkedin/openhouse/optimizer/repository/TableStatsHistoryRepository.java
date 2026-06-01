package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.db.TableStatsHistoryRow;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Append-only repository for per-commit stats history rows. */
public interface TableStatsHistoryRepository extends JpaRepository<TableStatsHistoryRow, String> {

  /**
   * Return history rows for a table, newest first. {@code since} is optional ({@link
   * Optional#empty()} to skip the time filter). {@code pageable} is required; callers pick the row
   * cap (default limit lives in {@code optimizer.repo.default-limit}).
   */
  default List<TableStatsHistoryRow> find(
      String tableUuid, Optional<Instant> since, Pageable pageable) {
    return findInternal(tableUuid, since.orElse(null), pageable);
  }

  // ---- Internals. Use the Optional-typed default method above. ----

  @Query(
      "SELECT r FROM TableStatsHistoryRow r "
          + "WHERE r.tableUuid = :tableUuid "
          + "AND (:since IS NULL OR r.recordedAt >= :since) "
          + "ORDER BY r.recordedAt DESC")
  List<TableStatsHistoryRow> findInternal(
      @Param("tableUuid") String tableUuid, @Param("since") Instant since, Pageable pageable);
}
