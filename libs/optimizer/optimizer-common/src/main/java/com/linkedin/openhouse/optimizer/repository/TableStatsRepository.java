package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Spring Data JPA repository for {@code table_stats} rows in the optimizer DB. */
public interface TableStatsRepository extends JpaRepository<TableStatsRow, String> {

  /**
   * Return stats rows matching the given filters. Every filter is optional ({@link
   * Optional#empty()} to skip). {@code pageable} is required; callers pick the row cap (default
   * limit lives in {@code optimizer.repo.default-limit}).
   */
  default List<TableStatsRow> find(
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid,
      Pageable pageable) {
    return findInternal(
        databaseName.orElse(null), tableName.orElse(null), tableUuid.orElse(null), pageable);
  }

  /**
   * Return the distinct {@code database_name} values present in {@code table_stats}. Used by the
   * Analyzer to enumerate databases when iterating per-db; the result set size is bounded by the
   * number of databases (small even at million-table scale).
   */
  @Query("SELECT DISTINCT r.databaseName FROM TableStatsRow r")
  List<String> findDistinctDatabaseNames();

  // ---- Internals. Use the Optional-typed default methods above. ----

  @Query(
      "SELECT r FROM TableStatsRow r "
          + "WHERE (:databaseName IS NULL OR r.databaseName = :databaseName) "
          + "AND (:tableName IS NULL OR r.tableName = :tableName) "
          + "AND (:tableUuid IS NULL OR r.tableUuid = :tableUuid)")
  List<TableStatsRow> findInternal(
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName,
      @Param("tableUuid") String tableUuid,
      Pageable pageable);
}
