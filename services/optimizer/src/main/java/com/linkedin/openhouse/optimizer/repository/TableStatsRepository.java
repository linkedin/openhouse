package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import java.time.Instant;
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
   * The analyzer's incremental scan, as a single join: every {@code table_stats} row written at or
   * after {@code watermark} (backed by {@code idx_ts_updated_at}), left-joined to its current
   * operation of {@code operationType} and to its latest completed-history entry of that type.
   *
   * <p>Returns one tuple per changed table (per matching active op): {@code [0]=TableStatsRow,
   * [1]=TableOperationsRow (nullable), [2]=latest completed_at (nullable), [3]=latest status
   * (nullable)}. Map with {@code
   * com.linkedin.openhouse.optimizer.model.ChangedTableDto#fromJoinRow}.
   *
   * <p>The current op is an entity join on {@code table_uuid + operation_type}. The latest history
   * uses correlated scalar subqueries (Hibernate cannot express a "latest row" join in JPQL); the
   * status subquery relies on {@code completed_at} being unique per {@code (table_uuid,
   * operation_type)} — a tie would make it return multiple rows (the same caveat as {@code
   * findLatest}).
   */
  @Query(
      "SELECT ts, op, "
          + "(SELECT MAX(h.completedAt) FROM TableOperationsHistoryRow h "
          + "   WHERE h.tableUuid = ts.tableUuid AND h.operationType = :operationType), "
          + "(SELECT h2.status FROM TableOperationsHistoryRow h2 "
          + "   WHERE h2.tableUuid = ts.tableUuid AND h2.operationType = :operationType "
          + "     AND h2.completedAt = (SELECT MAX(h3.completedAt) FROM TableOperationsHistoryRow h3 "
          + "        WHERE h3.tableUuid = ts.tableUuid AND h3.operationType = :operationType)) "
          + "FROM TableStatsRow ts "
          + "LEFT JOIN TableOperationsRow op "
          + "  ON op.tableUuid = ts.tableUuid AND op.operationType = :operationType "
          + "WHERE ts.updatedAt >= :watermark")
  List<Object[]> findChangedWithOpAndLatestHistory(
      @Param("operationType") OperationType operationType,
      @Param("watermark") Instant watermark,
      Pageable pageable);

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
