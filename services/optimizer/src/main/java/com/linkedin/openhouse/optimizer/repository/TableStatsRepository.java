package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.entity.TableStatsRow;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Spring Data JPA repository for {@code table_stats} rows in the optimizer DB. */
public interface TableStatsRepository extends JpaRepository<TableStatsRow, String> {

  /**
   * Return stats rows matching the given filters. Every parameter is optional — pass {@code null}
   * to skip that filter.
   */
  @Query(
      "SELECT r FROM TableStatsRow r "
          + "WHERE (:databaseName IS NULL OR r.databaseName = :databaseName) "
          + "AND (:tableName IS NULL OR r.tableName = :tableName) "
          + "AND (:tableUuid IS NULL OR r.tableUuid = :tableUuid)")
  List<TableStatsRow> find(
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName,
      @Param("tableUuid") String tableUuid);

  /**
   * Return the distinct {@code database_name} values present in {@code table_stats}. Used by the
   * Analyzer to enumerate databases when iterating per-db; the result set size is bounded by the
   * number of databases (small even at million-table scale).
   */
  @Query("SELECT DISTINCT r.databaseName FROM TableStatsRow r")
  List<String> findDistinctDatabaseNames();
}
