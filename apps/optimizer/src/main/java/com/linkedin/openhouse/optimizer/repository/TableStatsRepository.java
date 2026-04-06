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
          + "WHERE (:databaseId IS NULL OR r.databaseId = :databaseId) "
          + "AND (:tableName IS NULL OR r.tableName = :tableName) "
          + "AND (:tableUuid IS NULL OR r.tableUuid = :tableUuid)")
  List<TableStatsRow> find(
      @Param("databaseId") String databaseId,
      @Param("tableName") String tableName,
      @Param("tableUuid") String tableUuid);
}
