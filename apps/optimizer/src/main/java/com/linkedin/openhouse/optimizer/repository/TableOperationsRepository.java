package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.entity.TableOperationRow;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Spring Data JPA repository for {@code table_operations} rows in the optimizer DB. */
public interface TableOperationsRepository extends JpaRepository<TableOperationRow, String> {

  /**
   * Return operations matching the given filters. Every parameter is optional — pass {@code null}
   * to skip that filter.
   */
  @Query(
      "SELECT r FROM TableOperationRow r "
          + "WHERE (:operationType IS NULL OR r.operationType = :operationType) "
          + "AND (:status IS NULL OR r.status = :status) "
          + "AND (:tableUuid IS NULL OR r.tableUuid = :tableUuid) "
          + "AND (:databaseName IS NULL OR r.databaseName = :databaseName) "
          + "AND (:tableName IS NULL OR r.tableName = :tableName)")
  List<TableOperationRow> find(
      @Param("operationType") String operationType,
      @Param("status") String status,
      @Param("tableUuid") String tableUuid,
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName);
}
