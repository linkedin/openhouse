package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.api.model.OperationStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
import com.linkedin.openhouse.optimizer.entity.TableOperationsRow;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/** Repository for {@link TableOperationsRow}. PK is the client-generated UUID {@code id}. */
@Repository
public interface TableOperationsRepository extends JpaRepository<TableOperationsRow, String> {

  /**
   * Return operations matching the given filters. Every parameter is optional — pass {@code null}
   * to skip that filter. No filters returns all rows.
   */
  @Query(
      "SELECT r FROM TableOperationsRow r "
          + "WHERE (:operationType IS NULL OR r.operationType = :operationType) "
          + "AND (:status IS NULL OR r.status = :status) "
          + "AND (:databaseName IS NULL OR r.databaseName = :databaseName) "
          + "AND (:tableName IS NULL OR r.tableName = :tableName) "
          + "AND (:tableUuid IS NULL OR r.tableUuid = :tableUuid)")
  List<TableOperationsRow> findFiltered(
      @Param("operationType") OperationType operationType,
      @Param("status") OperationStatus status,
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName,
      @Param("tableUuid") String tableUuid);
}
