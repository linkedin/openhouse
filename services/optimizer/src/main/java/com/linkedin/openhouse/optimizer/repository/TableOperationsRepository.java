package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.entity.TableOperationsRow;
import java.time.Instant;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Spring Data JPA repository for {@code table_operations} rows in the optimizer DB. */
public interface TableOperationsRepository extends JpaRepository<TableOperationsRow, String> {

  /**
   * Return operations matching the given filters. Every parameter is optional — pass {@code null}
   * to skip that filter.
   */
  @Query(
      "SELECT r FROM TableOperationsRow r "
          + "WHERE (:operationType IS NULL OR r.operationType = :operationType) "
          + "AND (:status IS NULL OR r.status = :status) "
          + "AND (:tableUuid IS NULL OR r.tableUuid = :tableUuid) "
          + "AND (:databaseName IS NULL OR r.databaseName = :databaseName) "
          + "AND (:tableName IS NULL OR r.tableName = :tableName)")
  List<TableOperationsRow> find(
      @Param("operationType") String operationType,
      @Param("status") String status,
      @Param("tableUuid") String tableUuid,
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName);

  /**
   * Batch CAS: PENDING → SCHEDULING for every {@code id} still in PENDING. Returns the number of
   * rows transitioned. Rows already claimed by another instance are skipped silently; callers must
   * re-query if they need the precise list.
   */
  @Modifying
  @Query(
      "UPDATE TableOperationsRow r "
          + "SET r.status = 'SCHEDULING', r.scheduledAt = :scheduledAt, r.version = r.version + 1 "
          + "WHERE r.id IN :ids AND r.status = 'PENDING'")
  int markSchedulingBatch(
      @Param("ids") List<String> ids, @Param("scheduledAt") Instant scheduledAt);

  /**
   * Batch CAS: SCHEDULING → SCHEDULED with the given {@code jobId} for every {@code id} still in
   * SCHEDULING. Returns the number of rows transitioned.
   */
  @Modifying
  @Query(
      "UPDATE TableOperationsRow r "
          + "SET r.status = 'SCHEDULED', r.jobId = :jobId, r.version = r.version + 1 "
          + "WHERE r.id IN :ids AND r.status = 'SCHEDULING'")
  int markScheduledBatch(@Param("ids") List<String> ids, @Param("jobId") String jobId);

  /**
   * Batch transition: SCHEDULING → PENDING for every {@code id} still in SCHEDULING. Used by the
   * scheduler to release claimed rows when job submission fails so the next pass can retry. Returns
   * the number of rows reverted.
   */
  @Modifying
  @Query(
      "UPDATE TableOperationsRow r "
          + "SET r.status = 'PENDING', r.scheduledAt = NULL, r.version = r.version + 1 "
          + "WHERE r.id IN :ids AND r.status = 'SCHEDULING'")
  int markPendingBatch(@Param("ids") List<String> ids);

  /**
   * Batch-delete duplicate PENDING rows for the given operation type, keeping only the IDs in
   * {@code keepIds}. Used by the scheduler to deduplicate before claiming.
   */
  @Modifying
  @Query(
      "DELETE FROM TableOperationsRow r "
          + "WHERE r.operationType = :operationType "
          + "AND r.status = 'PENDING' "
          + "AND r.id NOT IN :keepIds")
  int cancelDuplicatePendingBatch(
      @Param("operationType") String operationType, @Param("keepIds") List<String> keepIds);
}
