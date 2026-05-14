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
   * Delete duplicate PENDING rows for the same (tableUuid, operationType), keeping only the
   * specified row. Used by the Scheduler to deduplicate before claiming.
   */
  @Modifying
  @Query(
      "DELETE FROM TableOperationsRow r "
          + "WHERE r.tableUuid = :tableUuid "
          + "AND r.operationType = :operationType "
          + "AND r.status = 'PENDING' "
          + "AND r.id <> :keepId")
  int cancelDuplicatePending(
      @Param("tableUuid") String tableUuid,
      @Param("operationType") String operationType,
      @Param("keepId") String keepId);

  /**
   * CAS transition: PENDING → SCHEDULING. Returns 1 if the row was claimed, 0 if already claimed by
   * another instance or the version has changed.
   */
  @Modifying
  @Query(
      "UPDATE TableOperationsRow r "
          + "SET r.status = 'SCHEDULING', r.scheduledAt = :scheduledAt, r.version = r.version + 1 "
          + "WHERE r.id = :id AND r.version = :version AND r.status = 'PENDING'")
  int markScheduling(
      @Param("id") String id,
      @Param("version") long version,
      @Param("scheduledAt") Instant scheduledAt);

  /**
   * CAS transition: SCHEDULING → SCHEDULED with the external job ID. Returns 1 on success, 0 if the
   * row is no longer in SCHEDULING state at the expected version.
   */
  @Modifying
  @Query(
      "UPDATE TableOperationsRow r "
          + "SET r.status = 'SCHEDULED', r.jobId = :jobId, r.version = r.version + 1 "
          + "WHERE r.id = :id AND r.version = :version AND r.status = 'SCHEDULING'")
  int markScheduled(
      @Param("id") String id, @Param("version") long version, @Param("jobId") String jobId);

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
