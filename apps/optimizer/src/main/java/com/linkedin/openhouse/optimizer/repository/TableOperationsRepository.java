package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.entity.TableOperationRow;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Spring Data JPA repository for {@code table_operations} rows in the optimizer DB. */
public interface TableOperationsRepository extends JpaRepository<TableOperationRow, String> {

  /**
   * Returns rows for the given operation type whose status is in {@code statuses}. Used by the
   * Scheduler to load all PENDING rows in one query.
   */
  @Query(
      "SELECT r FROM TableOperationRow r WHERE r.operationType = :type"
          + " AND r.status IN :statuses")
  List<TableOperationRow> findByTypeAndStatuses(
      @Param("type") String operationType, @Param("statuses") Collection<String> statuses);

  /**
   * Returns all rows for the given operation type regardless of status. Used by the Analyzer to
   * find the most recent row per table_uuid for scheduling decisions.
   */
  @Query("SELECT r FROM TableOperationRow r WHERE r.operationType = :type")
  List<TableOperationRow> findByType(@Param("type") String operationType);

  /**
   * Cancel older duplicate PENDING rows for the same (table_uuid, operation_type), keeping only the
   * row identified by {@code keepId}. Called by the Scheduler before claiming to prevent duplicate
   * job submissions from concurrent Analyzer runs.
   *
   * @return the number of rows marked CANCELED
   */
  @Modifying
  @Query(
      "UPDATE TableOperationRow r SET r.status = 'CANCELED' "
          + "WHERE r.tableUuid = :tableUuid AND r.operationType = :opType "
          + "AND r.status = 'PENDING' AND r.id != :keepId")
  int cancelDuplicatePending(
      @Param("tableUuid") String tableUuid,
      @Param("opType") String operationType,
      @Param("keepId") String keepId);

  /**
   * Atomically claim a PENDING row by flipping its status to SCHEDULING.
   *
   * <p>The {@code version} guard prevents double-scheduling when multiple scheduler instances run
   * concurrently. Returns 1 if the claim succeeded, 0 if the row was already claimed by another
   * instance.
   */
  @Modifying(flushAutomatically = true, clearAutomatically = true)
  @Query(
      "UPDATE TableOperationRow r SET r.status = 'SCHEDULING', r.scheduledAt = :now,"
          + " r.version = r.version + 1 WHERE r.id = :id AND r.version = :version")
  int markScheduling(
      @Param("id") String id, @Param("version") Long version, @Param("now") Instant now);

  /**
   * Transition a SCHEDULING row to SCHEDULED after the Jobs Service returns a job ID.
   *
   * @return 1 if updated, 0 if not found or wrong version/status
   */
  @Modifying(flushAutomatically = true, clearAutomatically = true)
  @Query(
      "UPDATE TableOperationRow r SET r.status = 'SCHEDULED', r.jobId = :jobId,"
          + " r.version = r.version + 1"
          + " WHERE r.id = :id AND r.version = :version AND r.status = 'SCHEDULING'")
  int markScheduled(
      @Param("id") String id, @Param("version") Long version, @Param("jobId") String jobId);
}
