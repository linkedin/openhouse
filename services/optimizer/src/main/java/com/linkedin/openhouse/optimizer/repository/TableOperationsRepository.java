package com.linkedin.openhouse.optimizer.repository;

import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Spring Data JPA repository for {@code table_operations} rows in the optimizer DB. */
public interface TableOperationsRepository extends JpaRepository<TableOperationsRow, String> {

  /**
   * Find operation rows matching the given filters. Every filter is optional ({@link
   * Optional#empty()} to skip). {@code pageable} is required; callers pick the row cap (default
   * limit lives in {@code optimizer.repo.default-limit}).
   */
  default List<TableOperationsRow> find(
      Optional<OperationType> operationType,
      Optional<OperationStatus> status,
      Optional<String> tableUuid,
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<Instant> scheduledAt,
      Optional<List<String>> ids,
      Pageable pageable) {
    // List parameters can't share an :ids IS NULL pattern with the IN clause —
    // Hibernate expands the list inline and the IS NULL check turns ungrammatical.
    // Two internal queries; dispatch by presence.
    if (ids.isPresent()) {
      return findInternalWithIds(
          operationType.orElse(null),
          status.orElse(null),
          tableUuid.orElse(null),
          databaseName.orElse(null),
          tableName.orElse(null),
          scheduledAt.orElse(null),
          ids.get(),
          pageable);
    }
    return findInternal(
        operationType.orElse(null),
        status.orElse(null),
        tableUuid.orElse(null),
        databaseName.orElse(null),
        tableName.orElse(null),
        scheduledAt.orElse(null),
        pageable);
  }

  /**
   * Batch CAS: transition rows from {@code fromStatus} to {@code toStatus} for every id in {@code
   * ids} that is still in {@code fromStatus}. Rows in a different status are skipped silently.
   * Returns the number of rows transitioned.
   *
   * <p>Side-effect columns use COALESCE — {@link Optional#empty()} means "leave unchanged". The
   * underlying transitions are:
   *
   * <ul>
   *   <li>PENDING → SCHEDULING: pass {@code scheduledAt = Optional.of(claimedAt)}; the watermark
   *       lets {@link #find} resolve the precise set of rows this caller claimed.
   *   <li>SCHEDULING → SCHEDULED: pass {@code jobId = Optional.of(...)}.
   *   <li>SCHEDULING → PENDING: pass both empty; {@code scheduledAt} stays at the prior claim's
   *       watermark (overwritten on the next claim) and {@code jobId} stays null.
   * </ul>
   */
  default int updateBatch(
      List<String> ids,
      OperationStatus fromStatus,
      OperationStatus toStatus,
      Optional<Instant> scheduledAt,
      Optional<String> jobId) {
    return updateBatchInternal(
        ids, fromStatus, toStatus, scheduledAt.orElse(null), jobId.orElse(null));
  }

  /**
   * Delete the specified rows, but only if they are still {@code PENDING}. The status gate is
   * defensive — never drop a row another instance has claimed. Returns the number of rows actually
   * removed.
   */
  @Modifying(flushAutomatically = true, clearAutomatically = true)
  @Query(
      "DELETE FROM TableOperationsRow r "
          + "WHERE r.id IN :ids "
          + "AND r.status = com.linkedin.openhouse.optimizer.db.OperationStatus.PENDING")
  int cancel(@Param("ids") List<String> ids);

  // ---- Internals. Use the Optional-typed default methods above. ----

  @Query(
      "SELECT r FROM TableOperationsRow r "
          + "WHERE (:operationType IS NULL OR r.operationType = :operationType) "
          + "AND (:status IS NULL OR r.status = :status) "
          + "AND (:tableUuid IS NULL OR r.tableUuid = :tableUuid) "
          + "AND (:databaseName IS NULL OR r.databaseName = :databaseName) "
          + "AND (:tableName IS NULL OR r.tableName = :tableName) "
          + "AND (:scheduledAt IS NULL OR r.scheduledAt = :scheduledAt)")
  List<TableOperationsRow> findInternal(
      @Param("operationType") OperationType operationType,
      @Param("status") OperationStatus status,
      @Param("tableUuid") String tableUuid,
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName,
      @Param("scheduledAt") Instant scheduledAt,
      Pageable pageable);

  @Query(
      "SELECT r FROM TableOperationsRow r "
          + "WHERE (:operationType IS NULL OR r.operationType = :operationType) "
          + "AND (:status IS NULL OR r.status = :status) "
          + "AND (:tableUuid IS NULL OR r.tableUuid = :tableUuid) "
          + "AND (:databaseName IS NULL OR r.databaseName = :databaseName) "
          + "AND (:tableName IS NULL OR r.tableName = :tableName) "
          + "AND (:scheduledAt IS NULL OR r.scheduledAt = :scheduledAt) "
          + "AND r.id IN :ids")
  List<TableOperationsRow> findInternalWithIds(
      @Param("operationType") OperationType operationType,
      @Param("status") OperationStatus status,
      @Param("tableUuid") String tableUuid,
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName,
      @Param("scheduledAt") Instant scheduledAt,
      @Param("ids") List<String> ids,
      Pageable pageable);

  @Modifying(flushAutomatically = true, clearAutomatically = true)
  @Query(
      "UPDATE TableOperationsRow r "
          + "SET r.status = :toStatus, "
          + "    r.scheduledAt = COALESCE(:scheduledAt, r.scheduledAt), "
          + "    r.jobId = COALESCE(:jobId, r.jobId) "
          + "WHERE r.id IN :ids "
          + "AND r.status = :fromStatus")
  int updateBatchInternal(
      @Param("ids") List<String> ids,
      @Param("fromStatus") OperationStatus fromStatus,
      @Param("toStatus") OperationStatus toStatus,
      @Param("scheduledAt") Instant scheduledAt,
      @Param("jobId") String jobId);
}
