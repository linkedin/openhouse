package com.linkedin.openhouse.housetables.services;

import static com.linkedin.openhouse.common.utils.PageableUtil.createPageable;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.NoSuchSoftDeletedUserTableException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.dto.mapper.SoftDeletedUserTablesMapper;
import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.metrics.UserTableMetricsConstant;
import com.linkedin.openhouse.housetables.model.EntityType;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRow;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.SoftDeletedUserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.model.UserViewQuery;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.util.Pair;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Slf4j
public class UserTablesServiceImpl implements UserTablesService {

  @Autowired UserTableHtsJdbcRepository htsJdbcRepository;

  @Autowired UserTablesMapper userTablesMapper;

  @Autowired SoftDeletedUserTableHtsJdbcRepository softDeletedHtsJdbcRepository;

  @Autowired SoftDeletedUserTablesMapper softDeletedUserTablesMapper;

  private static final MetricsReporter METRICS_REPORTER =
      MetricsReporter.of(MetricsConstant.HOUSETABLES_SERVICE);
  @Autowired private SoftDeletedUserTableHtsJdbcRepository softDeletedUserTableHtsJdbcRepository;

  @Override
  public UserTableDto getUserTable(String databaseId, String tableId) {
    UserTableRow userTableRow;

    try {
      userTableRow =
          htsJdbcRepository
              .findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(databaseId, tableId)
              .orElseThrow(NoSuchElementException::new);
    } catch (NoSuchElementException ne) {
      throw new NoSuchUserTableException(databaseId, tableId, ne);
    }

    return userTablesMapper.toUserTableDto(userTableRow);
  }

  @Override
  public Optional<UserTableDto> getNeutralEntity(String databaseId, String tableId) {
    return htsJdbcRepository
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(databaseId, tableId)
        .map(userTablesMapper::toUserTableDto);
  }

  @Override
  public Optional<UserTableDto> getUserView(String databaseId, String tableId) {
    return htsJdbcRepository
        .findViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(databaseId, tableId)
        .map(userTablesMapper::toUserTableDto);
  }

  @Override
  public Page<UserTableDto> getAllUserViews(
      UserViewQuery query, int page, int size, String sortBy) {
    METRICS_REPORTER.count(UserTableMetricsConstant.HTS_PAGE_VIEWS_REQUEST);
    Pageable pageable = createPageable(page, size, sortBy, "tableId");
    return METRICS_REPORTER.executeWithStats(
        () -> findViewRows(query, pageable).map(userTablesMapper::toUserTableDto),
        UserTableMetricsConstant.HTS_PAGE_VIEWS_TIME);
  }

  @Override
  public Pair<UserTableDto, Boolean> putUserView(UserTable userView) {
    return persistTypedEntity(userView, EntityType.VIEW);
  }

  @Override
  public boolean deleteUserView(String databaseId, String tableId) {
    // Views cannot be soft-deleted by design.
    return htsJdbcRepository.deleteViewById(
            UserTableRowPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build())
        != 0;
  }

  @Override
  public List<UserTableDto> getAllUserTables(UserTable userTable) {
    if (isListDatabases(userTable)) {
      return listDatabases();
    } else if (isListTables(userTable)) {
      return listTables(userTable);
    } else if (isListTablesWithPattern(userTable)) {
      return listTablesWithPattern(userTable);
    } else {
      return searchTables(userTable);
    }
  }

  @Override
  public Page<UserTableDto> getAllUserTables(
      UserTable userTable, int page, int size, String sortBy) {
    if (isListDatabases(userTable)) {
      return listDatabases(page, size, sortBy);
    } else if (isListTables(userTable)) {
      return listTables(userTable, page, size, sortBy);
    } else if (isListTablesWithPattern(userTable)) {
      return listTablesWithPattern(userTable, page, size, sortBy);
    } else {
      return searchTables(userTable, page, size, sortBy);
    }
  }

  @Override
  public Pair<UserTableDto, Boolean> putUserTable(UserTable userTable) {
    return persistTypedEntity(userTable, EntityType.TABLE);
  }

  /**
   * The one write primitive both named entry points share. The type comes from the caller that
   * named it, never from the transport object, so no direct Java caller can violate either method's
   * invariant.
   */
  private Pair<UserTableDto, Boolean> persistTypedEntity(
      UserTable userTable, EntityType entityType) {
    Optional<UserTableRow> existingUserTableRow =
        htsJdbcRepository.findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
            userTable.getDatabaseId(), userTable.getTableId());

    // Before any version mapping: a wrong-type collision is not a stale write, and the conflict
    // names the occupant rather than the requested type.
    if (existingUserTableRow.isPresent()
        && existingUserTableRow.get().getEntityType() != entityType) {
      throw new AlreadyExistsException(
          existingUserTableRow.get().getEntityType().name(),
          userTable.getDatabaseId() + "." + userTable.getTableId());
    }

    // Overwritten before mapping, so no transport spelling reaches the enum boundary.
    UserTable ownedEntity = userTable.toBuilder().entityType(entityType.name()).build();
    UserTableRow targetUserTableRow =
        userTablesMapper.toUserTableRow(ownedEntity, existingUserTableRow);
    UserTableDto returnedDto;

    try {
      returnedDto = userTablesMapper.toUserTableDto(htsJdbcRepository.save(targetUserTableRow));
    } catch (CommitFailedException
        | ObjectOptimisticLockingFailureException
        | DataIntegrityViolationException e) {
      throw new EntityConcurrentModificationException(
          String.format(
              "databaseId : %s, tableId : %s, version: %s %s",
              targetUserTableRow.getDatabaseId(),
              targetUserTableRow.getTableId(),
              targetUserTableRow.getVersion(),
              "The requested user table has been modified/created by other processes."),
          userTablesMapper.fromUserTableToRowKey(userTable).toString(),
          e);
    }

    return Pair.of(returnedDto, existingUserTableRow.isPresent());
  }

  /**
   * Renames a user table within the same database.
   *
   * @param fromDatabaseId The databaseId of the row to rename.
   * @param fromTableId The tableId of the row to rename.
   * @param toDatabaseId Until rename support across databases is supported, this should be the same
   *     as fromDatabaseId
   * @param toTableId The new tableId of the renamed row.
   * @param metadataLocation The new metadata file of the table with updated table properties that
   *     match the new tableId
   */
  @Override
  public void renameUserTable(
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String metadataLocation) {
    // No source precheck: the conditional update is the check, closing the TOCTOU window for the
    // source's type and existence.
    // TODO: it does not close the version window. The statement carries no version predicate, so a
    // writer committing between a caller's read and this rename is overwritten silently.
    try {
      log.info(
          "Renaming user table from {}.{} to {}.{}",
          fromDatabaseId,
          fromTableId,
          toTableId,
          toDatabaseId);
      // Use fromDatabaseId for destination db to preserve the original case of the database
      // TODO: Use toDataBaseId for destination instead of fromDatabaseId once rename across
      // databases is supported
      if (htsJdbcRepository.renameTableId(
              fromDatabaseId, fromTableId, fromDatabaseId, toTableId, metadataLocation)
          == 0) {
        throw new NoSuchUserTableException(fromDatabaseId, fromTableId);
      }
    } catch (DataIntegrityViolationException e) {
      throw new AlreadyExistsException("Table", toTableId);
    }
  }

  @Override
  @Transactional
  public void deleteUserTable(String databaseId, String tableId, boolean isSoftDeleted) {
    UserTableRowPrimaryKey key =
        UserTableRowPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build();
    if (isSoftDeleted) {
      // Table-scoped, never the neutral read: a view copied into that store would restore as one.
      // TODO: version-guard this archive-then-delete. The read below takes no lock and the delete
      // below carries no version predicate, so a writer committing between them makes us archive
      // the stale version and delete the newer one, losing that commit and making restore lossy.
      // The conditional delete closes the type and existence window only. Fix by locking the source
      // row, or by constraining the delete to the captured version and requiring one affected row.
      UserTableRow existingTable =
          htsJdbcRepository
              .findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(databaseId, tableId)
              .orElseThrow(() -> new NoSuchUserTableException(databaseId, tableId));
      softDeletedHtsJdbcRepository.save(
          softDeletedUserTablesMapper.toSoftDeletedUserTableRow(existingTable));
    }
    // Throwing inside the transaction rolls the copy above back.
    if (htsJdbcRepository.deleteTableById(key) == 0) {
      throw new NoSuchUserTableException(databaseId, tableId);
    }
  }

  /**
   * Moves a soft deleted user table back to the user tables repository.
   *
   * @param databaseId
   * @param tableId
   * @param deletedAt
   * @return
   */
  @Override
  @Transactional
  public UserTableDto restoreUserTable(String databaseId, String tableId, Long deletedAt) {
    // TODO: this occupancy check is a read-then-write, so a row created at the key between it and
    // the save below slips past it. The DataIntegrityViolationException catch further down is the
    // backstop rather than the guard; a conditional insert would make the check atomic.
    Optional<UserTableDto> existingUserTable =
        htsJdbcRepository
            .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(databaseId, tableId)
            .map(userTablesMapper::toUserTableDto);
    if (existingUserTable.isPresent()) {
      // If the table already exists, we throw an exception
      throw new AlreadyExistsException("Table", existingUserTable.get().getTableId());
    }
    SoftDeletedUserTableRowPrimaryKey softDeletedTableKey =
        SoftDeletedUserTableRowPrimaryKey.builder()
            .databaseId(databaseId)
            .tableId(tableId)
            .deletedAtMs(deletedAt)
            .build();
    SoftDeletedUserTableRow existingSoftDeletedTable =
        softDeletedHtsJdbcRepository
            .findById(softDeletedTableKey)
            .orElseThrow(
                () -> new NoSuchSoftDeletedUserTableException(databaseId, tableId, deletedAt));

    try {
      softDeletedHtsJdbcRepository.deleteById(softDeletedTableKey);
      return userTablesMapper.toUserTableDto(
          htsJdbcRepository.save(userTablesMapper.toUserTableRow(existingSoftDeletedTable)));
    } catch (DataIntegrityViolationException e) {
      throw new AlreadyExistsException("Table", existingSoftDeletedTable.getTableId());
    }
  }

  /**
   * Deletes all soft deleted user tables for a given databaseId and tableId that have a
   * purgeAfterMs earlier than purgeAfterMs.
   *
   * @param databaseId The database ID of the soft deleted user table.
   * @param tableId The table ID of the soft deleted user table.
   * @param purgeAfterMs The timestamp in milliseconds after which all soft deleted user tables
   *     should be deleted. If null, all soft deleted user tables for the given databaseId and
   *     tableId will be deleted.
   */
  @Override
  public void purgeSoftDeletedUserTables(String databaseId, String tableId, Long purgeAfterMs) {
    if (purgeAfterMs == null) {
      softDeletedHtsJdbcRepository.deleteAllByDatabaseIdTableId(databaseId, tableId);
    } else {
      softDeletedHtsJdbcRepository.deleteByDatabaseIdTableIdPurgeAfterMs(
          databaseId, tableId, purgeAfterMs);
    }
  }

  @Override
  public Page<UserTableDto> getAllSoftDeletedTables(
      UserTable userTable, int page, int size, String sortBy) {
    METRICS_REPORTER.count(MetricsConstant.HTS_PAGE_SEARCH_TABLES_REQUEST);
    Pageable pageable = createPageable(page, size, sortBy, "tableId");
    return METRICS_REPORTER.executeWithStats(
        () ->
            softDeletedHtsJdbcRepository
                .findAllByFilters(
                    userTable.getDatabaseId(),
                    userTable.getTableId(),
                    userTable.getPurgeAfterMs(),
                    pageable)
                .map(
                    softDeletedUserTableRow ->
                        softDeletedUserTablesMapper.toUserTableDto(softDeletedUserTableRow)),
        MetricsConstant.HTS_PAGE_SEARCH_TABLES_TIME);
  }

  private Page<UserTableRow> findViewRows(UserViewQuery query, Pageable pageable) {
    return query.getTableIdPattern().isPresent()
        ? htsJdbcRepository.findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(
            query.getDatabaseId().orElse(null), query.getTableIdPattern().get(), pageable)
        : htsJdbcRepository.findAllViewsByFilters(
            query.getDatabaseId().orElse(null), null, null, null, null, null, pageable);
  }

  private List<UserTableDto> listDatabases() {
    METRICS_REPORTER.count(MetricsConstant.HTS_LIST_DATABASES_REQUEST);
    return METRICS_REPORTER.executeWithStats(
        () ->
            StreamSupport.stream(
                    htsJdbcRepository.findAllDistinctDatabaseIds().spliterator(), false)
                .map(databaseId -> UserTableDto.builder().databaseId(databaseId).build())
                .collect(Collectors.toList()),
        MetricsConstant.HTS_LIST_DATABASES_TIME);
  }

  private Page<UserTableDto> listDatabases(int page, int size, String sortBy) {
    METRICS_REPORTER.count(MetricsConstant.HTS_PAGE_DATABASES_REQUEST);
    Pageable pageable = createPageable(page, size, sortBy, "databaseId");
    return METRICS_REPORTER.executeWithStats(
        () ->
            htsJdbcRepository
                .findAllDistinctDatabaseIds(null, pageable)
                .map(databaseId -> UserTableDto.builder().databaseId(databaseId).build()),
        MetricsConstant.HTS_PAGE_DATABASES_TIME);
  }

  private List<UserTableDto> listTables(UserTable userTable) {
    METRICS_REPORTER.count(MetricsConstant.HTS_LIST_TABLES_REQUEST);
    return METRICS_REPORTER.executeWithStats(
        () ->
            StreamSupport.stream(
                    htsJdbcRepository
                        .findAllTablesByFilters(
                            userTable.getDatabaseId(), null, null, null, null, null)
                        .spliterator(),
                    false)
                .map(userTablesMapper::toUserTableDto)
                .collect(Collectors.toList()),
        MetricsConstant.HTS_LIST_TABLES_TIME);
  }

  private Page<UserTableDto> listTables(UserTable userTable, int page, int size, String sortBy) {
    METRICS_REPORTER.count(MetricsConstant.HTS_PAGE_TABLES_REQUEST);
    Pageable pageable = createPageable(page, size, sortBy, "tableId");
    return METRICS_REPORTER.executeWithStats(
        () ->
            htsJdbcRepository
                .findAllTablesByFilters(
                    userTable.getDatabaseId(), null, null, null, null, null, pageable)
                .map(userTablesMapper::toUserTableDto),
        MetricsConstant.HTS_PAGE_TABLES_TIME);
  }

  private List<UserTableDto> listTablesWithPattern(UserTable userTable) {
    METRICS_REPORTER.count(MetricsConstant.HTS_LIST_TABLES_REQUEST);
    return METRICS_REPORTER.executeWithStats(
        () ->
            StreamSupport.stream(
                    htsJdbcRepository
                        .findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
                            userTable.getDatabaseId(), userTable.getTableId())
                        .spliterator(),
                    false)
                .map(userTablesMapper::toUserTableDto)
                .collect(Collectors.toList()),
        MetricsConstant.HTS_LIST_TABLES_TIME);
  }

  private Page<UserTableDto> listTablesWithPattern(
      UserTable userTable, int page, int size, String sortBy) {
    METRICS_REPORTER.count(MetricsConstant.HTS_PAGE_TABLES_REQUEST);
    Pageable pageable = createPageable(page, size, sortBy, "tableId");
    return METRICS_REPORTER.executeWithStats(
        () ->
            htsJdbcRepository
                .findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
                    userTable.getDatabaseId(), userTable.getTableId(), pageable)
                .map(userTablesMapper::toUserTableDto),
        MetricsConstant.HTS_PAGE_TABLES_TIME);
  }

  private Page<UserTableDto> searchTables(UserTable userTable, int page, int size, String sortBy) {
    METRICS_REPORTER.count(MetricsConstant.HTS_PAGE_SEARCH_TABLES_REQUEST);
    Pageable pageable = createPageable(page, size, sortBy, "tableId");
    log.warn(
        "Reaching general search for user table which is not expected: {}", userTable.toJson());
    return METRICS_REPORTER.executeWithStats(
        () ->
            htsJdbcRepository
                .findAllTablesByFilters(
                    userTable.getDatabaseId(),
                    userTable.getTableId(),
                    userTable.getTableVersion(),
                    userTable.getMetadataLocation(),
                    userTable.getStorageType(),
                    userTable.getCreationTime(),
                    pageable)
                .map(userTablesMapper::toUserTableDto),
        MetricsConstant.HTS_PAGE_SEARCH_TABLES_TIME);
  }

  private List<UserTableDto> searchTables(UserTable userTable) {
    METRICS_REPORTER.count(MetricsConstant.HTS_GENERAL_SEARCH_REQUEST);
    log.warn(
        "Reaching general search for user table which is not expected: {}", userTable.toJson());
    return METRICS_REPORTER.executeWithStats(
        () ->
            StreamSupport.stream(
                    htsJdbcRepository
                        .findAllTablesByFilters(
                            userTable.getDatabaseId(),
                            userTable.getTableId(),
                            userTable.getTableVersion(),
                            userTable.getMetadataLocation(),
                            userTable.getStorageType(),
                            userTable.getCreationTime())
                        .spliterator(),
                    false)
                .map(userTablesMapper::toUserTableDto)
                .collect(Collectors.toList()),
        MetricsConstant.HTS_SEARCH_TABLES_TIME);
  }

  private boolean isListDatabases(UserTable userTable) {
    return isNonKeyFieldsNullForUserTable(userTable)
        && userTable.getDatabaseId() == null
        && userTable.getTableId() == null;
  }

  private boolean isListTables(UserTable userTable) {
    return isNonKeyFieldsNullForUserTable(userTable)
        && userTable.getDatabaseId() != null
        && userTable.getTableId() == null;
  }

  private boolean isListTablesWithPattern(UserTable userTable) {
    return isNonKeyFieldsNullForUserTable(userTable)
        && userTable.getDatabaseId() != null
        && userTable.getTableId() != null;
  }

  private boolean isNonKeyFieldsNullForUserTable(UserTable userTable) {
    return userTable.getTableVersion() == null
        && userTable.getMetadataLocation() == null
        && userTable.getStorageType() == null
        && userTable.getCreationTime() == null;
  }
}
