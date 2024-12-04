package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.util.Pair;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class UserTablesServiceImpl implements UserTablesService {

  @Autowired UserTableHtsJdbcRepository htsJdbcRepository;

  @Autowired UserTablesMapper userTablesMapper;

  @Override
  public UserTableDto getUserTable(String databaseId, String tableId) {
    UserTableRow userTableRow;

    try {
      userTableRow =
          htsJdbcRepository
              .findById(
                  UserTableRowPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build())
              .orElseThrow(NoSuchElementException::new);
    } catch (NoSuchElementException ne) {
      throw new NoSuchUserTableException(databaseId, tableId, ne);
    }

    return userTablesMapper.toUserTableDto(userTableRow);
  }

  @Override
  public List<UserTableDto> getAllUserTables(UserTable userTable) {
    if (isListDatabases(userTable)) {
      // list databases
      return StreamSupport.stream(
              htsJdbcRepository.findAllDistinctDatabaseIds().spliterator(), false)
          .map(databaseId -> UserTableDto.builder().databaseId(databaseId).build())
          .collect(Collectors.toList());
    } else if (isListTables(userTable)) {
      // list tables in a database
      return StreamSupport.stream(
              htsJdbcRepository
                  .findAllByDatabaseIdIgnoreCase(userTable.getDatabaseId())
                  .spliterator(),
              false)
          .map(userTableRow -> userTablesMapper.toUserTableDto(userTableRow))
          .collect(Collectors.toList());
    } else if (isListTablesWithPattern(userTable)) {
      // list tables with tableId or with pattern in a database
      return StreamSupport.stream(
              htsJdbcRepository
                  .findAllByDatabaseIdAndTableIdLikeAllIgnoreCase(
                      userTable.getDatabaseId(), userTable.getTableId())
                  .spliterator(),
              false)
          .map(userTableRow -> userTablesMapper.toUserTableDto(userTableRow))
          .collect(Collectors.toList());
    } else {
      // general search
      log.warn(
          "Reaching general search for user table which is not expected: {}", userTable.toJson());
      UserTableDto targetUserTableDto = userTablesMapper.fromUserTable(userTable);
      return StreamSupport.stream(htsJdbcRepository.findAll().spliterator(), false)
          .map(userTableRow -> userTablesMapper.toUserTableDto(userTableRow))
          .filter(x -> x.match(targetUserTableDto))
          .collect(Collectors.toList());
    }
  }

  @Override
  public Pair<UserTableDto, Boolean> putUserTable(UserTable userTable) {
    Optional<UserTableRow> existingUserTableRow =
        htsJdbcRepository.findById(
            UserTableRowPrimaryKey.builder()
                .databaseId(userTable.getDatabaseId())
                .tableId(userTable.getTableId())
                .build());

    UserTableRow targetUserTableRow =
        userTablesMapper.toUserTableRow(userTable, existingUserTableRow);
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

  @Override
  public void deleteUserTable(String databaseId, String tableId) {
    if (!htsJdbcRepository.existsById(
        UserTableRowPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build())) {
      throw new NoSuchUserTableException(databaseId, tableId);
    }

    htsJdbcRepository.deleteById(
        UserTableRowPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build());
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
