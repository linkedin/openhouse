package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.util.Pair;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Component;

@Component
public class UserTablesServiceImpl implements UserTablesService {

  @Autowired HtsRepository<UserTableRow, UserTableRowPrimaryKey> htsJdbcRepository;

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
    UserTableDto targetUserTableDto = userTablesMapper.fromUserTable(userTable);

    return StreamSupport.stream(htsJdbcRepository.findAll().spliterator(), false)
        .map(userTableRow -> userTablesMapper.toUserTableDto(userTableRow))
        .filter(x -> x.match(targetUserTableDto))
        .collect(Collectors.toList());
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
}
