package com.linkedin.openhouse.housetables.repository.impl.iceberg;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import com.linkedin.openhouse.hts.catalog.model.usertable.UserTableIcebergRow;
import com.linkedin.openhouse.hts.catalog.model.usertable.UserTableIcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.repository.IcebergHtsRepository;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

@Deprecated
@Component
public class UserTableHtsRepository implements HtsRepository<UserTableRow, UserTableRowPrimaryKey> {

  @Autowired UserTablesMapper userTablesMapper;

  @Autowired(required = false)
  IcebergHtsRepository<UserTableIcebergRow, UserTableIcebergRowPrimaryKey> icebergHtsRepository;

  @Override
  public UserTableRow save(UserTableRow entity) {
    try {
      return userTablesMapper.toUserTableRow(
          icebergHtsRepository.save(userTablesMapper.toUserTableIcebergRow(entity)));
    } catch (CommitFailedException commitFailedException) {
      throw new EntityConcurrentModificationException(
          userTablesMapper.fromUserTableRowToRowKey(entity).toString(), commitFailedException);
    }
  }

  @Override
  public Optional<UserTableRow> findById(UserTableRowPrimaryKey userTableRowPrimaryKey) {
    return icebergHtsRepository
        .findById(userTablesMapper.toUserTableIcebergRowPrimaryKey(userTableRowPrimaryKey))
        .map(userTablesMapper::toUserTableRow);
  }

  @Override
  public boolean existsById(UserTableRowPrimaryKey userTableRowPrimaryKey) {
    return findById(userTableRowPrimaryKey).isPresent();
  }

  @Override
  public void deleteById(UserTableRowPrimaryKey userTableRowPrimaryKey) {
    try {
      icebergHtsRepository.deleteById(
          userTablesMapper.toUserTableIcebergRowPrimaryKey(userTableRowPrimaryKey));
    } catch (NotFoundException notFoundException) {
      throw new NoSuchUserTableException(
          userTableRowPrimaryKey.getDatabaseId(),
          userTableRowPrimaryKey.getTableId(),
          notFoundException);
    }
  }

  @Override
  public void delete(UserTableRow entity) {
    try {
      deleteById(userTablesMapper.fromUserTableRowToRowKey(entity));
    } catch (NotFoundException notFoundException) {
      throw new NoSuchUserTableException(
          entity.getDatabaseId(), entity.getTableId(), notFoundException);
    }
  }

  @Override
  public void deleteAll() {
    icebergHtsRepository.deleteAll();
  }

  @Override
  public Iterable<UserTableRow> findAll() {
    return Lists.newArrayList(
            icebergHtsRepository
                .searchByPartialId(UserTableIcebergRowPrimaryKey.builder().build())
                .iterator())
        .stream()
        .map(userTablesMapper::toUserTableRow)
        .collect(Collectors.toList());
  }

  /* IMPLEMENT AS NEEDED */
  @Override
  public <S extends UserTableRow> Iterable<S> saveAll(Iterable<S> entities) {
    throw getUnsupportedException();
  }

  @Override
  public Iterable<UserTableRow> findAllById(
      Iterable<UserTableRowPrimaryKey> userTableRowPrimaryKeys) {
    throw getUnsupportedException();
  }

  @Override
  public long count() {
    throw getUnsupportedException();
  }

  @Override
  public void deleteAllById(Iterable<? extends UserTableRowPrimaryKey> userTableRowPrimaryKeys) {
    throw getUnsupportedException();
  }

  @Override
  public void deleteAll(Iterable<? extends UserTableRow> entities) {
    throw getUnsupportedException();
  }

  private UnsupportedOperationException getUnsupportedException() {
    return new UnsupportedOperationException(
        "Only save, findById, existsById, deleteById supported for UserTableHtsRepositoryImpl");
  }

  public Iterable<UserTableRow> findAll(Sort sort) {
    throw getUnsupportedException();
  }

  @Override
  public Page<UserTableRow> findAll(Pageable pageable) {
    throw getUnsupportedException();
  }
}
