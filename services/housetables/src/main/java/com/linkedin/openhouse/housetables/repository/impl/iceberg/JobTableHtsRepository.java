package com.linkedin.openhouse.housetables.repository.impl.iceberg;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.NoSuchEntityException;
import com.linkedin.openhouse.housetables.dto.mapper.JobMapper;
import com.linkedin.openhouse.housetables.model.JobRow;
import com.linkedin.openhouse.housetables.model.JobRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import com.linkedin.openhouse.hts.catalog.model.jobtable.JobIcebergRow;
import com.linkedin.openhouse.hts.catalog.model.jobtable.JobIcebergRowPrimaryKey;
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
public class JobTableHtsRepository implements HtsRepository<JobRow, JobRowPrimaryKey> {

  @Autowired JobMapper jobMapper;

  @Autowired(required = false)
  IcebergHtsRepository<JobIcebergRow, JobIcebergRowPrimaryKey> icebergHtsRepository;

  @Override
  public JobRow save(JobRow entity) {
    try {
      return jobMapper.toJobRow(icebergHtsRepository.save(jobMapper.toJobIcebergRow(entity)));
    } catch (CommitFailedException commitFailedException) {
      throw new EntityConcurrentModificationException(
          jobMapper.toJobRowPrimaryKey(entity).toString(), commitFailedException);
    }
  }

  @Override
  public Optional<JobRow> findById(JobRowPrimaryKey jobRowPrimaryKey) {
    return icebergHtsRepository
        .findById(jobMapper.toJobIcebergRowPrimaryKey(jobRowPrimaryKey))
        .map(jobMapper::toJobRow);
  }

  @Override
  public boolean existsById(JobRowPrimaryKey jobRowPrimaryKey) {
    return findById(jobRowPrimaryKey).isPresent();
  }

  @Override
  public Iterable<JobRow> findAll() {
    return Lists.newArrayList(
            icebergHtsRepository
                .searchByPartialId(JobIcebergRowPrimaryKey.builder().build())
                .iterator())
        .stream()
        .map(jobMapper::toJobRow)
        .collect(Collectors.toList());
  }

  @Override
  public void deleteById(JobRowPrimaryKey jobRowPrimaryKey) {
    try {
      icebergHtsRepository.deleteById(jobMapper.toJobIcebergRowPrimaryKey(jobRowPrimaryKey));
    } catch (NotFoundException notFoundException) {
      throw new NoSuchEntityException("job", jobRowPrimaryKey.toString(), notFoundException);
    }
  }

  @Override
  public void delete(JobRow entity) {
    deleteById(jobMapper.toJobRowPrimaryKey(entity));
  }

  @Override
  public void deleteAll() {
    icebergHtsRepository.deleteAll();
  }

  /* IMPLEMENT AS NEEDED */

  @Override
  public void deleteAllById(Iterable<? extends JobRowPrimaryKey> jobRowPrimaryKeys) {
    throw getUnsupportedException();
  }

  @Override
  public void deleteAll(Iterable<? extends JobRow> entities) {
    throw getUnsupportedException();
  }

  @Override
  public Iterable<JobRow> findAllById(Iterable<JobRowPrimaryKey> jobRowPrimaryKeys) {
    throw getUnsupportedException();
  }

  @Override
  public long count() {
    throw getUnsupportedException();
  }

  @Override
  public <S extends JobRow> Iterable<S> saveAll(Iterable<S> entities) {
    throw getUnsupportedException();
  }

  private UnsupportedOperationException getUnsupportedException() {
    return new UnsupportedOperationException("Not supported yet");
  }

  @Override
  public Iterable<JobRow> findAll(Sort sort) {
    throw getUnsupportedException();
  }

  @Override
  public Page<JobRow> findAll(Pageable pageable) {
    throw getUnsupportedException();
  }
}
