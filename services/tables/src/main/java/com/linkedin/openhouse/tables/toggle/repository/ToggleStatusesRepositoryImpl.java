package com.linkedin.openhouse.tables.toggle.repository;

import com.linkedin.openhouse.housetables.client.api.ToggleStatusApi;
import com.linkedin.openhouse.housetables.client.model.EntityResponseBodyToggleStatus;
import com.linkedin.openhouse.tables.toggle.ToggleStatusMapper;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.model.ToggleStatusKey;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * A base implementation for {@link ToggleStatusesRepository} that represents an interface for fetch
 * feature-toggle-status of a table entity.
 */
@Repository
@Slf4j
public class ToggleStatusesRepositoryImpl implements ToggleStatusesRepository {
  @Autowired private ToggleStatusApi apiInstance;

  @Autowired private ToggleStatusMapper toggleStatusMapper;

  @Override
  public Optional<TableToggleStatus> findById(ToggleStatusKey toggleStatusKey) {
    return apiInstance
        .getTableToggleStatus(
            toggleStatusKey.getDatabaseId(),
            toggleStatusKey.getTableId(),
            toggleStatusKey.getFeatureId())
        .map(EntityResponseBodyToggleStatus::getEntity)
        .map(s -> toggleStatusMapper.toTableToggleStatus(toggleStatusKey, s))
        .blockOptional();
  }

  @Override
  public <S extends TableToggleStatus> S save(S entity) {
    throw new UnsupportedOperationException(
        "Write Operation into Toggle status API is not supported");
  }

  @Override
  public <S extends TableToggleStatus> Iterable<S> saveAll(Iterable<S> entities) {
    throw new UnsupportedOperationException(
        "Write Operation into Toggle status API is not supported");
  }

  @Override
  public boolean existsById(ToggleStatusKey toggleStatusKey) {
    throw new UnsupportedOperationException(
        "exists-by-id Operation into Toggle status API is not supported");
  }

  @Override
  public Iterable<TableToggleStatus> findAll() {
    throw new UnsupportedOperationException("findAll into Toggle status API is not supported");
  }

  @Override
  public Iterable<TableToggleStatus> findAllById(Iterable<ToggleStatusKey> ruleKeys) {
    throw new UnsupportedOperationException("findAllById into Toggle status API is not supported");
  }

  @Override
  public long count() {
    return 0;
  }

  @Override
  public void deleteById(ToggleStatusKey toggleStatusKey) {
    throw new UnsupportedOperationException("deleteById into Toggle status API is not supported");
  }

  @Override
  public void delete(TableToggleStatus entity) {
    throw new UnsupportedOperationException("delete into Toggle status API is not supported");
  }

  @Override
  public void deleteAllById(Iterable<? extends ToggleStatusKey> ruleKeys) {
    throw new UnsupportedOperationException(
        "deleteAllById into Toggle status API is not supported");
  }

  @Override
  public void deleteAll(Iterable<? extends TableToggleStatus> entities) {
    throw new UnsupportedOperationException("deleteAll into Toggle status API is not supported");
  }

  @Override
  public void deleteAll() {
    throw new UnsupportedOperationException("deleteAll into Toggle status API is not supported");
  }
}
