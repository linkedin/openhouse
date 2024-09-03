package com.linkedin.openhouse.internal.catalog.toggle.repository;

import com.linkedin.openhouse.housetables.client.api.ToggleStatusApi;
import com.linkedin.openhouse.housetables.client.model.EntityResponseBodyToggleStatus;
import com.linkedin.openhouse.internal.catalog.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.internal.catalog.toggle.model.ToggleStatusKey;
import com.linkedin.openhouse.internal.catalog.toggle.model.ToggleStatusMapper;
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
    return false;
  }

  @Override
  public Iterable<TableToggleStatus> findAll() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public Iterable<TableToggleStatus> findAllById(Iterable<ToggleStatusKey> ruleKeys) {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public long count() {
    return 0;
  }

  @Override
  public void deleteById(ToggleStatusKey toggleStatusKey) {}

  @Override
  public void delete(TableToggleStatus entity) {}

  @Override
  public void deleteAllById(Iterable<? extends ToggleStatusKey> ruleKeys) {}

  @Override
  public void deleteAll(Iterable<? extends TableToggleStatus> entities) {}

  @Override
  public void deleteAll() {}
}
