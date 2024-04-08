package com.linkedin.openhouse.tables.toggle.repository;

import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.model.ToggleStatusKey;
import java.util.Optional;

/**
 * THIS IS A TEMPORARY PLACEHOLDER, without this the whole springboot application is failed to start
 * given missing injection entity for {@link ToggleStatusesRepository}.
 */
public class BaseToggleStatusesRepository implements ToggleStatusesRepository {
  @Override
  public <S extends TableToggleStatus> S save(S entity) {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public <S extends TableToggleStatus> Iterable<S> saveAll(Iterable<S> entities) {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public Optional<TableToggleStatus> findById(ToggleStatusKey toggleStatusKey) {
    return Optional.empty();
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
