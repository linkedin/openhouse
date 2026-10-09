package com.linkedin.openhouse.tables.toggle.repository;

import com.linkedin.openhouse.common.exception.DependencyUnavailableException;
import com.linkedin.openhouse.housetables.client.api.ToggleStatusApi;
import com.linkedin.openhouse.housetables.client.model.EntityResponseBodyToggleStatus;
import com.linkedin.openhouse.tables.toggle.ToggleStatusMapper;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.model.ToggleStatusKey;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

/**
 * A base implementation for {@link ToggleStatusesRepository} that represents an interface for fetch
 * feature-toggle-status of a table entity.
 */
@Repository
@Slf4j
public class ToggleStatusesRepositoryImpl implements ToggleStatusesRepository {
  @Autowired private ToggleStatusApi apiInstance;

  @Autowired private ToggleStatusMapper toggleStatusMapper;

  /**
   * HouseTables answers every lookup, defaulting to inactive, so a failed call is never a toggle
   * state. When HouseTables is unreachable or fails (5xx), report the outage. A 4xx means this call
   * is wrong, which is a bug, so it propagates as itself.
   *
   * @throws DependencyUnavailableException if HouseTables is unreachable or fails
   */
  @Override
  public Optional<TableToggleStatus> findById(ToggleStatusKey toggleStatusKey) {
    try {
      return apiInstance
          .getTableToggleStatus(
              toggleStatusKey.getDatabaseId(),
              toggleStatusKey.getTableId(),
              toggleStatusKey.getFeatureId())
          .map(EntityResponseBodyToggleStatus::getEntity)
          .map(s -> toggleStatusMapper.toTableToggleStatus(toggleStatusKey, s))
          .blockOptional();
    } catch (WebClientRequestException e) {
      throw unavailable(toggleStatusKey, e);
    } catch (WebClientResponseException e) {
      if (e.getRawStatusCode() >= 500) {
        throw unavailable(toggleStatusKey, e);
      }
      throw e;
    }
  }

  private static DependencyUnavailableException unavailable(
      ToggleStatusKey toggleStatusKey, Exception cause) {
    return new DependencyUnavailableException(
        String.format(
            "HouseTables could not report feature %s for table %s.%s. Retry.",
            toggleStatusKey.getFeatureId(),
            toggleStatusKey.getDatabaseId(),
            toggleStatusKey.getTableId()),
        cause);
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
