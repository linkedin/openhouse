package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.common.exception.CorruptEntityTypeException;
import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.repository.UserTableReadRepository;
import com.linkedin.openhouse.housetables.services.model.UserViewQuery;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

/**
 * Compositional adapter over the frozen {@link UserTableHtsJdbcRepository}: it delegates, maps to
 * DTOs, and unwraps the converter's corruption from the wrapper Spring's persistence exception
 * translation put it in. That unwrapping is why this class exists.
 *
 * <p>Mapping runs inside the translation rather than above it. With the frozen finders that is
 * defensive rather than load-bearing, because they all hydrate eagerly, so a corrupt row throws
 * during the query itself. It becomes load-bearing if a {@code Stream}-returning finder is ever
 * added.
 */
@Component
public class JpaUserTableReadRepository implements UserTableReadRepository {

  /** Bounds the cause walk, so a cyclic chain terminates instead of spinning. */
  private static final int CAUSE_CHAIN_MAX_DEPTH = 20;

  @Autowired UserTableHtsJdbcRepository htsJdbcRepository;

  @Autowired UserTablesMapper userTablesMapper;

  @Override
  public Optional<UserTableDto> findEntity(String databaseId, String tableId) {
    return translating(
        () ->
            htsJdbcRepository
                .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(databaseId, tableId)
                .map(userTablesMapper::toUserTableDto));
  }

  @Override
  public Optional<UserTableDto> findTable(String databaseId, String tableId) {
    return translating(
        () ->
            htsJdbcRepository
                .findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(databaseId, tableId)
                .map(userTablesMapper::toUserTableDto));
  }

  @Override
  public Optional<UserTableDto> findView(String databaseId, String tableId) {
    return translating(
        () ->
            htsJdbcRepository
                .findViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(databaseId, tableId)
                .map(userTablesMapper::toUserTableDto));
  }

  @Override
  public List<UserTableDto> findTablesByFilters(
      String databaseId,
      String tableId,
      String tableVersion,
      String metadataLocation,
      String storageType,
      Long creationTime) {
    return translating(
        () ->
            materialize(
                htsJdbcRepository.findAllTablesByFilters(
                    databaseId,
                    tableId,
                    tableVersion,
                    metadataLocation,
                    storageType,
                    creationTime)));
  }

  @Override
  public Page<UserTableDto> findTablesByFilters(
      String databaseId,
      String tableId,
      String tableVersion,
      String metadataLocation,
      String storageType,
      Long creationTime,
      Pageable pageable) {
    return translating(
        () ->
            materialize(
                htsJdbcRepository.findAllTablesByFilters(
                    databaseId,
                    tableId,
                    tableVersion,
                    metadataLocation,
                    storageType,
                    creationTime,
                    pageable)));
  }

  @Override
  public List<UserTableDto> findTablesByTableIdPattern(String databaseId, String tableIdPattern) {
    return translating(
        () ->
            materialize(
                htsJdbcRepository.findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
                    databaseId, tableIdPattern)));
  }

  @Override
  public Page<UserTableDto> findTablesByTableIdPattern(
      String databaseId, String tableIdPattern, Pageable pageable) {
    return translating(
        () ->
            materialize(
                htsJdbcRepository.findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
                    databaseId, tableIdPattern, pageable)));
  }

  @Override
  public List<String> findAllDistinctDatabaseIds() {
    return translating(
        () -> {
          List<String> databaseIds = new ArrayList<>();
          for (String databaseId : htsJdbcRepository.findAllDistinctDatabaseIds()) {
            databaseIds.add(databaseId);
          }
          return databaseIds;
        });
  }

  @Override
  public Page<String> findAllDistinctDatabaseIds(String databaseId, Pageable pageable) {
    return translating(
        () -> {
          Page<String> databaseIds =
              htsJdbcRepository.findAllDistinctDatabaseIds(databaseId, pageable);
          return new PageImpl<>(
              new ArrayList<>(databaseIds.getContent()),
              databaseIds.getPageable(),
              databaseIds.getTotalElements());
        });
  }

  @Override
  public List<UserTableDto> findViews(UserViewQuery query) {
    return translating(
        () ->
            materialize(
                query.getTableIdPattern().isPresent()
                    ? htsJdbcRepository.findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(
                        query.getDatabaseId().orElse(null), query.getTableIdPattern().get())
                    : htsJdbcRepository.findAllViewsByFilters(
                        query.getDatabaseId().orElse(null), null, null, null, null, null)));
  }

  @Override
  public Page<UserTableDto> findViews(UserViewQuery query, Pageable pageable) {
    return translating(
        () ->
            materialize(
                query.getTableIdPattern().isPresent()
                    ? htsJdbcRepository.findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(
                        query.getDatabaseId().orElse(null),
                        query.getTableIdPattern().get(),
                        pageable)
                    : htsJdbcRepository.findAllViewsByFilters(
                        query.getDatabaseId().orElse(null),
                        null,
                        null,
                        null,
                        null,
                        null,
                        pageable)));
  }

  @Override
  public Optional<UserTableRow> findTableRow(String databaseId, String tableId) {
    return translating(
        () ->
            htsJdbcRepository.findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
                databaseId, tableId));
  }

  @Override
  public Optional<UserTableRow> findRowForWrite(String databaseId, String tableId) {
    return translating(
        () ->
            htsJdbcRepository.findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(databaseId, tableId));
  }

  private List<UserTableDto> materialize(Iterable<UserTableRow> rows) {
    List<UserTableDto> mapped = new ArrayList<>();
    for (UserTableRow row : rows) {
      mapped.add(userTablesMapper.toUserTableDto(row));
    }
    return mapped;
  }

  private Page<UserTableDto> materialize(Page<UserTableRow> rows) {
    return new PageImpl<>(
        materialize(rows.getContent()), rows.getPageable(), rows.getTotalElements());
  }

  /**
   * Runs immediately after {@code PersistenceExceptionTranslationInterceptor}. Corruption is
   * rethrown unwrapped so the advice can render its diagnostic; everything else is rethrown exactly
   * as it arrived.
   */
  private <T> T translating(Callable<T> read) {
    try {
      return read.call();
    } catch (DataAccessException dataAccessException) {
      throw findCorruptEntityTypeCause(dataAccessException)
          .map(corruption -> (RuntimeException) corruption)
          .orElse(dataAccessException);
    } catch (RuntimeException runtimeException) {
      throw runtimeException;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /** Bounded by depth and by visited identity, so a cyclic cause chain terminates. */
  private static Optional<CorruptEntityTypeException> findCorruptEntityTypeCause(
      Throwable exception) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Throwable current = exception;
    for (int depth = 0; current != null && depth < CAUSE_CHAIN_MAX_DEPTH; depth++) {
      if (!visited.add(current)) {
        break;
      }
      if (current instanceof CorruptEntityTypeException) {
        return Optional.of((CorruptEntityTypeException) current);
      }
      current = current.getCause();
    }
    return Optional.empty();
  }
}
