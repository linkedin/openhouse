package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.exception.CorruptEntityTypeCauseFinder;
import com.linkedin.openhouse.housetables.exception.CorruptUserTableDataException;
import com.linkedin.openhouse.housetables.exception.UserTableReadException;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.repository.UserTableReadRepository;
import com.linkedin.openhouse.housetables.services.model.UserViewQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

/**
 * Compositional adapter over the frozen {@link UserTableHtsJdbcRepository}: it delegates, consumes
 * the result completely, maps to DTOs, and translates Spring's persistence wrappers into this
 * module's unchecked failures, preserving the original as cause.
 *
 * <p>Consuming completely is the point: a corrupt row discovered after the boundary would surface
 * as a partial success.
 */
@Component
public class JpaUserTableReadRepository implements UserTableReadRepository {

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
  public Optional<UserTableDto> findView(String databaseId, String tableId) {
    return translating(
        () ->
            htsJdbcRepository
                .findViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(databaseId, tableId)
                .map(userTablesMapper::toUserTableDto));
  }

  @Override
  public List<UserTableDto> findViews(UserViewQuery query) {
    return translating(
        () -> {
          Iterable<UserTableRow> rows =
              query.getTableIdPattern().isPresent()
                  ? htsJdbcRepository.findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(
                      query.getDatabaseId().orElse(null), query.getTableIdPattern().get())
                  : htsJdbcRepository.findAllViewsByFilters(
                      query.getDatabaseId().orElse(null), null, null, null, null, null);
          List<UserTableDto> views = new ArrayList<>();
          for (UserTableRow row : rows) {
            views.add(userTablesMapper.toUserTableDto(row));
          }
          return views;
        });
  }

  @Override
  public Page<UserTableDto> findViews(UserViewQuery query, Pageable pageable) {
    return translating(
        () -> {
          Page<UserTableRow> rows =
              query.getTableIdPattern().isPresent()
                  ? htsJdbcRepository.findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(
                      query.getDatabaseId().orElse(null), query.getTableIdPattern().get(), pageable)
                  : htsJdbcRepository.findAllViewsByFilters(
                      query.getDatabaseId().orElse(null), null, null, null, null, null, pageable);
          // Mapped here rather than deferred, so any failure happens inside the boundary.
          List<UserTableDto> content = new ArrayList<>();
          for (UserTableRow row : rows.getContent()) {
            content.add(userTablesMapper.toUserTableDto(row));
          }
          return new PageImpl<>(content, rows.getPageable(), rows.getTotalElements());
        });
  }

  @Override
  public Optional<UserTableRow> findRowForWrite(String databaseId, String tableId) {
    return translating(
        () ->
            htsJdbcRepository.findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(databaseId, tableId));
  }

  /**
   * Runs immediately after Spring's {@code PersistenceExceptionTranslationInterceptor}, which is
   * the only place the wrappers it produces can be caught once and replaced before the service sees
   * them. The original is always preserved as cause.
   */
  private <T> T translating(Callable<T> read) {
    try {
      return read.call();
    } catch (DataAccessException dataAccessException) {
      throw CorruptEntityTypeCauseFinder.find(dataAccessException)
          .<UserTableReadException>map(
              corruption ->
                  new CorruptUserTableDataException(corruption.getMessage(), dataAccessException))
          .orElseGet(
              () ->
                  new UserTableReadException(
                      "Reading user table rows failed", dataAccessException));
    } catch (Exception e) {
      throw CorruptEntityTypeCauseFinder.find(e)
          .<UserTableReadException>map(
              corruption -> new CorruptUserTableDataException(corruption.getMessage(), e))
          .orElseGet(() -> new UserTableReadException("Reading user table rows failed", e));
    }
  }
}
