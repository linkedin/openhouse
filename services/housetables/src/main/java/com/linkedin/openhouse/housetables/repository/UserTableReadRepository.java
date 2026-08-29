package com.linkedin.openhouse.housetables.repository;

import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.services.model.UserViewQuery;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

/**
 * The persistence boundary the neutral and view read paths cross, and the one read the shared typed
 * write primitive performs before it saves.
 *
 * <p>It sits immediately after Spring's {@code PersistenceExceptionTranslationInterceptor}, which
 * is the only place the exact converter wrappers can be caught once and replaced with this module's
 * unchecked failures before the service sees them. Every read-facing method returns fully mapped
 * DTOs: no JPA entity, {@code Iterable} or {@code Stream} escapes, so a hydration failure can never
 * arrive as a partial result.
 *
 * <p>Existing table read paths deliberately do not cross this boundary; refactoring them is out of
 * scope for this change.
 */
public interface UserTableReadRepository {

  /**
   * The occupancy read: whatever holds the key, of either type.
   *
   * @return empty only for genuine absence
   * @throws com.linkedin.openhouse.housetables.exception.CorruptUserTableDataException the selected
   *     row's stored discriminator is corrupt
   * @throws com.linkedin.openhouse.housetables.exception.UserTableReadException any other
   *     repository or dependency failure
   */
  Optional<UserTableDto> findEntity(String databaseId, String tableId);

  /**
   * View-scoped point read; a table or a legacy null at the key resolves as absent.
   *
   * @throws com.linkedin.openhouse.housetables.exception.CorruptUserTableDataException the selected
   *     row's stored discriminator is corrupt
   * @throws com.linkedin.openhouse.housetables.exception.UserTableReadException any other
   *     repository or dependency failure
   */
  Optional<UserTableDto> findView(String databaseId, String tableId);

  /**
   * The complete result of an unpaged view query. The whole {@code Iterable} is consumed and mapped
   * inside the translation boundary, so the caller gets either every row or one failure.
   *
   * @throws com.linkedin.openhouse.housetables.exception.CorruptUserTableDataException any selected
   *     row's stored discriminator is corrupt
   * @throws com.linkedin.openhouse.housetables.exception.UserTableReadException any other
   *     repository or dependency failure
   */
  List<UserTableDto> findViews(UserViewQuery query);

  /**
   * One page of a view query, filtered before it is paged. Every content element is mapped inside
   * the translation boundary, so a partial page can never be returned.
   *
   * @throws com.linkedin.openhouse.housetables.exception.CorruptUserTableDataException any selected
   *     row's stored discriminator is corrupt
   * @throws com.linkedin.openhouse.housetables.exception.UserTableReadException any other
   *     repository or dependency failure
   */
  Page<UserTableDto> findViews(UserViewQuery query, Pageable pageable);

  /**
   * The one row that crosses this boundary un-mapped: the write primitive needs the fully hydrated
   * entity for {@code UserTableVersionMapper}. It is already materialized, so no deferred ORM
   * operation escapes with it.
   *
   * @throws com.linkedin.openhouse.housetables.exception.CorruptUserTableDataException the occupant
   *     row's stored discriminator is corrupt, which must never read as "the key is free"
   * @throws com.linkedin.openhouse.housetables.exception.UserTableReadException any other
   *     repository or dependency failure
   */
  Optional<UserTableRow> findRowForWrite(String databaseId, String tableId);
}
