package com.linkedin.openhouse.housetables.repository;

import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.exception.CorruptUserTableDataException;
import com.linkedin.openhouse.housetables.exception.UserTableReadException;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.services.model.UserViewQuery;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

/**
 * The persistence boundary the neutral and view read paths cross.
 *
 * <p>It sits immediately after Spring's {@code PersistenceExceptionTranslationInterceptor}, the
 * only place the converter wrappers can be caught once and replaced before the service sees them.
 * Every read returns fully mapped DTOs and is consumed to exhaustion here, so a hydration failure
 * can never arrive as a partial result. Existing table reads deliberately do not cross this
 * boundary.
 *
 * <p>Every method throws {@link CorruptUserTableDataException} when a selected row's stored
 * discriminator is corrupt, and {@link UserTableReadException} for any other repository failure.
 */
public interface UserTableReadRepository {

  /**
   * Reads whatever holds the key, of either type.
   *
   * @return empty only for genuine absence
   */
  Optional<UserTableDto> findEntity(String databaseId, String tableId);

  /** A table or a legacy null at the key resolves as absent. */
  Optional<UserTableDto> findView(String databaseId, String tableId);

  List<UserTableDto> findViews(UserViewQuery query);

  Page<UserTableDto> findViews(UserViewQuery query, Pageable pageable);

  /**
   * The one row that crosses this boundary un-mapped, because the write primitive needs the
   * hydrated entity for {@code UserTableVersionMapper}. A corrupt occupant must fail rather than
   * read as "the key is free".
   */
  Optional<UserTableRow> findRowForWrite(String databaseId, String tableId);
}
