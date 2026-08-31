package com.linkedin.openhouse.housetables.repository;

import com.linkedin.openhouse.common.exception.CorruptEntityTypeException;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.services.model.UserViewQuery;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

/**
 * The persistence boundary every House Tables read crosses, table and view alike.
 *
 * <p>It sits immediately after Spring's {@code PersistenceExceptionTranslationInterceptor}, the
 * only place the converter's {@link CorruptEntityTypeException} can be recovered from the wrapper
 * that carries it. Corruption is rethrown unwrapped; every other failure is rethrown exactly as
 * Spring produced it, so non-corrupt infrastructure failures behave as they always have.
 *
 * <p>Results are materialized here rather than handed back lazily. With the frozen finders that is
 * belt-and-braces rather than a live guard: they return {@code Optional}, {@code Iterable} and
 * {@code Page}, all of which Hibernate hydrates during the query, so corruption already surfaces
 * inside the translation below. It would start mattering the moment a {@code Stream}-returning
 * finder is added, which is why {@link #findViews} and friends never hand one out.
 */
public interface UserTableReadRepository {

  /**
   * Reads whatever holds the key, of either type.
   *
   * @return empty only for genuine absence
   */
  Optional<UserTableDto> findEntity(String databaseId, String tableId);

  /** A view or a corrupt row at the key resolves as absent. */
  Optional<UserTableDto> findTable(String databaseId, String tableId);

  /** A table or a legacy null at the key resolves as absent. */
  Optional<UserTableDto> findView(String databaseId, String tableId);

  List<UserTableDto> findTablesByFilters(
      String databaseId,
      String tableId,
      String tableVersion,
      String metadataLocation,
      String storageType,
      Long creationTime);

  Page<UserTableDto> findTablesByFilters(
      String databaseId,
      String tableId,
      String tableVersion,
      String metadataLocation,
      String storageType,
      Long creationTime,
      Pageable pageable);

  List<UserTableDto> findTablesByTableIdPattern(String databaseId, String tableIdPattern);

  Page<UserTableDto> findTablesByTableIdPattern(
      String databaseId, String tableIdPattern, Pageable pageable);

  List<String> findAllDistinctDatabaseIds();

  Page<String> findAllDistinctDatabaseIds(String databaseId, Pageable pageable);

  List<UserTableDto> findViews(UserViewQuery query);

  Page<UserTableDto> findViews(UserViewQuery query, Pageable pageable);

  /**
   * The table row itself, which the soft-delete archive needs to copy into its own store. A corrupt
   * row resolves as absent, exactly as {@link #findTable} does.
   */
  Optional<UserTableRow> findTableRow(String databaseId, String tableId);

  /**
   * The occupant row, which the write primitive needs hydrated for {@code UserTableVersionMapper}.
   * A corrupt occupant must fail rather than read as "the key is free".
   */
  Optional<UserTableRow> findRowForWrite(String databaseId, String tableId);
}
