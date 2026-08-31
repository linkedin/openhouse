package com.linkedin.openhouse.housetables.mock.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.exception.CorruptEntityTypeConversionException;
import com.linkedin.openhouse.housetables.exception.CorruptUserTableDataException;
import com.linkedin.openhouse.housetables.exception.UserTablePersistenceException;
import com.linkedin.openhouse.housetables.exception.UserTableReadException;
import com.linkedin.openhouse.housetables.model.EntityType;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.repository.UserTableReadRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.JpaUserTableReadRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.model.UserViewQuery;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.persistence.PersistenceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * The adapter is the one place that replaces Spring's wrappers and guarantees a read is complete
 * before the service sees it.
 *
 * <p>Failures are injected as a wrapper raised while the result is consumed — the shape Hibernate
 * produces when the converter fails on a row. H2 cannot reproduce it, so it is simulated here and
 * the real corrupt-row behaviour is pinned by the integration tests.
 */
public class JpaUserTableReadRepositoryTest {

  private static final String DB = "adapter_db";

  private static final String CORRUPT_MSG =
      "Column user_table_row.entity_type holds unrecognized value ['UNKNOWN']; "
          + "only TABLE, VIEW (in any case) and NULL are valid";

  private UserTableHtsJdbcRepository htsJdbcRepository;

  private JpaUserTableReadRepository readRepository;

  @BeforeEach
  public void setup() {
    htsJdbcRepository = Mockito.mock(UserTableHtsJdbcRepository.class);
    UserTablesMapper userTablesMapper = Mockito.mock(UserTablesMapper.class);
    Mockito.when(userTablesMapper.toUserTableDto(any(UserTableRow.class)))
        .thenAnswer(
            invocation -> {
              UserTableRow row = invocation.getArgument(0);
              return UserTableDto.builder()
                  .databaseId(row.getDatabaseId())
                  .tableId(row.getTableId())
                  .metadataLocation(row.getMetadataLocation())
                  .tableVersion(row.getMetadataLocation())
                  .entityType(row.getEntityType())
                  .build();
            });

    readRepository = new JpaUserTableReadRepository();
    ReflectionTestUtils.setField(readRepository, "htsJdbcRepository", htsJdbcRepository);
    ReflectionTestUtils.setField(readRepository, "userTablesMapper", userTablesMapper);
  }

  private static UserTableRow row(String tableId, EntityType entityType) {
    return UserTableRow.builder()
        .databaseId(DB)
        .tableId(tableId)
        .version(0L)
        .metadataLocation(String.format("/openhouse/%s/%s/v0_metadata.json", DB, tableId))
        .entityType(entityType)
        .build();
  }

  private static CorruptEntityTypeConversionException corruption() {
    return new CorruptEntityTypeConversionException(
        CORRUPT_MSG, new IllegalArgumentException("UNKNOWN"));
  }

  private static JpaSystemException corruptWrapper() {
    return new JpaSystemException(
        new PersistenceException("Error attempting to apply AttributeConverter", corruption()));
  }

  /** How a row-by-row hydration failure presents itself to a caller walking the result. */
  private static Iterable<UserTableRow> failingAt(
      List<UserTableRow> rows, int failAtIndex, RuntimeException failure) {
    return () -> failingIterator(rows.iterator(), failAtIndex, failure);
  }

  private static Iterator<UserTableRow> failingIterator(
      Iterator<UserTableRow> delegate, int failAtIndex, RuntimeException failure) {
    return new Iterator<UserTableRow>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return delegate.hasNext();
      }

      @Override
      public UserTableRow next() {
        if (index++ == failAtIndex) {
          throw failure;
        }
        return delegate.next();
      }
    };
  }

  // -------------------------------------------------------------------------------------------
  // the boundary returns DTOs, never JPA rows or lazy results
  // -------------------------------------------------------------------------------------------

  /**
   * A {@code Stream} or {@code Iterable} of entities could still fail after the boundary. Only
   * {@code findRowForWrite} may hand back a row, and it is already hydrated.
   */
  @Test
  public void testNoReadFacingMethodExposesAStreamOrAJpaEntity() {
    for (Method method : UserTableReadRepository.class.getDeclaredMethods()) {
      Class<?> returnType = method.getReturnType();
      Assertions.assertNotEquals(
          Stream.class, returnType, method.getName() + " must not return a Stream");
      Assertions.assertNotEquals(
          Iterable.class, returnType, method.getName() + " must not return a raw Iterable");
      if (!"findRowForWrite".equals(method.getName())) {
        Assertions.assertFalse(
            method.getGenericReturnType().getTypeName().contains(UserTableRow.class.getName()),
            method.getName() + " must return DTOs, not persistence rows");
      }
    }
  }

  @Test
  public void testPointReadsMapToDtosAndReportAbsenceAsEmpty() {
    doReturn(Optional.of(row("neutral_view", EntityType.VIEW)))
        .when(htsJdbcRepository)
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(DB, "neutral_view");
    doReturn(Optional.empty())
        .when(htsJdbcRepository)
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(DB, "absent");
    doReturn(Optional.of(row("a_view", EntityType.VIEW)))
        .when(htsJdbcRepository)
        .findViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(DB, "a_view");
    doReturn(Optional.empty())
        .when(htsJdbcRepository)
        .findViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(DB, "absent");

    assertThat(readRepository.findEntity(DB, "neutral_view"))
        .hasValueSatisfying(dto -> assertThat(dto.getEntityType()).isEqualTo(EntityType.VIEW));
    assertThat(readRepository.findEntity(DB, "absent")).isEmpty();
    assertThat(readRepository.findView(DB, "a_view"))
        .hasValueSatisfying(dto -> assertThat(dto.getTableId()).isEqualTo("a_view"));
    assertThat(readRepository.findView(DB, "absent")).isEmpty();
  }

  @Test
  public void testFindRowForWriteReturnsTheHydratedRow() {
    UserTableRow occupant = row("occupied", EntityType.VIEW);
    doReturn(Optional.of(occupant))
        .when(htsJdbcRepository)
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(DB, "occupied");

    assertThat(readRepository.findRowForWrite(DB, "occupied")).containsSame(occupant);
  }

  // -------------------------------------------------------------------------------------------
  // query routing
  // -------------------------------------------------------------------------------------------

  @Test
  public void testUnboundedQueryDelegatesWithNoDatabaseFilter() {
    doReturn(Arrays.asList(row("v1", EntityType.VIEW), row("v2", EntityType.VIEW)))
        .when(htsJdbcRepository)
        .findAllViewsByFilters(null, null, null, null, null, null);

    assertThat(readRepository.findViews(UserViewQuery.all()))
        .extracting(UserTableDto::getTableId)
        .containsExactly("v1", "v2");
  }

  @Test
  public void testDatabaseScopedQueryDelegatesToTheExactFilterFamily() {
    doReturn(Collections.singletonList(row("v1", EntityType.VIEW)))
        .when(htsJdbcRepository)
        .findAllViewsByFilters(DB, null, null, null, null, null);

    assertThat(readRepository.findViews(UserViewQuery.inDatabase(DB)))
        .extracting(UserTableDto::getTableId)
        .containsExactly("v1");
  }

  /** The exact-filter family would turn {@code my_%} into an equality and answer with nothing. */
  @Test
  public void testPatternQueryDelegatesToThePatternFamily() {
    doReturn(Collections.singletonList(row("match_v1", EntityType.VIEW)))
        .when(htsJdbcRepository)
        .findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(DB, "match_%");

    assertThat(readRepository.findViews(UserViewQuery.matchingPattern(DB, "match_%")))
        .extracting(UserTableDto::getTableId)
        .containsExactly("match_v1");
    Mockito.verify(htsJdbcRepository)
        .findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(DB, "match_%");
    Mockito.verify(htsJdbcRepository, Mockito.never())
        .findAllViewsByFilters(anyString(), any(), any(), any(), any(), any());
  }

  @Test
  public void testPagedQueryMapsEveryContentElementAndPreservesPagingMetadata() {
    Pageable pageable = PageRequest.of(0, 2);
    doReturn(
            new PageImpl<>(
                Arrays.asList(row("v1", EntityType.VIEW), row("v2", EntityType.VIEW)), pageable, 3))
        .when(htsJdbcRepository)
        .findAllViewsByFilters(DB, null, null, null, null, null, pageable);

    Page<UserTableDto> page = readRepository.findViews(UserViewQuery.inDatabase(DB), pageable);

    Assertions.assertEquals(3, page.getTotalElements());
    Assertions.assertEquals(2, page.getTotalPages());
    assertThat(page.getContent()).extracting(UserTableDto::getTableId).containsExactly("v1", "v2");
    assertThat(page.getContent())
        .allSatisfy(dto -> assertThat(dto.getEntityType()).isEqualTo(EntityType.VIEW));
  }

  // -------------------------------------------------------------------------------------------
  // translation of the exact wrappers
  // -------------------------------------------------------------------------------------------

  @Test
  public void testCorruptWrapperOnAPointReadBecomesCorruptUserTableData() {
    JpaSystemException wrapper = corruptWrapper();
    doThrow(wrapper)
        .when(htsJdbcRepository)
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(DB, "corrupt");

    assertThatThrownBy(() -> readRepository.findEntity(DB, "corrupt"))
        .isInstanceOf(CorruptUserTableDataException.class)
        .hasCauseReference(wrapper);
  }

  /** A dependency outage must stay distinguishable from bad data, and the reverse. */
  @Test
  public void testUnrelatedExactWrapperBecomesUserTableReadExceptionPreservingItsCause() {
    JpaSystemException wrapper =
        new JpaSystemException(new PersistenceException("connection reset"));
    doThrow(wrapper)
        .when(htsJdbcRepository)
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(DB, "boom");

    assertThatThrownBy(() -> readRepository.findEntity(DB, "boom"))
        .isInstanceOf(UserTableReadException.class)
        .isNotInstanceOf(CorruptUserTableDataException.class)
        .hasCauseReference(wrapper);
  }

  @Test
  public void testInvalidDataAccessApiUsageWrapperIsAlsoTranslated() {
    doThrow(new InvalidDataAccessApiUsageException("converter failed", corruption()))
        .when(htsJdbcRepository)
        .findViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(DB, "corrupt");

    assertThatThrownBy(() -> readRepository.findView(DB, "corrupt"))
        .isInstanceOf(CorruptUserTableDataException.class);
  }

  @Test
  public void testOtherDataAccessExceptionsAreAlsoTranslated() {
    DataAccessResourceFailureException failure =
        new DataAccessResourceFailureException("datasource down");
    doThrow(failure)
        .when(htsJdbcRepository)
        .findAllViewsByFilters(DB, null, null, null, null, null);

    assertThatThrownBy(() -> readRepository.findViews(UserViewQuery.inDatabase(DB)))
        .isInstanceOf(UserTablePersistenceException.class)
        .hasCauseReference(failure);
  }

  // -------------------------------------------------------------------------------------------
  // all-or-nothing consumption
  // -------------------------------------------------------------------------------------------

  /** An adapter returning what it had collected so far would answer 200 with a truncated list. */
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  public void testUnpagedQueryFailsWholeWhenAnyRowIsCorrupt(int corruptPosition) {
    List<UserTableRow> rows =
        Arrays.asList(
            row("v0", EntityType.VIEW), row("v1", EntityType.VIEW), row("v2", EntityType.VIEW));
    doReturn(failingAt(rows, corruptPosition, corruptWrapper()))
        .when(htsJdbcRepository)
        .findAllViewsByFilters(DB, null, null, null, null, null);

    assertThatThrownBy(() -> readRepository.findViews(UserViewQuery.inDatabase(DB)))
        .as("corrupt row at position %s", corruptPosition)
        .isInstanceOf(CorruptUserTableDataException.class);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 2})
  public void testUnpagedPatternQueryFailsWholeWhenAnyRowIsCorrupt(int corruptPosition) {
    List<UserTableRow> rows =
        Arrays.asList(
            row("match_v0", EntityType.VIEW),
            row("match_v1", EntityType.VIEW),
            row("match_v2", EntityType.VIEW));
    doReturn(failingAt(rows, corruptPosition, corruptWrapper()))
        .when(htsJdbcRepository)
        .findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(DB, "match_%");

    assertThatThrownBy(() -> readRepository.findViews(UserViewQuery.matchingPattern(DB, "match_%")))
        .isInstanceOf(CorruptUserTableDataException.class);
  }

  /** The failure is raised while the content is traversed, where {@code Page.map} meets it. */
  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  public void testPagedQueryFailsWholeWhenAnyContentElementIsCorrupt(int corruptPosition) {
    Pageable pageable = PageRequest.of(0, 2);
    doReturn(
            new FailingContentPage(
                Arrays.asList(row("v0", EntityType.VIEW), row("v1", EntityType.VIEW)),
                pageable,
                corruptPosition,
                corruptWrapper()))
        .when(htsJdbcRepository)
        .findAllViewsByFilters(DB, null, null, null, null, null, pageable);

    assertThatThrownBy(() -> readRepository.findViews(UserViewQuery.inDatabase(DB), pageable))
        .as("corrupt content element at position %s", corruptPosition)
        .isInstanceOf(CorruptUserTableDataException.class);
  }

  @Test
  public void testPagedQueryTranslatesAFailureRaisedByTheRepositoryCall() {
    Pageable pageable = PageRequest.of(0, 2);
    doThrow(corruptWrapper())
        .when(htsJdbcRepository)
        .findAllViewsByFilters(eq(DB), any(), any(), any(), any(), any(), eq(pageable));

    assertThatThrownBy(() -> readRepository.findViews(UserViewQuery.inDatabase(DB), pageable))
        .isInstanceOf(CorruptUserTableDataException.class);
  }

  /**
   * Real paging metadata over content that throws when traversed: {@link PageImpl} stores the safe
   * copy, and every traversal path is overridden onto the failing view of it.
   */
  private static class FailingContentPage extends PageImpl<UserTableRow> {

    private final transient List<UserTableRow> safeContent;
    private final transient int failAtIndex;
    private final transient RuntimeException failure;

    FailingContentPage(
        List<UserTableRow> content, Pageable pageable, int failAtIndex, RuntimeException failure) {
      super(new ArrayList<>(content), pageable, content.size());
      this.safeContent = new ArrayList<>(content);
      this.failAtIndex = failAtIndex;
      this.failure = failure;
    }

    @Override
    public Iterator<UserTableRow> iterator() {
      return failingIterator(safeContent.iterator(), failAtIndex, failure);
    }

    @Override
    public List<UserTableRow> getContent() {
      return new ArrayList<UserTableRow>(safeContent) {
        @Override
        public Iterator<UserTableRow> iterator() {
          return failingIterator(safeContent.iterator(), failAtIndex, failure);
        }
      };
    }
  }
}
