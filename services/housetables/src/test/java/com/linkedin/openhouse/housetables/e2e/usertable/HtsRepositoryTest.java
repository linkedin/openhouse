package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static org.assertj.core.api.Assertions.*;

import com.google.common.collect.Lists;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
public class HtsRepositoryTest {

  /**
   * Canonical interleaved fixture. Four visible tables (two legacy NULL, two explicit TABLE) are
   * interleaved with three VIEW rows so that a fetch-then-filter implementation returns a SHORT
   * first page (1 row) and totalElements=7, while the correct pre-pagination predicate returns a
   * full page (2 rows) and totalElements=4.
   */
  private static final String ENTITY_TYPE_DB = "entity_type_db";

  private static final String[] CANONICAL_TABLE_IDS = {
    "t00_legacy", "t02_explicit", "t04_legacy", "t06_explicit"
  };

  private static final String[] CANONICAL_VIEW_IDS = {"t01_view", "t03_view", "t05_view"};

  /** Case-normalization fixture; see {@code CASE_DB}. */
  private static final String CASE_DB = "entity_type_case_db";

  private static final String[] CASE_VISIBLE_TABLE_IDS = {
    "case00_null", "case01_upper_table", "case02_lower_table", "case03_mixed_table"
  };

  private static final String[] CASE_VIEW_IDS = {
    "case04_upper_view", "case05_lower_view", "case06_mixed_view"
  };

  private static final String CASE_GARBAGE_ID = "case07_garbage";

  @Autowired UserTableHtsJdbcRepository htsRepository;

  @AfterEach
  public void tearDown() {
    htsRepository.deleteAll();
  }

  private UserTableRow row(String databaseId, String tableId, String entityType) {
    return UserTableRow.builder()
        .databaseId(databaseId)
        .tableId(tableId)
        .version(null)
        .metadataLocation(String.format("/openhouse/%s/%s/v0_metadata.json", databaseId, tableId))
        .storageType(TEST_DEFAULT_STORAGE_TYPE)
        .creationTime(TEST_CREATION_TIME)
        .entityType(entityType)
        .build();
  }

  /** Seeds the canonical 7-row interleaved fixture into {@code databaseId} under {@code prefix}. */
  private void seedCanonicalRows(String databaseId, String prefix) {
    htsRepository.save(row(databaseId, prefix + "t00_legacy", null));
    htsRepository.save(row(databaseId, prefix + "t01_view", "VIEW"));
    htsRepository.save(row(databaseId, prefix + "t02_explicit", "TABLE"));
    htsRepository.save(row(databaseId, prefix + "t03_view", "VIEW"));
    htsRepository.save(row(databaseId, prefix + "t04_legacy", null));
    htsRepository.save(row(databaseId, prefix + "t05_view", "VIEW"));
    htsRepository.save(row(databaseId, prefix + "t06_explicit", "TABLE"));
  }

  /** Seeds the 8-row case-normalization fixture into {@link #CASE_DB}. */
  private void seedCaseNormalizationRows() {
    htsRepository.save(row(CASE_DB, "case00_null", null));
    htsRepository.save(row(CASE_DB, "case01_upper_table", "TABLE"));
    htsRepository.save(row(CASE_DB, "case02_lower_table", "table"));
    htsRepository.save(row(CASE_DB, "case03_mixed_table", "TaBlE"));
    htsRepository.save(row(CASE_DB, "case04_upper_view", "VIEW"));
    htsRepository.save(row(CASE_DB, "case05_lower_view", "view"));
    htsRepository.save(row(CASE_DB, "case06_mixed_view", "ViEw"));
    htsRepository.save(row(CASE_DB, CASE_GARBAGE_ID, "UNKNOWN"));
  }

  private static List<String> tableIds(Iterable<UserTableRow> rows) {
    return Lists.newArrayList(rows).stream()
        .map(UserTableRow::getTableId)
        .sorted()
        .collect(Collectors.toList());
  }

  private static List<String> pageTableIds(Page<UserTableRow> page) {
    return page.getContent().stream().map(UserTableRow::getTableId).collect(Collectors.toList());
  }

  private static Pageable sortedPage(int page) {
    return PageRequest.of(page, 2, Sort.by("tableId"));
  }

  @Test
  public void testSaveFirstRecord() {
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    // before insertion
    Assertions.assertEquals(null, testUserTableRow.getVersion());
    // after insertion
    Assertions.assertEquals(0, htsRepository.save(testUserTableRow).getVersion());
  }

  @Test
  public void testFindDistinctDatabases() {
    htsRepository.save(TEST_TUPLE_1_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_1_1.get_userTableRow());
    htsRepository.save(TEST_TUPLE_2_0.get_userTableRow());
    List<String> result = Lists.newArrayList(htsRepository.findAllDistinctDatabaseIds());
    Assertions.assertEquals(Lists.newArrayList("test_db0", "test_db1"), result);
  }

  @Test
  public void testFindAllByDatabaseId() {
    htsRepository.save(TEST_TUPLE_1_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_1_1.get_userTableRow());
    htsRepository.save(TEST_TUPLE_2_0.get_userTableRow());
    List<UserTableRow> result =
        Lists.newArrayList(htsRepository.findAllTablesByDatabaseIdIgnoreCase("test_db0"));
    Assertions.assertEquals(
        Lists.newArrayList("test_table1", "test_table2"),
        result.stream().map(UserTableRow::getTableId).collect(Collectors.toList()));
  }

  @Test
  public void testFindAllByTableIdPattern() {
    htsRepository.save(TEST_TUPLE_1_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_1_1.get_userTableRow());
    htsRepository.save(TEST_TUPLE_2_0.get_userTableRow());
    List<UserTableRow> result =
        Lists.newArrayList(
            htsRepository.findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
                "test_db0", "test_table%"));
    Assertions.assertEquals(
        Lists.newArrayList("test_table1", "test_table2"),
        result.stream().map(UserTableRow::getTableId).collect(Collectors.toList()));
  }

  @Test
  public void testFindAllByTableId() {
    htsRepository.save(TEST_TUPLE_1_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_1_1.get_userTableRow());
    htsRepository.save(TEST_TUPLE_2_0.get_userTableRow());
    List<UserTableRow> result =
        Lists.newArrayList(
            htsRepository.findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
                "test_db0", "test_table1"));
    Assertions.assertEquals(
        Lists.newArrayList("test_table1"),
        result.stream().map(UserTableRow::getTableId).collect(Collectors.toList()));
  }

  @Test
  public void testHouseTable() {
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    htsRepository.save(testUserTableRow);
    UserTableRow actual =
        htsRepository
            .findById(
                UserTableRowPrimaryKey.builder()
                    .databaseId(TEST_DB_ID)
                    .tableId(TEST_TABLE_ID)
                    .build())
            .orElse(UserTableRow.builder().build());

    Assertions.assertEquals(testUserTableRow, actual);
    htsRepository.delete(actual);
  }

  @Test
  public void testDeleteUserTable() {
    htsRepository.save(TEST_TUPLE_1_1.get_userTableRow());
    UserTableRowPrimaryKey key =
        UserTableRowPrimaryKey.builder()
            .tableId(TEST_TUPLE_1_1.getTableId())
            .databaseId(TEST_TUPLE_1_1.getDatabaseId())
            .build();
    // verify testTuple1_1 exist first.
    assertThat(htsRepository.existsById(key)).isTrue();
    // Delete testTuple1_1 from house table.
    htsRepository.deleteById(key);
    // verify testTuple1_1 doesn't exist any more.
    assertThat(htsRepository.existsById(key)).isFalse();
  }

  @Test
  public void testSaveUserTableWithConflict() {
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    Long currentVersion = htsRepository.save(testUserTableRow).getVersion();
    // test create the table again
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> htsRepository.save(testUserTableRow.toBuilder().version(null).build()));
    Assertions.assertTrue(exception instanceof DataIntegrityViolationException);

    // test update at wrong version
    exception =
        Assertions.assertThrows(
            Exception.class,
            () -> htsRepository.save(testUserTableRow.toBuilder().version(100L).build()));
    Assertions.assertTrue(
        exception instanceof ObjectOptimisticLockingFailureException
            | exception instanceof EntityConcurrentModificationException);

    // test update at correct version
    Assertions.assertNotEquals(
        htsRepository
            .save(
                testUserTableRow
                    .toBuilder()
                    .version(currentVersion)
                    .metadataLocation("file:/ml2")
                    .build())
            .getVersion(),
        currentVersion);

    // test update at older version
    exception =
        Assertions.assertThrows(Exception.class, () -> htsRepository.save(testUserTableRow));
    Assertions.assertTrue(
        exception instanceof ObjectOptimisticLockingFailureException
            | exception instanceof EntityConcurrentModificationException);

    htsRepository.deleteById(
        UserTableRowPrimaryKey.builder().databaseId(TEST_DB_ID).tableId(TEST_TABLE_ID).build());
  }

  @Test
  public void testRenameUserTable() {
    htsRepository.save(TEST_TUPLE_1_1.get_userTableRow());
    UserTableRowPrimaryKey key =
        UserTableRowPrimaryKey.builder()
            .tableId(TEST_TUPLE_1_1.getTableId())
            .databaseId(TEST_TUPLE_1_1.getDatabaseId())
            .build();
    // verify testTuple1_1 exist first.
    assertThat(htsRepository.existsById(key)).isTrue();

    String newTableMetadata = TEST_TUPLE_1_1.getTableLoc() + "_v2";
    htsRepository.renameTableId(
        TEST_TUPLE_1_1.getDatabaseId(),
        TEST_TUPLE_1_1.getTableId(),
        TEST_TUPLE_1_1.getDatabaseId(),
        TEST_TUPLE_1_1.getTableId() + "_renamed",
        newTableMetadata);

    UserTableRow result =
        htsRepository
            .findById(
                UserTableRowPrimaryKey.builder()
                    .databaseId(TEST_TUPLE_1_1.getDatabaseId())
                    .tableId(TEST_TUPLE_1_1.getTableId() + "_renamed")
                    .build())
            .orElse(UserTableRow.builder().build());
    assertThat(result.getMetadataLocation()).isEqualTo(newTableMetadata);

    // verify testTuple1_1 doesn't exist any more.
    assertThat(htsRepository.existsById(key)).isFalse();
  }

  @Test
  public void testRenameCaseSensitivity() {
    UserTableRow testUpperCaseRow =
        TEST_TUPLE_1_1
            .get_userTableRow()
            .toBuilder()
            .tableId(TEST_TUPLE_1_1.getTableId().toUpperCase())
            .databaseId(TEST_TUPLE_1_1.getDatabaseId())
            .build();
    htsRepository.save(testUpperCaseRow);

    UserTableRowPrimaryKey key =
        UserTableRowPrimaryKey.builder()
            .tableId(TEST_TUPLE_1_1.getTableId().toUpperCase())
            .databaseId(TEST_TUPLE_1_1.getDatabaseId())
            .build();
    // verify fetch is case in-sensitive
    assertThat(htsRepository.existsById(key)).isTrue();

    String renamedUpperCaseTableId = TEST_TUPLE_1_1.getTableId() + "_RENAMED";

    htsRepository.renameTableId(
        TEST_TUPLE_1_1.getDatabaseId(),
        TEST_TUPLE_1_1.getTableId(),
        TEST_TUPLE_1_1.getDatabaseId().toUpperCase(),
        renamedUpperCaseTableId,
        TEST_TUPLE_1_1.getTableLoc());

    // Try fetching with lower case ID, should still work
    UserTableRow result =
        htsRepository
            .findById(
                UserTableRowPrimaryKey.builder()
                    .databaseId(TEST_TUPLE_1_1.getDatabaseId())
                    .tableId(renamedUpperCaseTableId.toLowerCase())
                    .build())
            .orElse(UserTableRow.builder().build());

    // Should preserve original case
    Assertions.assertEquals(result.getTableId(), renamedUpperCaseTableId);

    // verify testTuple1_1 doesn't exist any more.
    assertThat(htsRepository.existsById(key)).isFalse();
  }

  // ---------------------------------------------------------------------------------------------
  // entityType discriminator
  // ---------------------------------------------------------------------------------------------

  /** The discriminator must persist verbatim and must not perturb version/metadata behavior. */
  @Test
  public void testEntityTypePersistenceRoundTrip() {
    UserTableRow viewRow = htsRepository.save(row(ENTITY_TYPE_DB, "persist_view", "VIEW"));
    UserTableRow tableRow = htsRepository.save(row(ENTITY_TYPE_DB, "persist_table", "TABLE"));
    UserTableRow legacyRow = htsRepository.save(row(ENTITY_TYPE_DB, "persist_legacy", null));

    // Insert still yields version 0 for all three; the discriminator is orthogonal to versioning.
    assertThat(viewRow.getVersion()).isEqualTo(0L);
    assertThat(tableRow.getVersion()).isEqualTo(0L);
    assertThat(legacyRow.getVersion()).isEqualTo(0L);

    assertThat(findRow(ENTITY_TYPE_DB, "persist_view").getEntityType()).isEqualTo("VIEW");
    assertThat(findRow(ENTITY_TYPE_DB, "persist_table").getEntityType()).isEqualTo("TABLE");
    assertThat(findRow(ENTITY_TYPE_DB, "persist_legacy").getEntityType()).isNull();

    assertThat(findRow(ENTITY_TYPE_DB, "persist_view").getMetadataLocation())
        .isEqualTo(viewRow.getMetadataLocation());

    // An update at the correct version preserves the stored discriminator.
    UserTableRow updated =
        htsRepository.save(
            findRow(ENTITY_TYPE_DB, "persist_view")
                .toBuilder()
                .metadataLocation("/openhouse/entity_type_db/persist_view/v1_metadata.json")
                .build());
    assertThat(updated.getEntityType()).isEqualTo("VIEW");
    assertThat(findRow(ENTITY_TYPE_DB, "persist_view").getEntityType()).isEqualTo("VIEW");
  }

  /** SHOW-TABLES-equivalent plain listing hides views and keeps legacy NULL rows. */
  @Test
  public void testFindAllByDatabaseIdFiltersViewsAndKeepsLegacyTables() {
    seedCanonicalRows(ENTITY_TYPE_DB, "");
    // A table in another database must not leak in.
    htsRepository.save(row("other_db", "t00_legacy", null));

    List<UserTableRow> result =
        Lists.newArrayList(htsRepository.findAllTablesByDatabaseIdIgnoreCase(ENTITY_TYPE_DB));

    assertThat(tableIds(result)).containsExactly(CANONICAL_TABLE_IDS);
    assertThat(result)
        .allSatisfy(r -> assertThat(r.getEntityType()).isNotEqualToIgnoringCase("VIEW"));
  }

  /**
   * The canonical anti-post-filter assertion for the per-database page. A fetch-then-filter
   * implementation returns [t00_legacy] on page 0 with totalElements=7/totalPages=4; the correct
   * pre-pagination predicate returns a full 2-row page with totalElements=4/totalPages=2.
   */
  @Test
  public void testFindAllByDatabaseIdFiltersBeforePagination() {
    seedCanonicalRows(ENTITY_TYPE_DB, "");

    Page<UserTableRow> page0 =
        htsRepository.findAllTablesByDatabaseIdIgnoreCase(ENTITY_TYPE_DB, sortedPage(0));
    assertThat(page0.getTotalElements()).isEqualTo(4);
    assertThat(page0.getTotalPages()).isEqualTo(2);
    assertThat(page0.getContent()).hasSize(2);
    assertThat(pageTableIds(page0)).containsExactly("t00_legacy", "t02_explicit");

    Page<UserTableRow> page1 =
        htsRepository.findAllTablesByDatabaseIdIgnoreCase(ENTITY_TYPE_DB, sortedPage(1));
    assertThat(page1.getTotalElements()).isEqualTo(4);
    assertThat(page1.getTotalPages()).isEqualTo(2);
    assertThat(page1.getContent()).hasSize(2);
    assertThat(pageTableIds(page1)).containsExactly("t04_legacy", "t06_explicit");

    assertThat(pageTableIds(page0)).doesNotContainAnyElementsOf(Arrays.asList(CANONICAL_VIEW_IDS));
    assertThat(pageTableIds(page1)).doesNotContainAnyElementsOf(Arrays.asList(CANONICAL_VIEW_IDS));
  }

  /** The pattern (LIKE) listing family applies the same table-only predicate. */
  @Test
  public void testFindAllByPatternFiltersViewsAndKeepsLegacyTables() {
    seedCanonicalRows(ENTITY_TYPE_DB, "match_");
    // Non-matching table in the same database must be excluded by the pattern, not by type.
    htsRepository.save(row(ENTITY_TYPE_DB, "nomatch_table", "TABLE"));

    List<UserTableRow> result =
        Lists.newArrayList(
            htsRepository.findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
                ENTITY_TYPE_DB, "match_%"));

    assertThat(tableIds(result))
        .containsExactly(
            "match_t00_legacy", "match_t02_explicit", "match_t04_legacy", "match_t06_explicit");
  }

  /** Anti-post-filter assertion for the paged pattern listing. */
  @Test
  public void testFindAllByPatternFiltersBeforePagination() {
    seedCanonicalRows(ENTITY_TYPE_DB, "match_");
    htsRepository.save(row(ENTITY_TYPE_DB, "nomatch_table", "TABLE"));

    Page<UserTableRow> page0 =
        htsRepository.findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
            ENTITY_TYPE_DB, "match_%", sortedPage(0));
    assertThat(page0.getTotalElements()).isEqualTo(4);
    assertThat(page0.getTotalPages()).isEqualTo(2);
    assertThat(page0.getContent()).hasSize(2);
    assertThat(pageTableIds(page0)).containsExactly("match_t00_legacy", "match_t02_explicit");

    Page<UserTableRow> page1 =
        htsRepository.findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
            ENTITY_TYPE_DB, "match_%", sortedPage(1));
    assertThat(page1.getTotalElements()).isEqualTo(4);
    assertThat(page1.getTotalPages()).isEqualTo(2);
    assertThat(page1.getContent()).hasSize(2);
    assertThat(pageTableIds(page1)).containsExactly("match_t04_legacy", "match_t06_explicit");
  }

  /**
   * The general-filter query defaults to tables (null and any TABLE spelling) and can be asked
   * explicitly for views. This is the only query family that can return VIEW rows.
   */
  @Test
  public void testFindAllByFiltersDefaultsToTablesAndCanSelectViews() {
    seedCanonicalRows(ENTITY_TYPE_DB, "");

    // entityType == null means "tables", not "everything".
    assertThat(
            tableIds(
                htsRepository.findAllByFilters(
                    ENTITY_TYPE_DB, null, null, null, null, null, (String) null)))
        .containsExactly(CANONICAL_TABLE_IDS);

    for (String tableSpelling : new String[] {"TABLE", "table", "TaBlE"}) {
      assertThat(
              tableIds(
                  htsRepository.findAllByFilters(
                      ENTITY_TYPE_DB, null, null, null, null, null, tableSpelling)))
          .as("entityType=%s must resolve to the four visible tables", tableSpelling)
          .containsExactly(CANONICAL_TABLE_IDS);
    }

    for (String viewSpelling : new String[] {"VIEW", "view", "ViEw"}) {
      assertThat(
              tableIds(
                  htsRepository.findAllByFilters(
                      ENTITY_TYPE_DB, null, null, null, null, null, viewSpelling)))
          .as("entityType=%s must resolve to exactly the three views", viewSpelling)
          .containsExactly(CANONICAL_VIEW_IDS);
    }

    // Pageable overload: default (tables) and explicit VIEW both count in the database.
    Page<UserTableRow> defaultPage0 =
        htsRepository.findAllByFilters(
            ENTITY_TYPE_DB, null, null, null, null, null, (String) null, sortedPage(0));
    assertThat(defaultPage0.getTotalElements()).isEqualTo(4);
    assertThat(defaultPage0.getTotalPages()).isEqualTo(2);
    assertThat(pageTableIds(defaultPage0)).containsExactly("t00_legacy", "t02_explicit");

    Page<UserTableRow> viewPage0 =
        htsRepository.findAllByFilters(
            ENTITY_TYPE_DB, null, null, null, null, null, "VIEW", sortedPage(0));
    assertThat(viewPage0.getTotalElements()).isEqualTo(3);
    assertThat(viewPage0.getTotalPages()).isEqualTo(2);
    assertThat(pageTableIds(viewPage0)).containsExactly("t01_view", "t03_view");

    Page<UserTableRow> viewPage1 =
        htsRepository.findAllByFilters(
            ENTITY_TYPE_DB, null, null, null, null, null, "VIEW", sortedPage(1));
    assertThat(viewPage1.getTotalElements()).isEqualTo(3);
    assertThat(pageTableIds(viewPage1)).containsExactly("t05_view");
  }

  /**
   * Case/garbage matrix at the SQL layer.
   *
   * <p>H2 runs in {@code MODE=MySQL} which is case-SENSITIVE for string comparison, whereas
   * production MySQL's default collation is case-INSENSITIVE. This test therefore proves that the
   * query normalizes explicitly (e.g. {@code upper(u.entityType) = 'TABLE'}) rather than leaning on
   * a provider collation: an implementation using a bare {@code = 'TABLE'} comparison would hide
   * {@code table}/{@code TaBlE} here and fail. It does NOT certify production MySQL behavior — a
   * MySQL staging smoke test is still required before views are enabled.
   */
  @Test
  public void testEntityTypePredicatesAreCaseInsensitiveAndGarbageFailsClosed() {
    seedCaseNormalizationRows();

    assertThat(tableIds(htsRepository.findAllTablesByDatabaseIdIgnoreCase(CASE_DB)))
        .containsExactly(CASE_VISIBLE_TABLE_IDS);
    assertThat(
            tableIds(
                htsRepository.findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
                    CASE_DB, "case%")))
        .containsExactly(CASE_VISIBLE_TABLE_IDS);

    Page<UserTableRow> dbPage0 =
        htsRepository.findAllTablesByDatabaseIdIgnoreCase(CASE_DB, sortedPage(0));
    assertThat(dbPage0.getTotalElements()).isEqualTo(4);
    assertThat(dbPage0.getTotalPages()).isEqualTo(2);
    assertThat(pageTableIds(dbPage0)).containsExactly("case00_null", "case01_upper_table");

    Page<UserTableRow> patternPage0 =
        htsRepository.findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
            CASE_DB, "case%", sortedPage(0));
    assertThat(patternPage0.getTotalElements()).isEqualTo(4);
    assertThat(patternPage0.getTotalPages()).isEqualTo(2);
    assertThat(pageTableIds(patternPage0)).containsExactly("case00_null", "case01_upper_table");

    // Every VIEW spelling is selectable and the garbage row is never one of them.
    for (String viewSpelling : new String[] {"VIEW", "view", "ViEw"}) {
      assertThat(
              tableIds(
                  htsRepository.findAllByFilters(
                      CASE_DB, null, null, null, null, null, viewSpelling)))
          .as("entityType=%s", viewSpelling)
          .containsExactly(CASE_VIEW_IDS);
    }

    // Garbage fails closed on the repository: it is neither a table nor a view.
    assertThat(
            Lists.newArrayList(
                htsRepository.findAllByFilters(CASE_DB, null, null, null, null, null, "UNKNOWN")))
        .isEmpty();
    assertThat(tableIds(htsRepository.findAllTablesByDatabaseIdIgnoreCase(CASE_DB)))
        .doesNotContain(CASE_GARBAGE_ID);

    // The garbage row is still stored — it is hidden, not dropped.
    assertThat(findRow(CASE_DB, CASE_GARBAGE_ID).getEntityType()).isEqualTo("UNKNOWN");
  }

  /**
   * Table-scoped point read: the query, not any caller, is what makes a view unreadable through the
   * table path. NULL and every spelling of TABLE resolve; every spelling of VIEW and an
   * unrecognized value resolve to empty.
   */
  @ParameterizedTest
  @CsvSource(
      nullValues = "NULL",
      value = {
        "case00_null,        true",
        "case01_upper_table, true",
        "case02_lower_table, true",
        "case03_mixed_table, true",
        "case04_upper_view,  false",
        "case05_lower_view,  false",
        "case06_mixed_view,  false",
        "case07_garbage,     false"
      })
  public void testFindTableByKeyResolvesOnlyTableRows(String tableId, boolean expectedVisible) {
    seedCaseNormalizationRows();

    assertThat(
            htsRepository
                .findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(CASE_DB, tableId)
                .isPresent())
        .as("findTableBy... for %s", tableId)
        .isEqualTo(expectedVisible);

    // Case-insensitive on the key itself, exactly like the neutral read.
    assertThat(
            htsRepository
                .findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
                    CASE_DB.toUpperCase(), tableId.toUpperCase())
                .isPresent())
        .isEqualTo(expectedVisible);
  }

  /**
   * The neutral read must keep seeing every row type. It backs {@code findById}/{@code existsById},
   * which the HTS writers use to detect a collision at a key held by another entity type; filtering
   * it would make a view invisible to the very code that must refuse to overwrite it.
   */
  @Test
  public void testNeutralPointReadStillSeesEveryEntityType() {
    seedCaseNormalizationRows();

    for (String tableId :
        new String[] {
          "case00_null",
          "case01_upper_table",
          "case04_upper_view",
          "case06_mixed_view",
          CASE_GARBAGE_ID
        }) {
      assertThat(
              htsRepository
                  .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(CASE_DB, tableId)
                  .isPresent())
          .as("neutral read must still see %s", tableId)
          .isTrue();
      assertThat(htsRepository.existsByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(CASE_DB, tableId))
          .as("neutral exists must still see %s", tableId)
          .isTrue();
    }
  }

  private UserTableRow findRow(String databaseId, String tableId) {
    return htsRepository
        .findById(UserTableRowPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build())
        .orElseThrow(
            () -> new AssertionError("Expected row " + databaseId + "." + tableId + " to exist"));
  }
}
