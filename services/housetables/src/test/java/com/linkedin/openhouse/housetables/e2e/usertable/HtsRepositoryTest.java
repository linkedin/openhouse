package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static org.assertj.core.api.Assertions.*;

import com.google.common.collect.Lists;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.model.EntityType;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;
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
import org.springframework.jdbc.core.JdbcTemplate;
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

  private static final String[] CASE_VISIBLE_TABLE_IDS = {"case00_null", "case01_upper_table"};

  private static final String CASE_GARBAGE_ID = "case07_garbage";

  @Autowired UserTableHtsJdbcRepository htsRepository;

  @Autowired DataSource dataSource;

  @AfterEach
  public void tearDown() {
    // The JPA cleanup loads every row, so a planted non-canonical spelling must go first.
    new JdbcTemplate(dataSource)
        .update("DELETE FROM user_table_row WHERE entity_type NOT IN ('TABLE', 'VIEW')");
    htsRepository.deleteAll();
  }

  private UserTableRow row(String databaseId, String tableId, EntityType entityType) {
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

  /**
   * The enum-typed entity cannot express a non-canonical spelling, and the converter refuses to
   * write a null, so a row holding either can only be planted through the column itself.
   */
  private void insertRawEntityType(String databaseId, String tableId, String entityType) {
    new JdbcTemplate(dataSource)
        .update(
            "INSERT INTO user_table_row "
                + "(database_id, table_id, version, metadata_location, storage_type, creation_time, entity_type) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?)",
            databaseId,
            tableId,
            0L,
            String.format("/openhouse/%s/%s/v0_metadata.json", databaseId, tableId),
            TEST_DEFAULT_STORAGE_TYPE,
            TEST_CREATION_TIME,
            entityType);
  }

  private String readRawEntityType(String databaseId, String tableId) {
    return new JdbcTemplate(dataSource)
        .queryForObject(
            "SELECT entity_type FROM user_table_row WHERE database_id = ? AND table_id = ?",
            String.class,
            databaseId,
            tableId);
  }

  /**
   * Plants a legacy row through the column, because the write path does not accept a null
   * discriminator.
   */
  private void seedLegacyRow(String databaseId, String tableId) {
    insertRawEntityType(databaseId, tableId, null);
  }

  /** Plants a typed row through JPA, so the enum boundary is still the thing under test. */
  private void seedTypedRow(String databaseId, String tableId, EntityType entityType) {
    htsRepository.save(row(databaseId, tableId, entityType));
  }

  /** Seeds the canonical 7-row interleaved fixture into {@code databaseId} under {@code prefix}. */
  private void seedCanonicalRows(String databaseId, String prefix) {
    seedLegacyRow(databaseId, prefix + "t00_legacy");
    seedTypedRow(databaseId, prefix + "t01_view", EntityType.VIEW);
    seedTypedRow(databaseId, prefix + "t02_explicit", EntityType.TABLE);
    seedTypedRow(databaseId, prefix + "t03_view", EntityType.VIEW);
    seedLegacyRow(databaseId, prefix + "t04_legacy");
    seedTypedRow(databaseId, prefix + "t05_view", EntityType.VIEW);
    seedTypedRow(databaseId, prefix + "t06_explicit", EntityType.TABLE);
  }

  /**
   * Seeds {@link #CASE_DB} with the two canonical visible rows plus every non-table spelling a
   * legacy writer could have left behind. Only spellings the table predicate excludes are planted
   * here, so the table-scoped queries never return one.
   */
  private void seedCaseNormalizationRows() {
    seedLegacyRow(CASE_DB, "case00_null");
    seedTypedRow(CASE_DB, "case01_upper_table", EntityType.TABLE);
    insertRawEntityType(CASE_DB, "case04_upper_view", "VIEW");
    insertRawEntityType(CASE_DB, "case05_lower_view", "view");
    insertRawEntityType(CASE_DB, "case06_mixed_view", "ViEw");
    insertRawEntityType(CASE_DB, CASE_GARBAGE_ID, "UNKNOWN");
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
        Lists.newArrayList(
            htsRepository.findAllTablesByFilters("test_db0", null, null, null, null, null));
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

    // The row is a table from construction, so storage returns exactly what was written.
    Assertions.assertEquals(
        testUserTableRow.toBuilder().entityType(EntityType.TABLE).build(), actual);
    htsRepository.deleteTableById(
        UserTableRowPrimaryKey.builder().databaseId(TEST_DB_ID).tableId(TEST_TABLE_ID).build());
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
    assertThat(htsRepository.deleteTableById(key)).isEqualTo(1);
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

    htsRepository.deleteTableById(
        UserTableRowPrimaryKey.builder().databaseId(TEST_DB_ID).tableId(TEST_TABLE_ID).build());
  }

  @Test
  public void testRenameUserTable() {
    // Seeded as a legacy row: a source that already held TABLE would make the raw-column proof
    // below tautological, because an untouched column reads the same either way.
    insertRawEntityType(TEST_TUPLE_1_1.getDatabaseId(), TEST_TUPLE_1_1.getTableId(), null);
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

    // The type is written, not merely assumed: the column itself holds TABLE.
    assertThat(
            readRawEntityType(
                TEST_TUPLE_1_1.getDatabaseId(), TEST_TUPLE_1_1.getTableId() + "_renamed"))
        .isEqualTo("TABLE");

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
    UserTableRow viewRow = htsRepository.save(row(ENTITY_TYPE_DB, "persist_view", EntityType.VIEW));
    UserTableRow tableRow =
        htsRepository.save(row(ENTITY_TYPE_DB, "persist_table", EntityType.TABLE));
    // The strict converter refuses to write a null, so a legacy row is planted through the column.
    insertRawEntityType(ENTITY_TYPE_DB, "persist_legacy", null);
    UserTableRow legacyRow = findRow(ENTITY_TYPE_DB, "persist_legacy");

    // Insert still yields version 0 for all three; the discriminator is orthogonal to versioning.
    assertThat(viewRow.getVersion()).isEqualTo(0L);
    assertThat(tableRow.getVersion()).isEqualTo(0L);
    assertThat(legacyRow.getVersion()).isEqualTo(0L);

    assertThat(findRow(ENTITY_TYPE_DB, "persist_view").getEntityType()).isEqualTo(EntityType.VIEW);
    assertThat(findRow(ENTITY_TYPE_DB, "persist_table").getEntityType())
        .isEqualTo(EntityType.TABLE);
    assertThat(findRow(ENTITY_TYPE_DB, "persist_legacy").getEntityType())
        .isEqualTo(EntityType.TABLE);

    // The stored column text is the constant name, so the enum changes no byte in the database.
    // The null stays null: only the read defaults, the write stores what it was handed.
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "persist_view")).isEqualTo("VIEW");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "persist_table")).isEqualTo("TABLE");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "persist_legacy")).isNull();

    assertThat(findRow(ENTITY_TYPE_DB, "persist_view").getMetadataLocation())
        .isEqualTo(viewRow.getMetadataLocation());

    // An update at the correct version preserves the stored discriminator.
    UserTableRow updated =
        htsRepository.save(
            findRow(ENTITY_TYPE_DB, "persist_view")
                .toBuilder()
                .metadataLocation("/openhouse/entity_type_db/persist_view/v1_metadata.json")
                .build());
    assertThat(updated.getEntityType()).isEqualTo(EntityType.VIEW);
    assertThat(findRow(ENTITY_TYPE_DB, "persist_view").getEntityType()).isEqualTo(EntityType.VIEW);
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "persist_view")).isEqualTo("VIEW");
  }

  /**
   * The column is nullable, the Java field is not. A row whose {@code entity_type} is null reads
   * back as {@link EntityType#TABLE} through every read on the entity, so no caller downstream of
   * the repository has to re-state the null-means-table rule.
   */
  @Test
  public void testStoredNullHydratesAsTable() {
    insertRawEntityType(ENTITY_TYPE_DB, "legacy_null", null);
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "legacy_null")).isNull();

    assertThat(findRow(ENTITY_TYPE_DB, "legacy_null").getEntityType()).isEqualTo(EntityType.TABLE);
    assertThat(
            htsRepository
                .findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(ENTITY_TYPE_DB, "legacy_null")
                .orElseThrow(() -> new AssertionError("table read must resolve a legacy null"))
                .getEntityType())
        .isEqualTo(EntityType.TABLE);
    assertThat(
            Lists.newArrayList(
                htsRepository.findAllTablesByFilters(
                    ENTITY_TYPE_DB, "legacy_null", null, null, null, null)))
        .singleElement()
        .satisfies(r -> assertThat(r.getEntityType()).isEqualTo(EntityType.TABLE));

    // Hydrating a default must not write one back.
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "legacy_null")).isNull();
  }

  /**
   * A value the column should never hold must not silently become a table. Only a null carries the
   * legacy meaning; anything else outside the vocabulary is corrupt and fails loudly, naming the
   * column and the offending value.
   */
  @Test
  public void testUnrecognizedStoredValueFailsLoudly() {
    insertRawEntityType(CASE_DB, "garbage_row", "FOO");

    assertThatThrownBy(
            () ->
                htsRepository.findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
                    CASE_DB, "garbage_row"))
        .hasStackTraceContaining("user_table_row.entity_type")
        .hasStackTraceContaining("FOO");

    assertThat(readRawEntityType(CASE_DB, "garbage_row")).isEqualTo("FOO");
  }

  /** SHOW-TABLES-equivalent plain listing hides views and keeps legacy NULL rows. */
  @Test
  public void testFindAllByDatabaseIdFiltersViewsAndKeepsLegacyTables() {
    seedCanonicalRows(ENTITY_TYPE_DB, "");
    // A table in another database must not leak in.
    seedLegacyRow("other_db", "t00_legacy");

    List<UserTableRow> result =
        Lists.newArrayList(
            htsRepository.findAllTablesByFilters(ENTITY_TYPE_DB, null, null, null, null, null));

    assertThat(tableIds(result)).containsExactly(CANONICAL_TABLE_IDS);
    assertThat(result).allSatisfy(r -> assertThat(r.getEntityType()).isNotEqualTo(EntityType.VIEW));
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
        htsRepository.findAllTablesByFilters(
            ENTITY_TYPE_DB, null, null, null, null, null, sortedPage(0));
    assertThat(page0.getTotalElements()).isEqualTo(4);
    assertThat(page0.getTotalPages()).isEqualTo(2);
    assertThat(page0.getContent()).hasSize(2);
    assertThat(pageTableIds(page0)).containsExactly("t00_legacy", "t02_explicit");

    Page<UserTableRow> page1 =
        htsRepository.findAllTablesByFilters(
            ENTITY_TYPE_DB, null, null, null, null, null, sortedPage(1));
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
    htsRepository.save(row(ENTITY_TYPE_DB, "nomatch_table", EntityType.TABLE));

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
    htsRepository.save(row(ENTITY_TYPE_DB, "nomatch_table", EntityType.TABLE));

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

  /** The general-filter query is table-scoped too: no overload can return a VIEW row. */
  @Test
  public void testFindAllTablesByFiltersReturnsOnlyTables() {
    seedCanonicalRows(ENTITY_TYPE_DB, "");

    assertThat(
            tableIds(
                htsRepository.findAllTablesByFilters(ENTITY_TYPE_DB, null, null, null, null, null)))
        .containsExactly(CANONICAL_TABLE_IDS);

    Page<UserTableRow> page0 =
        htsRepository.findAllTablesByFilters(
            ENTITY_TYPE_DB, null, null, null, null, null, sortedPage(0));
    assertThat(page0.getTotalElements()).isEqualTo(4);
    assertThat(page0.getTotalPages()).isEqualTo(2);
    assertThat(pageTableIds(page0)).containsExactly("t00_legacy", "t02_explicit");

    Page<UserTableRow> page1 =
        htsRepository.findAllTablesByFilters(
            ENTITY_TYPE_DB, null, null, null, null, null, sortedPage(1));
    assertThat(page1.getTotalElements()).isEqualTo(4);
    assertThat(pageTableIds(page1)).containsExactly("t04_legacy", "t06_explicit");

    // A view is unreachable through this family, by tableId as well as by database.
    assertThat(
            Lists.newArrayList(
                htsRepository.findAllTablesByFilters(
                    ENTITY_TYPE_DB, "t01_view", null, null, null, null)))
        .isEmpty();
  }

  /**
   * Every non-table spelling fails closed. Two rows are visible: the legacy NULL and the canonical
   * TABLE. Every spelling of VIEW and an unrecognized value are excluded before pagination, and
   * excluded rows are hidden rather than dropped.
   */
  @Test
  public void testEveryNonTableSpellingAndGarbageFailsClosed() {
    seedCaseNormalizationRows();

    assertThat(
            tableIds(htsRepository.findAllTablesByFilters(CASE_DB, null, null, null, null, null)))
        .containsExactly(CASE_VISIBLE_TABLE_IDS);
    assertThat(
            tableIds(
                htsRepository.findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
                    CASE_DB, "case%")))
        .containsExactly(CASE_VISIBLE_TABLE_IDS);

    Page<UserTableRow> dbPage0 =
        htsRepository.findAllTablesByFilters(CASE_DB, null, null, null, null, null, sortedPage(0));
    assertThat(dbPage0.getTotalElements()).isEqualTo(2);
    assertThat(dbPage0.getTotalPages()).isEqualTo(1);
    assertThat(pageTableIds(dbPage0)).containsExactly(CASE_VISIBLE_TABLE_IDS);

    Page<UserTableRow> patternPage0 =
        htsRepository.findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
            CASE_DB, "case%", sortedPage(0));
    assertThat(patternPage0.getTotalElements()).isEqualTo(2);
    assertThat(patternPage0.getTotalPages()).isEqualTo(1);
    assertThat(pageTableIds(patternPage0)).containsExactly(CASE_VISIBLE_TABLE_IDS);

    // Garbage fails closed: it is neither a table nor a view.
    assertThat(
            tableIds(htsRepository.findAllTablesByFilters(CASE_DB, null, null, null, null, null)))
        .doesNotContain(CASE_GARBAGE_ID);

    // The garbage row is still stored — it is hidden, not dropped.
    assertThat(readRawEntityType(CASE_DB, CASE_GARBAGE_ID)).isEqualTo("UNKNOWN");
  }

  /**
   * SQL matching and hydration must agree on case. The table predicate normalizes explicitly
   * ({@code upper(u.entityType) = 'TABLE'}), so hydration does too — otherwise a legacy row would
   * be selected by the query and then explode while loading, which is worse than either matching or
   * skipping it consistently.
   *
   * <p>H2 runs in {@code MODE=MySQL} which is case-SENSITIVE for string comparison, so a bare
   * {@code = 'TABLE'} predicate would leave the planted row unmatched and the read would simply
   * come back empty. It is matched, which is what proves the query normalizes rather than leaning
   * on a provider collation. No writer can produce such a row: the enum boundary normalizes before
   * the value reaches the column.
   */
  @Test
  public void testLegacyNonCanonicalSpellingIsMatchedAndHydrates() {
    insertRawEntityType(CASE_DB, "case02_lower_table", "table");
    insertRawEntityType(CASE_DB, "case03_mixed_table", "TaBlE");
    insertRawEntityType(CASE_DB, "case05_lower_view", "view");

    assertThat(
            htsRepository
                .findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(CASE_DB, "case02_lower_table")
                .orElseThrow(() -> new AssertionError("the table predicate must match 'table'"))
                .getEntityType())
        .isEqualTo(EntityType.TABLE);
    assertThat(
            htsRepository
                .findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(CASE_DB, "case03_mixed_table")
                .orElseThrow(() -> new AssertionError("the table predicate must match 'TaBlE'"))
                .getEntityType())
        .isEqualTo(EntityType.TABLE);

    // The view spellings the table predicate excludes still hydrate through the neutral read.
    assertThat(
            htsRepository
                .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(CASE_DB, "case05_lower_view")
                .orElseThrow(() -> new AssertionError("the neutral read must see 'view'"))
                .getEntityType())
        .isEqualTo(EntityType.VIEW);

    // Reading normalizes; the column text does not change.
    assertThat(readRawEntityType(CASE_DB, "case02_lower_table")).isEqualTo("table");
    assertThat(readRawEntityType(CASE_DB, "case03_mixed_table")).isEqualTo("TaBlE");
    assertThat(readRawEntityType(CASE_DB, "case05_lower_view")).isEqualTo("view");
  }

  /**
   * Table-scoped point read: the query, not any caller, is what makes a view unreadable through the
   * table path. NULL and TABLE resolve; every spelling of VIEW and an unrecognized value resolve to
   * empty.
   */
  @ParameterizedTest
  @CsvSource({
    "case00_null,        true",
    "case01_upper_table, true",
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

    for (String tableId : new String[] {"case00_null", "case01_upper_table", "case04_upper_view"}) {
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

  /**
   * The type is chosen by which method you call, not by an argument. {@code findAllByFilters} is
   * the general query and returns both types; its table-scoped sibling adds only the table
   * predicate, which also matches a legacy stored null.
   */
  @Test
  public void testGeneralFiltersReturnBothTypesAndTableFiltersReturnOnlyTables() {
    seedCanonicalRows(ENTITY_TYPE_DB, "");

    List<String> everything = new ArrayList<>(Arrays.asList(CANONICAL_TABLE_IDS));
    everything.addAll(Arrays.asList(CANONICAL_VIEW_IDS));
    Collections.sort(everything);

    assertThat(
            tableIds(htsRepository.findAllByFilters(ENTITY_TYPE_DB, null, null, null, null, null)))
        .as("the general query must return tables and views together")
        .isEqualTo(everything);

    assertThat(
            tableIds(
                htsRepository.findAllTablesByFilters(ENTITY_TYPE_DB, null, null, null, null, null)))
        .as("the table query must return tables and legacy nulls only")
        .containsExactly(CANONICAL_TABLE_IDS);

    // A view is unreachable through the table family, by tableId as well as by database.
    assertThat(
            Lists.newArrayList(
                htsRepository.findAllTablesByFilters(
                    ENTITY_TYPE_DB, "t01_view", null, null, null, null)))
        .isEmpty();

    // Paged overloads agree, counts included.
    Page<UserTableRow> anyPage0 =
        htsRepository.findAllByFilters(ENTITY_TYPE_DB, null, null, null, null, null, sortedPage(0));
    assertThat(anyPage0.getTotalElements()).isEqualTo(7);
    assertThat(pageTableIds(anyPage0)).containsExactly("t00_legacy", "t01_view");

    Page<UserTableRow> tablePage0 =
        htsRepository.findAllTablesByFilters(
            ENTITY_TYPE_DB, null, null, null, null, null, sortedPage(0));
    assertThat(tablePage0.getTotalElements()).isEqualTo(4);
    assertThat(tablePage0.getTotalPages()).isEqualTo(2);
    assertThat(pageTableIds(tablePage0)).containsExactly("t00_legacy", "t02_explicit");
  }

  /** The pattern family splits the same way. */
  @Test
  public void testGeneralPatternReturnsBothTypesAndTablePatternOnlyTables() {
    seedCanonicalRows(ENTITY_TYPE_DB, "match_");

    assertThat(
            tableIds(
                htsRepository.findAllByDatabaseIdAndTableIdLikeAllIgnoreCase(
                    ENTITY_TYPE_DB, "match_%")))
        .hasSize(7);

    assertThat(
            tableIds(
                htsRepository.findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
                    ENTITY_TYPE_DB, "match_%")))
        .containsExactly(
            "match_t00_legacy", "match_t02_explicit", "match_t04_legacy", "match_t06_explicit");

    Page<UserTableRow> tablePage0 =
        htsRepository.findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
            ENTITY_TYPE_DB, "match_%", sortedPage(0));
    assertThat(tablePage0.getTotalElements()).isEqualTo(4);
    assertThat(tablePage0.getTotalPages()).isEqualTo(2);
  }

  // ---------------------------------------------------------------------------------------------
  // view-scoped reads
  // ---------------------------------------------------------------------------------------------

  /** The view predicate has no legacy-null arm: a stored null is a table, unreachable here. */
  @ParameterizedTest
  @CsvSource({
    "case00_null,        false",
    "case01_upper_table, false",
    "case04_upper_view,  true",
    "case05_lower_view,  true",
    "case06_mixed_view,  true",
    "case07_garbage,     false"
  })
  public void testFindViewByKeyResolvesOnlyViewRows(String tableId, boolean expectedVisible) {
    seedCaseNormalizationRows();

    assertThat(
            htsRepository
                .findViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(CASE_DB, tableId)
                .isPresent())
        .as("findViewBy... for %s", tableId)
        .isEqualTo(expectedVisible);

    // Case-insensitive on the key itself, exactly like the table and neutral point reads.
    assertThat(
            htsRepository
                .findViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
                    CASE_DB.toUpperCase(), tableId.toUpperCase())
                .isPresent())
        .isEqualTo(expectedVisible);
  }

  /** A row the SQL predicate matched must never then fail to hydrate. */
  @ParameterizedTest
  @CsvSource({"case04_upper_view, VIEW", "case05_lower_view, view", "case06_mixed_view, ViEw"})
  public void testFindViewByKeyHydratesEveryViewSpelling(String tableId, String storedSpelling) {
    seedCaseNormalizationRows();

    assertThat(
            htsRepository
                .findViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(CASE_DB, tableId)
                .orElseThrow(() -> new AssertionError("the view predicate must match " + tableId))
                .getEntityType())
        .as("view read must hydrate %s", tableId)
        .isEqualTo(EntityType.VIEW);

    // Reading normalizes; the column text does not change.
    assertThat(readRawEntityType(CASE_DB, tableId)).isEqualTo(storedSpelling);
  }

  /** The exact-filter view family returns views only, and its page filters before it pages. */
  @Test
  public void testFindAllViewsByFiltersReturnsOnlyViewsAndFiltersBeforePagination() {
    seedCanonicalRows(ENTITY_TYPE_DB, "");
    // A view in another database must not leak in.
    htsRepository.save(row("other_db", "t01_view", EntityType.VIEW));

    assertThat(
            tableIds(
                htsRepository.findAllViewsByFilters(ENTITY_TYPE_DB, null, null, null, null, null)))
        .containsExactly(CANONICAL_VIEW_IDS);

    // A table is unreachable through this family, by tableId as well as by database.
    assertThat(
            Lists.newArrayList(
                htsRepository.findAllViewsByFilters(
                    ENTITY_TYPE_DB, "t00_legacy", null, null, null, null)))
        .isEmpty();
    assertThat(
            Lists.newArrayList(
                htsRepository.findAllViewsByFilters(
                    ENTITY_TYPE_DB, "t02_explicit", null, null, null, null)))
        .isEmpty();

    // A fetch-then-filter implementation would report totalElements=7/totalPages=4 and a 1-row
    // first page; filtering before paging yields a full first page over exactly three views.
    Page<UserTableRow> page0 =
        htsRepository.findAllViewsByFilters(
            ENTITY_TYPE_DB, null, null, null, null, null, sortedPage(0));
    assertThat(page0.getTotalElements()).isEqualTo(3);
    assertThat(page0.getTotalPages()).isEqualTo(2);
    assertThat(page0.getContent()).hasSize(2);
    assertThat(pageTableIds(page0)).containsExactly("t01_view", "t03_view");

    Page<UserTableRow> page1 =
        htsRepository.findAllViewsByFilters(
            ENTITY_TYPE_DB, null, null, null, null, null, sortedPage(1));
    assertThat(page1.getTotalElements()).isEqualTo(3);
    assertThat(page1.getTotalPages()).isEqualTo(2);
    assertThat(pageTableIds(page1)).containsExactly("t05_view");

    assertThat(pageTableIds(page0)).doesNotContainAnyElementsOf(Arrays.asList(CANONICAL_TABLE_IDS));
    assertThat(pageTableIds(page1)).doesNotContainAnyElementsOf(Arrays.asList(CANONICAL_TABLE_IDS));
  }

  /** The pattern (LIKE) view family applies the same predicate, plain and paged. */
  @Test
  public void testFindAllViewsByPatternReturnsOnlyViewsAndFiltersBeforePagination() {
    seedCanonicalRows(ENTITY_TYPE_DB, "match_");
    // Non-matching view in the same database must be excluded by the pattern, not by type.
    htsRepository.save(row(ENTITY_TYPE_DB, "nomatch_view", EntityType.VIEW));

    assertThat(
            tableIds(
                htsRepository.findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(
                    ENTITY_TYPE_DB, "match_%")))
        .containsExactly("match_t01_view", "match_t03_view", "match_t05_view");

    Page<UserTableRow> page0 =
        htsRepository.findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(
            ENTITY_TYPE_DB, "match_%", sortedPage(0));
    assertThat(page0.getTotalElements()).isEqualTo(3);
    assertThat(page0.getTotalPages()).isEqualTo(2);
    assertThat(page0.getContent()).hasSize(2);
    assertThat(pageTableIds(page0)).containsExactly("match_t01_view", "match_t03_view");

    Page<UserTableRow> page1 =
        htsRepository.findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(
            ENTITY_TYPE_DB, "match_%", sortedPage(1));
    assertThat(page1.getTotalElements()).isEqualTo(3);
    assertThat(pageTableIds(page1)).containsExactly("match_t05_view");
  }

  /**
   * H2 in {@code MODE=MySQL} compares case-sensitively, so a bare {@code = 'VIEW'} would drop the
   * lower and mixed-case rows; returning them is what proves {@code upper(...)} is applied.
   */
  @Test
  public void testViewQueriesIncludeEverySpellingAndExcludeNullAndGarbage() {
    seedCaseNormalizationRows();

    List<String> everyViewSpelling =
        Arrays.asList("case04_upper_view", "case05_lower_view", "case06_mixed_view");

    assertThat(tableIds(htsRepository.findAllViewsByFilters(CASE_DB, null, null, null, null, null)))
        .containsExactlyElementsOf(everyViewSpelling);
    assertThat(
            tableIds(
                htsRepository.findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(
                    CASE_DB, "case%")))
        .containsExactlyElementsOf(everyViewSpelling);

    Page<UserTableRow> dbPage0 =
        htsRepository.findAllViewsByFilters(CASE_DB, null, null, null, null, null, sortedPage(0));
    assertThat(dbPage0.getTotalElements()).isEqualTo(3);
    assertThat(dbPage0.getTotalPages()).isEqualTo(2);

    Page<UserTableRow> patternPage0 =
        htsRepository.findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(
            CASE_DB, "case%", sortedPage(0));
    assertThat(patternPage0.getTotalElements()).isEqualTo(3);

    // A legacy null is a table, and an unrecognized value is neither; both fail closed here.
    assertThat(tableIds(htsRepository.findAllViewsByFilters(CASE_DB, null, null, null, null, null)))
        .doesNotContain("case00_null", "case01_upper_table", CASE_GARBAGE_ID);

    // Rows the view predicate excludes are hidden, not dropped.
    assertThat(readRawEntityType(CASE_DB, "case00_null")).isNull();
    assertThat(readRawEntityType(CASE_DB, CASE_GARBAGE_ID)).isEqualTo("UNKNOWN");
  }

  // ---------------------------------------------------------------------------------------------
  // type-scoped deletion
  // ---------------------------------------------------------------------------------------------

  /** One conditional statement, not a read-then-delete; a wrong-type key reports zero. */
  @Test
  public void testDeleteTableByIdRemovesOnlyTableRows() {
    seedLegacyRow(ENTITY_TYPE_DB, "del_legacy");
    seedTypedRow(ENTITY_TYPE_DB, "del_table", EntityType.TABLE);
    seedTypedRow(ENTITY_TYPE_DB, "del_view", EntityType.VIEW);

    // A view at a table-scoped delete is a no-op, and the view survives byte-identical.
    UserTableRow viewBefore = findRow(ENTITY_TYPE_DB, "del_view");
    assertThat(htsRepository.deleteTableById(key(ENTITY_TYPE_DB, "del_view"))).isEqualTo(0);
    UserTableRow viewAfter = findRow(ENTITY_TYPE_DB, "del_view");
    assertThat(viewAfter.getEntityType()).isEqualTo(EntityType.VIEW);
    assertThat(viewAfter.getVersion()).isEqualTo(viewBefore.getVersion());
    assertThat(viewAfter.getMetadataLocation()).isEqualTo(viewBefore.getMetadataLocation());

    // A missing key is the same zero, so the service maps both to one 404.
    assertThat(htsRepository.deleteTableById(key(ENTITY_TYPE_DB, "del_absent"))).isEqualTo(0);

    // Explicit TABLE and legacy NULL are both tables and are both removed, case-insensitively.
    assertThat(htsRepository.deleteTableById(key(ENTITY_TYPE_DB, "DEL_TABLE"))).isEqualTo(1);
    assertThat(htsRepository.deleteTableById(key(ENTITY_TYPE_DB, "del_legacy"))).isEqualTo(1);
    assertThat(htsRepository.existsById(key(ENTITY_TYPE_DB, "del_table"))).isFalse();
    assertThat(htsRepository.existsById(key(ENTITY_TYPE_DB, "del_legacy"))).isFalse();
    assertThat(htsRepository.existsById(key(ENTITY_TYPE_DB, "del_view"))).isTrue();
  }

  /** The mirror: a view delete cannot reach a table or a legacy null. */
  @Test
  public void testDeleteViewByIdRemovesOnlyViewRows() {
    seedLegacyRow(ENTITY_TYPE_DB, "del_legacy");
    seedTypedRow(ENTITY_TYPE_DB, "del_table", EntityType.TABLE);
    seedTypedRow(ENTITY_TYPE_DB, "del_view", EntityType.VIEW);

    assertThat(htsRepository.deleteViewById(key(ENTITY_TYPE_DB, "del_table"))).isEqualTo(0);
    assertThat(htsRepository.deleteViewById(key(ENTITY_TYPE_DB, "del_legacy"))).isEqualTo(0);
    assertThat(htsRepository.deleteViewById(key(ENTITY_TYPE_DB, "del_absent"))).isEqualTo(0);

    assertThat(findRow(ENTITY_TYPE_DB, "del_table").getEntityType()).isEqualTo(EntityType.TABLE);
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "del_legacy")).isNull();

    assertThat(htsRepository.deleteViewById(key(ENTITY_TYPE_DB, "DEL_VIEW"))).isEqualTo(1);
    assertThat(htsRepository.existsById(key(ENTITY_TYPE_DB, "del_view"))).isFalse();
    assertThat(htsRepository.existsById(key(ENTITY_TYPE_DB, "del_table"))).isTrue();
    assertThat(htsRepository.existsById(key(ENTITY_TYPE_DB, "del_legacy"))).isTrue();
  }

  /** A corrupt discriminator matches neither predicate, so the row is left for an operator. */
  @Test
  public void testTypedDeletesAndTableRenameIgnoreCorruptRows() {
    insertRawEntityType(CASE_DB, "corrupt_row", "UNKNOWN");

    assertThat(htsRepository.deleteTableById(key(CASE_DB, "corrupt_row"))).isEqualTo(0);
    assertThat(htsRepository.deleteViewById(key(CASE_DB, "corrupt_row"))).isEqualTo(0);
    assertThat(
            htsRepository.renameTableId(
                CASE_DB,
                "corrupt_row",
                CASE_DB,
                "corrupt_row_renamed",
                "/openhouse/entity_type_case_db/corrupt_row_renamed/v1_metadata.json"))
        .isEqualTo(0);

    assertThat(readRawEntityType(CASE_DB, "corrupt_row")).isEqualTo("UNKNOWN");
  }

  /**
   * Programming-error guard, not an HTTP path: no route reaches these, so the point is that a
   * future caller cannot reintroduce a neutral key-addressed mutation by accident. The no-arg
   * {@code deleteAll()} is deliberately not sealed because it addresses no key.
   */
  @Test
  public void testInheritedKeyAddressedDeletesAreSealed() {
    htsRepository.save(row(ENTITY_TYPE_DB, "sealed_view", EntityType.VIEW));
    UserTableRow sealedView = findRow(ENTITY_TYPE_DB, "sealed_view");
    UserTableRowPrimaryKey sealedKey = key(ENTITY_TYPE_DB, "sealed_view");

    assertThatThrownBy(() -> htsRepository.deleteById(sealedKey))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> htsRepository.delete(sealedView))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> htsRepository.deleteAllById(Collections.singletonList(sealedKey)))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> htsRepository.deleteAll(Collections.singletonList(sealedView)))
        .isInstanceOf(UnsupportedOperationException.class);

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "sealed_view")).isEqualTo("VIEW");

    // The whole-repository administrative form keeps working; three teardowns depend on it.
    htsRepository.deleteAll();
    assertThat(htsRepository.existsById(sealedKey)).isFalse();
  }

  // ---------------------------------------------------------------------------------------------
  // table-scoped rename
  // ---------------------------------------------------------------------------------------------

  /** A rename scoped to tables cannot move a view, and reports zero rather than throwing. */
  @Test
  public void testRenameTableIdRefusesViewSource() {
    htsRepository.save(row(ENTITY_TYPE_DB, "rename_view_src", EntityType.VIEW));
    UserTableRow before = findRow(ENTITY_TYPE_DB, "rename_view_src");

    assertThat(
            htsRepository.renameTableId(
                ENTITY_TYPE_DB,
                "rename_view_src",
                ENTITY_TYPE_DB,
                "rename_view_dst",
                "/openhouse/entity_type_db/rename_view_dst/v1_metadata.json"))
        .isEqualTo(0);

    assertThat(htsRepository.existsById(key(ENTITY_TYPE_DB, "rename_view_dst"))).isFalse();
    UserTableRow after = findRow(ENTITY_TYPE_DB, "rename_view_src");
    assertThat(after.getEntityType()).isEqualTo(EntityType.VIEW);
    assertThat(after.getMetadataLocation()).isEqualTo(before.getMetadataLocation());
    assertThat(after.getVersion()).isEqualTo(before.getVersion());
  }

  /** A missing source is the same zero the service maps to 404. */
  @Test
  public void testRenameTableIdMissingSourceAffectsZeroRows() {
    assertThat(
            htsRepository.renameTableId(
                ENTITY_TYPE_DB,
                "rename_absent_src",
                ENTITY_TYPE_DB,
                "rename_absent_dst",
                "/openhouse/entity_type_db/rename_absent_dst/v1_metadata.json"))
        .isEqualTo(0);
    assertThat(htsRepository.existsById(key(ENTITY_TYPE_DB, "rename_absent_dst"))).isFalse();
  }

  /**
   * The source column holds SQL NULL, so a hydrated {@code getEntityType() == TABLE} would be
   * tautological; only the raw column distinguishes "stamped" from "left alone".
   */
  @Test
  public void testRenameStampsCanonicalTableOnLegacyNullSource() {
    insertRawEntityType(ENTITY_TYPE_DB, "rename_legacy_src", null);
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_legacy_src")).isNull();

    assertThat(
            htsRepository.renameTableId(
                ENTITY_TYPE_DB,
                "rename_legacy_src",
                ENTITY_TYPE_DB,
                "rename_legacy_dst",
                "/openhouse/entity_type_db/rename_legacy_dst/v1_metadata.json"))
        .isEqualTo(1);

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_legacy_dst")).isEqualTo("TABLE");
    assertThat(findRow(ENTITY_TYPE_DB, "rename_legacy_dst").getMetadataLocation())
        .isEqualTo("/openhouse/entity_type_db/rename_legacy_dst/v1_metadata.json");
    assertThat(htsRepository.existsById(key(ENTITY_TYPE_DB, "rename_legacy_src"))).isFalse();
  }

  /** The same for a non-canonical stored spelling: the rename rewrites it to the constant. */
  @Test
  public void testRenameStampsCanonicalTableOnMixedCaseSource() {
    insertRawEntityType(ENTITY_TYPE_DB, "rename_mixed_src", "TaBlE");

    assertThat(
            htsRepository.renameTableId(
                ENTITY_TYPE_DB,
                "rename_mixed_src",
                ENTITY_TYPE_DB,
                "rename_mixed_dst",
                "/openhouse/entity_type_db/rename_mixed_dst/v1_metadata.json"))
        .isEqualTo(1);

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_mixed_dst")).isEqualTo("TABLE");
  }

  /**
   * The shared primary key is what turns an occupied destination into a conflict, whatever type or
   * spelling occupies it. Nothing is mutated on either side.
   *
   * <p>Regression guard: a view-typed or corrupt-typed destination must stay "occupied" and never
   * read as "free" under {@code TABLE_ROW_PREDICATE}.
   */
  @Test
  public void testRenameIntoOccupiedDestinationLeavesBothRowsUnchanged() {
    htsRepository.save(row(ENTITY_TYPE_DB, "rename_src", EntityType.TABLE));
    htsRepository.save(row(ENTITY_TYPE_DB, "rename_dst_view", EntityType.VIEW));
    insertRawEntityType(ENTITY_TYPE_DB, "rename_dst_corrupt", "UNKNOWN");

    assertThatThrownBy(
            () ->
                htsRepository.renameTableId(
                    ENTITY_TYPE_DB,
                    "rename_src",
                    ENTITY_TYPE_DB,
                    "rename_dst_view",
                    "/openhouse/entity_type_db/rename_dst_view/v1_metadata.json"))
        .isInstanceOf(DataIntegrityViolationException.class);

    assertThatThrownBy(
            () ->
                htsRepository.renameTableId(
                    ENTITY_TYPE_DB,
                    "rename_src",
                    ENTITY_TYPE_DB,
                    "rename_dst_corrupt",
                    "/openhouse/entity_type_db/rename_dst_corrupt/v1_metadata.json"))
        .isInstanceOf(DataIntegrityViolationException.class);

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_src")).isEqualTo("TABLE");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_dst_view")).isEqualTo("VIEW");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "rename_dst_corrupt")).isEqualTo("UNKNOWN");
  }

  private static UserTableRowPrimaryKey key(String databaseId, String tableId) {
    return UserTableRowPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build();
  }

  private UserTableRow findRow(String databaseId, String tableId) {
    return htsRepository
        .findById(UserTableRowPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build())
        .orElseThrow(
            () -> new AssertionError("Expected row " + databaseId + "." + tableId + " to exist"));
  }
}
