package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.CorruptEntityTypeException;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.e2e.SpringH2HtsApplication;
import com.linkedin.openhouse.housetables.e2e.fixture.UserTableRawSeeder;
import com.linkedin.openhouse.housetables.e2e.fixture.UserTableStoreCleaner;
import com.linkedin.openhouse.housetables.metrics.UserTableMetricsConstant;
import com.linkedin.openhouse.housetables.model.EntityType;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.UserTableReadRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.SoftDeletedUserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.UserTablesService;
import com.linkedin.openhouse.housetables.services.model.UserViewQuery;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.domain.Page;
import org.springframework.data.util.Pair;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest(classes = SpringH2HtsApplication.class)
public class UserTablesServiceTest {

  private static final String CASE_DB_1 = "deleteDb1";
  private static final String CASE_DB_2 = "deleteDb2";
  private static final String CASE_TBL_1 = "deleteTb1";
  private static final String CASE_TBL_2 = "deleteTb2";

  @Autowired UserTablesService userTablesService;

  @SpyBean UserTableHtsJdbcRepository htsRepository;

  @Autowired UserTableStoreCleaner userTableStoreCleaner;

  @Autowired UserTableRawSeeder userTableRawSeeder;

  @SpyBean SoftDeletedUserTableHtsJdbcRepository softDeletedHtsJdbcRepository;

  @SpyBean UserTableReadRepository userTableReadRepository;

  @Autowired DataSource dataSource;

  @BeforeEach
  public void setup() {
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    userTableRawSeeder.seedLegacyRow(testUserTableRow);
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_0.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_2_0.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_3_0.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_4_0.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_1.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_2_1.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_3_1.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_2.get_userTableRow());
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_2_2.get_userTableRow());

    // delete candidate
    userTableRawSeeder.seedLegacyRow(
        TEST_TUPLE_1_0
            .get_userTableRow()
            .toBuilder()
            .tableId(CASE_TBL_1)
            .databaseId(CASE_DB_1)
            .build());
    userTableRawSeeder.seedLegacyRow(
        TEST_TUPLE_1_0
            .get_userTableRow()
            .toBuilder()
            .tableId(CASE_TBL_2)
            .databaseId(CASE_DB_2)
            .build());
    // Clear any mocks
    Mockito.reset(htsRepository);
    Mockito.reset(softDeletedHtsJdbcRepository);
    Mockito.reset(userTableReadRepository);
  }

  @AfterEach
  public void tearDown() {
    // The JPA cleanup loads every row, so a planted non-canonical spelling must go first.
    new JdbcTemplate(dataSource)
        .update("DELETE FROM user_table_row WHERE entity_type NOT IN ('TABLE', 'VIEW')");
    userTableStoreCleaner.clear();
    softDeletedHtsJdbcRepository.deleteAll();
  }

  @Test
  public void testUserTableGet() {
    // TODO: Use service layer function to create/update the repository.
    Assertions.assertTrue(
        isUserTableDtoEqual(
            asStored(TestHouseTableModelConstants.TEST_USER_TABLE_DTO),
            userTablesService.getUserTable(
                TestHouseTableModelConstants.TEST_DB_ID,
                TestHouseTableModelConstants.TEST_TABLE_ID)));
    Assertions.assertTrue(
        isUserTableDtoEqual(
            asStored(TEST_TUPLE_1_0.get_userTableDto()),
            userTablesService.getUserTable(
                TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId())));
    Assertions.assertTrue(
        isUserTableDtoEqual(
            asStored(TestHouseTableModelConstants.TEST_TUPLE_1_1.get_userTableDto()),
            userTablesService.getUserTable(
                TestHouseTableModelConstants.TEST_TUPLE_1_1.getDatabaseId(),
                TestHouseTableModelConstants.TEST_TUPLE_1_1.getTableId())));
    // testing case insensitivity when lookup by repeating the lookup again
    Assertions.assertTrue(
        isUserTableDtoEqual(
            asStored(TestHouseTableModelConstants.TEST_USER_TABLE_DTO),
            userTablesService.getUserTable(
                TestHouseTableModelConstants.TEST_DB_ID.toLowerCase(),
                TestHouseTableModelConstants.TEST_TABLE_ID.toLowerCase())));
    Assertions.assertTrue(
        isUserTableDtoEqual(
            asStored(TEST_TUPLE_1_0.get_userTableDto()),
            userTablesService.getUserTable(
                TEST_TUPLE_1_0.getDatabaseId().toLowerCase(),
                TEST_TUPLE_1_0.getTableId().toLowerCase())));
    Assertions.assertTrue(
        isUserTableDtoEqual(
            asStored(TestHouseTableModelConstants.TEST_TUPLE_1_1.get_userTableDto()),
            userTablesService.getUserTable(
                TestHouseTableModelConstants.TEST_TUPLE_1_1.getDatabaseId().toUpperCase(),
                TestHouseTableModelConstants.TEST_TUPLE_1_1.getTableId().toUpperCase())));
  }

  @Test
  public void testGetUserTables() {
    UserTable userTable =
        UserTable.builder().databaseId(TestHouseTableModelConstants.TEST_DB_ID).build();
    List<UserTableDto> list = userTablesService.getAllUserTables(userTable);
    Assertions.assertEquals(5, list.size());
    Page<UserTableDto> userTableDtoPage0 =
        userTablesService.getAllUserTables(userTable, 0, 2, "tableId");
    Assertions.assertEquals(5, userTableDtoPage0.getTotalElements());
    Assertions.assertEquals(3, userTableDtoPage0.getTotalPages());
    List<UserTableDto> list0 = userTableDtoPage0.getContent();
    Assertions.assertEquals(2, list0.size());

    Page<UserTableDto> userTableDtoPage1 =
        userTablesService.getAllUserTables(userTable, 1, 2, "tableId");
    Assertions.assertEquals(5, userTableDtoPage1.getTotalElements());
    Assertions.assertEquals(3, userTableDtoPage1.getTotalPages());
    List<UserTableDto> list1 = userTableDtoPage1.getContent();
    Assertions.assertEquals(2, list1.size());

    Page<UserTableDto> userTableDtoPage2 =
        userTablesService.getAllUserTables(userTable, 2, 2, "tableId");
    Assertions.assertEquals(5, userTableDtoPage2.getTotalElements());
    Assertions.assertEquals(3, userTableDtoPage2.getTotalPages());
    List<UserTableDto> list2 = userTableDtoPage2.getContent();
    Assertions.assertEquals(1, list2.size());
  }

  @Test
  public void testListDatabases() {
    UserTable userTable = UserTable.builder().build();
    List<UserTableDto> list = userTablesService.getAllUserTables(userTable);
    Assertions.assertEquals(5, list.size());
    Page<UserTableDto> userTableDtoPage0 =
        userTablesService.getAllUserTables(userTable, 0, 2, "databaseId");
    Assertions.assertEquals(5, userTableDtoPage0.getTotalElements());
    Assertions.assertEquals(3, userTableDtoPage0.getTotalPages());
    List<UserTableDto> list0 = userTableDtoPage0.getContent();
    Assertions.assertEquals(2, list0.size());
    for (UserTableDto userTableDto : list0) {
      Assertions.assertNotNull(userTableDto.getDatabaseId());
      Assertions.assertNull(userTableDto.getTableId());
    }

    Page<UserTableDto> userTableDtoPage1 =
        userTablesService.getAllUserTables(userTable, 1, 2, "databaseId");
    Assertions.assertEquals(5, userTableDtoPage1.getTotalElements());
    Assertions.assertEquals(3, userTableDtoPage1.getTotalPages());
    List<UserTableDto> list1 = userTableDtoPage1.getContent();
    Assertions.assertEquals(2, list1.size());
    for (UserTableDto userTableDto : list1) {
      Assertions.assertNotNull(userTableDto.getDatabaseId());
      Assertions.assertNull(userTableDto.getTableId());
    }

    Page<UserTableDto> userTableDtoPage2 =
        userTablesService.getAllUserTables(userTable, 2, 2, "databaseId");
    Assertions.assertEquals(5, userTableDtoPage2.getTotalElements());
    Assertions.assertEquals(3, userTableDtoPage2.getTotalPages());
    List<UserTableDto> list2 = userTableDtoPage2.getContent();
    Assertions.assertEquals(1, list2.size());
    for (UserTableDto userTableDto : list2) {
      Assertions.assertNotNull(userTableDto.getDatabaseId());
      Assertions.assertNull(userTableDto.getTableId());
    }
  }

  @Test
  public void testGetUserTablesWithTablePattern() {
    UserTable userTable =
        UserTable.builder()
            .databaseId(TestHouseTableModelConstants.TEST_DB_ID)
            .tableId("test_table%")
            .build();
    List<UserTableDto> list = userTablesService.getAllUserTables(userTable);
    Assertions.assertEquals(5, list.size());
    Page<UserTableDto> userTableDtoPage0 =
        userTablesService.getAllUserTables(userTable, 0, 2, "tableId");
    Assertions.assertEquals(5, userTableDtoPage0.getTotalElements());
    Assertions.assertEquals(3, userTableDtoPage0.getTotalPages());
    List<UserTableDto> list0 = userTableDtoPage0.getContent();
    Assertions.assertEquals(2, list0.size());

    Page<UserTableDto> userTableDtoPage1 =
        userTablesService.getAllUserTables(userTable, 1, 2, "tableId");
    Assertions.assertEquals(5, userTableDtoPage1.getTotalElements());
    Assertions.assertEquals(3, userTableDtoPage1.getTotalPages());
    List<UserTableDto> list1 = userTableDtoPage1.getContent();
    Assertions.assertEquals(2, list1.size());

    Page<UserTableDto> userTableDtoPage2 =
        userTablesService.getAllUserTables(userTable, 2, 2, "tableId");
    Assertions.assertEquals(5, userTableDtoPage2.getTotalElements());
    Assertions.assertEquals(3, userTableDtoPage2.getTotalPages());
    List<UserTableDto> list2 = userTableDtoPage2.getContent();
    Assertions.assertEquals(1, list2.size());
  }

  @Test
  public void testGetUserTablesWithSearchFilter() {
    UserTable userTable = UserTable.builder().creationTime(123L).build();
    List<UserTableDto> list = userTablesService.getAllUserTables(userTable);
    Assertions.assertEquals(12, list.size());
    Page<UserTableDto> userTableDtoPage0 =
        userTablesService.getAllUserTables(userTable, 0, 4, "tableId");
    Assertions.assertEquals(12, userTableDtoPage0.getTotalElements());
    Assertions.assertEquals(3, userTableDtoPage0.getTotalPages());
    List<UserTableDto> list0 = userTableDtoPage0.getContent();
    Assertions.assertEquals(4, list0.size());

    Page<UserTableDto> userTableDtoPage1 =
        userTablesService.getAllUserTables(userTable, 1, 4, "tableId");
    Assertions.assertEquals(12, userTableDtoPage1.getTotalElements());
    Assertions.assertEquals(3, userTableDtoPage1.getTotalPages());
    List<UserTableDto> list1 = userTableDtoPage1.getContent();
    Assertions.assertEquals(4, list1.size());

    Page<UserTableDto> userTableDtoPage2 =
        userTablesService.getAllUserTables(userTable, 2, 4, "tableId");
    Assertions.assertEquals(12, userTableDtoPage2.getTotalElements());
    Assertions.assertEquals(3, userTableDtoPage2.getTotalPages());
    List<UserTableDto> list2 = userTableDtoPage2.getContent();
    Assertions.assertEquals(4, list2.size());
  }

  @Test
  public void testUserTableQuery() {
    List<UserTableDto> results = new ArrayList<>();
    results.add(asStored(TEST_TUPLE_1_0.get_userTableDto()));
    results.add(asStored(TEST_TUPLE_2_0.get_userTableDto()));
    results.add(asStored(TEST_TUPLE_3_0.get_userTableDto()));
    results.add(asStored(TEST_TUPLE_4_0.get_userTableDto()));
    results.add(asStored(TEST_USER_TABLE_DTO));

    // No filter, should return all tables.
    List<UserTableDto> actual = userTablesService.getAllUserTables(UserTable.builder().build());
    assertThat(actual.size()).isEqualTo(5);

    // Only specify the database ID to find all tables under this database.
    actual =
        userTablesService.getAllUserTables(
            UserTable.builder().databaseId(TEST_TUPLE_1_0.getDatabaseId()).build());
    assertThat(results).hasSameElementsAs(actual);

    // Specify the database ID and table ID to find matched tables.
    actual =
        userTablesService.getAllUserTables(
            UserTable.builder()
                .databaseId(TEST_TUPLE_1_0.getDatabaseId())
                .tableId("test_table%")
                .build());
    assertThat(results).hasSameElementsAs(actual);

    // Only specify the table Id to find matched tables.
    // Should only have one table matching.
    actual =
        userTablesService.getAllUserTables(
            UserTable.builder().tableId(TEST_TUPLE_2_0.getTableId()).build());
    assertThat(actual.size()).isEqualTo(3);
    assertThat(isUserTableDtoEqual(actual.get(0), asStored(TEST_TUPLE_2_0.get_userTableDto())))
        .isTrue();
  }

  @Test
  public void testUserTableDelete() {
    testUserTableDeleteHelper(TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId());

    // Repeat for case insensitivity, see setup method for source table identifier information
    testUserTableDeleteHelper(CASE_DB_1.toLowerCase(), CASE_TBL_1.toLowerCase());
  }

  private void testUserTableDeleteHelper(String databaseId, String tableId) {
    Assertions.assertDoesNotThrow(
        () -> userTablesService.deleteUserTable(databaseId, tableId, false));
    NoSuchUserTableException noSuchUserTableException =
        Assertions.assertThrows(
            NoSuchUserTableException.class,
            () -> userTablesService.deleteUserTable(databaseId, tableId, false));
    Assertions.assertEquals(noSuchUserTableException.getTableId(), tableId);
    Assertions.assertEquals(noSuchUserTableException.getDatabaseId(), databaseId);
  }

  @Test
  public void testUserTableSoftDelete() {
    UserTable searchByTable =
        UserTable.builder().databaseId(TEST_TUPLE_1_0.getDatabaseId()).build();
    int sizeBeforeSoftDelete = userTablesService.getAllUserTables(searchByTable).size();
    Assertions.assertDoesNotThrow(
        () ->
            userTablesService.deleteUserTable(
                TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId(), true));

    // Cannot double delete the same table by exact ID
    NoSuchUserTableException noSuchUserTableException =
        Assertions.assertThrows(
            NoSuchUserTableException.class,
            () ->
                userTablesService.deleteUserTable(
                    TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId(), true));
    Assertions.assertEquals(noSuchUserTableException.getTableId(), TEST_TUPLE_1_0.getTableId());
    Assertions.assertEquals(
        noSuchUserTableException.getDatabaseId(), TEST_TUPLE_1_0.getDatabaseId());

    Assertions.assertEquals(
        userTablesService.getAllUserTables(searchByTable).size(), sizeBeforeSoftDelete - 1);
    Page<UserTableDto> softDeletedTablePage =
        userTablesService.getAllSoftDeletedTables(searchByTable, 0, 1, null);
    Assertions.assertEquals(1, softDeletedTablePage.getTotalElements());
    Optional<UserTableDto> softDeletedTable = softDeletedTablePage.get().findFirst();
    Assertions.assertTrue(softDeletedTable.isPresent());
    // Validate soft deleted table TTL is correct
    Assertions.assertEquals(
        softDeletedTable.get().getPurgeAfterMs(),
        Instant.ofEpochMilli(softDeletedTable.get().getDeletedAtMs())
            .plus(7, ChronoUnit.DAYS)
            .toEpochMilli());
  }

  @Test
  public void testUserTableUpdate() {
    // testTuple1_0 is one of the table that is created from setup method.
    String modifiedLocation = TEST_TUPLE_1_0.get_userTableRow().getMetadataLocation() + "/new";
    String atVersion = TEST_TUPLE_1_0.get_userTableRow().getMetadataLocation();
    UserTable updated_1_0 =
        UserTable.builder()
            .tableId(TEST_TUPLE_1_0.get_userTableRow().getTableId())
            .databaseId(TEST_TUPLE_1_0.get_userTableRow().getDatabaseId())
            .metadataLocation(modifiedLocation)
            .tableVersion(atVersion)
            .build();
    Pair<UserTableDto, Boolean> result = userTablesService.putUserTable(updated_1_0);
    assertThat(result.getSecond()).isTrue();
    assertThat(result.getFirst().getMetadataLocation()).isEqualTo(modifiedLocation);
    assertThat(result.getFirst().getTableVersion()).isEqualTo(modifiedLocation);
  }

  @Test
  public void testUserTableRename() {
    // testTuple1_0 is one of the table that is created from setup method.
    String newTableName = TEST_TUPLE_1_0.getTableId() + "_newName";
    String newMetadataLocation = TEST_TUPLE_1_0.getTableLoc() + "_new";
    userTablesService.renameUserTable(
        TEST_TUPLE_1_0.getDatabaseId(),
        TEST_TUPLE_1_0.getTableId(),
        TEST_TUPLE_1_0.getDatabaseId(),
        newTableName,
        newMetadataLocation);

    // check if the table is renamed
    UserTableDto result =
        userTablesService.getUserTable(TEST_TUPLE_1_0.getDatabaseId(), newTableName);
    assertThat(result.getTableId()).isEqualTo(newTableName);
    assertThat(result.getDatabaseId()).isEqualTo(TEST_TUPLE_1_0.getDatabaseId());
    assertThat(result.getMetadataLocation()).isEqualTo(newMetadataLocation);

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () ->
            userTablesService.getUserTable(
                TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId()));
  }

  @Test
  public void testUserTableRenameFails() {
    // Ensure that the rename is occurring in the same database
    assertThat(TEST_TUPLE_1_0.getDatabaseId()).isEqualTo(TEST_TUPLE_2_0.getDatabaseId());

    // Expect that the rename will fail as the table already exists
    Assertions.assertThrows(
        AlreadyExistsException.class,
        () -> {
          userTablesService.renameUserTable(
              TEST_TUPLE_1_0.getDatabaseId(),
              TEST_TUPLE_1_0.getTableId(),
              TEST_TUPLE_1_0.getDatabaseId(),
              TEST_TUPLE_2_0.getTableId(),
              TEST_TUPLE_2_0.getTableLoc());
        });

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> {
          userTablesService.getUserTable(TEST_TUPLE_1_0.getDatabaseId(), "no_such_table");
        });
  }

  /**
   * Restore is the second and only other path that writes the column, and it stamps through a
   * mapper expression rather than the service. Seeded legacy so the assertion cannot pass by the
   * value merely surviving: NULL in, TABLE out.
   */
  @Test
  public void testRestoreStampsTheColumnRatherThanWritingNull() {
    seedLegacyRow(ENTITY_TYPE_DB, "restore_stamps");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "restore_stamps")).isEmpty();

    userTablesService.deleteUserTable(ENTITY_TYPE_DB, "restore_stamps", true);
    UserTableDto softDeleted =
        userTablesService
            .getAllSoftDeletedTables(
                UserTable.builder().databaseId(ENTITY_TYPE_DB).tableId("restore_stamps").build(),
                0,
                10,
                null)
            .getContent()
            .get(0);

    UserTableDto restored =
        userTablesService.restoreUserTable(
            ENTITY_TYPE_DB, "restore_stamps", softDeleted.getDeletedAtMs());

    assertThat(restored.getEntityType()).isEqualTo(EntityType.TABLE);
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "restore_stamps")).hasValue("TABLE");
  }

  @Test
  public void testUserTableRestore() {
    UserTable searchByTable =
        UserTable.builder().databaseId(TEST_TUPLE_1_0.getDatabaseId()).build();
    int sizeBeforeSoftDelete = userTablesService.getAllUserTables(searchByTable).size();
    Assertions.assertDoesNotThrow(
        () ->
            userTablesService.deleteUserTable(
                TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId(), true));

    Assertions.assertDoesNotThrow(
        () -> userTablesService.putUserTable(TEST_TUPLE_1_0.get_userTable()));
    Assertions.assertDoesNotThrow(
        () ->
            userTablesService.deleteUserTable(
                TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId(), true));

    // Validate table sizes are expected
    Assertions.assertEquals(
        userTablesService.getAllUserTables(searchByTable).size(), sizeBeforeSoftDelete - 1);
    Page<UserTableDto> softDeletedTablePage =
        userTablesService.getAllSoftDeletedTables(searchByTable, 0, 1, null);
    Assertions.assertEquals(2, softDeletedTablePage.getTotalElements());
    Assertions.assertTrue(softDeletedTablePage.get().findFirst().isPresent());
    UserTableDto softDeletedTable = softDeletedTablePage.get().findFirst().get();
    UserTableDto recoveredUserTable =
        userTablesService.restoreUserTable(
            TEST_TUPLE_1_0.getDatabaseId(),
            TEST_TUPLE_1_0.getTableId(),
            softDeletedTable.getDeletedAtMs());
    Assertions.assertNull(recoveredUserTable.getDeletedAtMs());
    Assertions.assertNull(recoveredUserTable.getPurgeAfterMs());
    Assertions.assertEquals(recoveredUserTable.getTableId(), TEST_TUPLE_1_0.getTableId());
    Assertions.assertEquals(recoveredUserTable.getDatabaseId(), TEST_TUPLE_1_0.getDatabaseId());
    Assertions.assertEquals(
        userTablesService.getAllUserTables(searchByTable).size(), sizeBeforeSoftDelete);
  }

  @Test
  public void testUserTableSoftDeleteIsAtomic() {
    UserTableDto table =
        userTablesService.getUserTable(TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId());
    Assertions.assertNotNull(table);
    doThrow(new RuntimeException("Mocked exception for testing atomicity"))
        .when(htsRepository)
        .deleteTableById(any());

    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            userTablesService.deleteUserTable(
                TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId(), true));

    UserTable searchByTableId =
        UserTable.builder()
            .tableId(TEST_TUPLE_1_0.getTableId())
            .databaseId(TEST_TUPLE_1_0.getDatabaseId())
            .build();
    // Assert that the insertion into soft deleted table is rolled back when a failure occurs
    Assertions.assertEquals(
        0,
        userTablesService.getAllSoftDeletedTables(searchByTableId, 0, 10, null).getTotalElements());
    Assertions.assertDoesNotThrow(
        () ->
            userTablesService.getUserTable(
                TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId()));
  }

  @Test
  public void testUserTableRestoreIsAtomic() {
    UserTableDto table =
        userTablesService.getUserTable(TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId());
    Assertions.assertNotNull(table);
    doThrow(new RuntimeException("Mocked exception for testing atomicity"))
        .when(softDeletedHtsJdbcRepository)
        .deleteById(any());

    Assertions.assertDoesNotThrow(
        () ->
            userTablesService.deleteUserTable(
                TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId(), true));
    // Get the deleted timestamp
    UserTable searchByTableId =
        UserTable.builder()
            .tableId(TEST_TUPLE_1_0.getTableId())
            .databaseId(TEST_TUPLE_1_0.getDatabaseId())
            .build();
    Page<UserTableDto> softDeletedTablePage =
        userTablesService.getAllSoftDeletedTables(searchByTableId, 0, 1, null);
    Assertions.assertEquals(1, softDeletedTablePage.getTotalElements());
    Assertions.assertTrue(softDeletedTablePage.get().findFirst().isPresent());
    UserTableDto softDeletedTable = softDeletedTablePage.get().findFirst().get();

    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            userTablesService.restoreUserTable(
                TEST_TUPLE_1_0.getDatabaseId(),
                TEST_TUPLE_1_0.getTableId(),
                softDeletedTable.getDeletedAtMs()));

    // Assert that soft deleted table is not inserted into the active user tables
    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () ->
            userTablesService.getUserTable(
                TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId()));
    Assertions.assertEquals(
        1,
        userTablesService.getAllSoftDeletedTables(searchByTableId, 0, 10, null).getTotalElements());
  }

  @Test
  public void testUserTableRestoreDoesNotOverwriteExistingTable() {
    UserTableDto table =
        userTablesService.getUserTable(TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId());
    Assertions.assertNotNull(table);
    Assertions.assertDoesNotThrow(
        () ->
            userTablesService.deleteUserTable(
                TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId(), true));
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_1_0.get_userTableRow());
    // Get the deleted timestamp
    UserTable searchByTableId =
        UserTable.builder()
            .tableId(TEST_TUPLE_1_0.getTableId())
            .databaseId(TEST_TUPLE_1_0.getDatabaseId())
            .build();
    Page<UserTableDto> softDeletedTablePage =
        userTablesService.getAllSoftDeletedTables(searchByTableId, 0, 1, null);
    Assertions.assertEquals(1, softDeletedTablePage.getTotalElements());
    Assertions.assertTrue(softDeletedTablePage.get().findFirst().isPresent());
    UserTableDto softDeletedTable = softDeletedTablePage.get().findFirst().get();
    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            userTablesService.restoreUserTable(
                TEST_TUPLE_1_0.getDatabaseId(),
                TEST_TUPLE_1_0.getTableId(),
                softDeletedTable.getDeletedAtMs()));
  }

  @Test
  public void testUserTablePurge() {
    Assertions.assertDoesNotThrow(
        () ->
            userTablesService.deleteUserTable(
                TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId(), true));
    Assertions.assertDoesNotThrow(
        () ->
            userTablesService.deleteUserTable(
                TEST_TUPLE_2_0.getDatabaseId(), TEST_TUPLE_2_0.getTableId(), true));
    userTableRawSeeder.seedLegacyRow(TEST_TUPLE_2_0.get_userTableRow());
    Assertions.assertDoesNotThrow(
        () ->
            userTablesService.deleteUserTable(
                TEST_TUPLE_2_0.getDatabaseId(), TEST_TUPLE_2_0.getTableId(), true));
    // Get the deleted timestamp
    UserTable searchByTableId =
        UserTable.builder()
            .tableId(TEST_TUPLE_2_0.getTableId())
            .databaseId(TEST_TUPLE_2_0.getDatabaseId())
            .build();
    Page<UserTableDto> softDeletedTablePage =
        userTablesService.getAllSoftDeletedTables(searchByTableId, 0, 2, "purgeAfterMs");
    Assertions.assertEquals(2, softDeletedTablePage.getTotalElements());
    List<UserTableDto> softDeletedTables_2_0 =
        softDeletedTablePage.get().collect(Collectors.toList());
    Assertions.assertTrue(
        softDeletedTables_2_0.get(0).getPurgeAfterMs()
            < softDeletedTables_2_0.get(1).getPurgeAfterMs());
    Assertions.assertDoesNotThrow(
        () ->
            userTablesService.purgeSoftDeletedUserTables(
                TEST_TUPLE_2_0.getDatabaseId(),
                TEST_TUPLE_2_0.getTableId(),
                softDeletedTables_2_0.get(0).getPurgeAfterMs() + 1));

    // Validate the row is deleted
    Assertions.assertEquals(
        1,
        userTablesService.getAllSoftDeletedTables(searchByTableId, 0, 10, null).getTotalElements());

    // Delete all
    Assertions.assertDoesNotThrow(
        () ->
            userTablesService.purgeSoftDeletedUserTables(
                TEST_TUPLE_2_0.getDatabaseId(), TEST_TUPLE_2_0.getTableId(), null));
    Assertions.assertEquals(
        0,
        userTablesService.getAllSoftDeletedTables(searchByTableId, 0, 10, null).getTotalElements());
  }

  /**
   * The DTO a stored tuple reads back as: HTS answers with the metadata location as the version,
   * and a stored null entity type is a legacy table.
   */
  private static UserTableDto asStored(UserTableDto dto) {
    return dto.toBuilder()
        .tableVersion(dto.getMetadataLocation())
        .entityType(EntityType.TABLE)
        .build();
  }

  private Boolean isUserTableDtoEqual(UserTableDto expected, UserTableDto actual) {
    return expected
        .toBuilder()
        .tableVersion("")
        .build()
        .equals(actual.toBuilder().tableVersion("").build());
  }

  // ---------------------------------------------------------------------------------------------
  // entityType discriminator at the service call sites
  // ---------------------------------------------------------------------------------------------

  /**
   * Canonical interleaved fixture. Seeded into its own database so it never perturbs the
   * pre-existing per-database counts asserted by the tests above.
   */
  private static final String ENTITY_TYPE_DB = "entity_type_db";

  private static final List<String> CANONICAL_TABLE_IDS =
      Arrays.asList("t00_legacy", "t02_explicit", "t04_legacy", "t06_explicit");

  private static final List<String> CANONICAL_VIEW_IDS =
      Arrays.asList("t01_view", "t03_view", "t05_view");

  /**
   * {@code getUserTable} is the single HTS endpoint behind every table point read in the tables
   * service, so filtering it here is what makes doRefresh, dropTable, the rename source and
   * findTableRefById all treat a view as absent without a check of their own.
   */
  @Test
  public void testGetUserTableHidesNonTableRows() {
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "point_read", EntityType.VIEW));

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> userTablesService.getUserTable(ENTITY_TYPE_DB, "point_read"));

    // Hidden from the table read, not deleted.
    assertThat(
            htsRepository
                .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(ENTITY_TYPE_DB, "point_read")
                .isPresent())
        .isTrue();
  }

  /** Regression: a legacy row with a SQL NULL discriminator still reads as a table. */
  @Test
  public void testGetUserTableResolvesLegacyRowAsTable() {
    seedLegacyRow(ENTITY_TYPE_DB, "point_read");

    expectPointReadResolvesAsTable();
  }

  /** Regression: an explicitly typed table row reads as the same thing. */
  @Test
  public void testGetUserTableResolvesTypedTableRow() {
    seedTypedRow(ENTITY_TYPE_DB, "point_read", EntityType.TABLE);

    expectPointReadResolvesAsTable();
  }

  private void expectPointReadResolvesAsTable() {
    UserTableDto dto = userTablesService.getUserTable(ENTITY_TYPE_DB, "point_read");
    assertThat(dto.getTableId()).isEqualTo("point_read");
    // A stored null is a legacy table, so the DTO carries a type either way.
    assertThat(dto.getEntityType()).isEqualTo(EntityType.TABLE);
  }

  /**
   * Writers must still see a view at a shared key, or a table create would act on a name another
   * entity holds. Seeing it is not licence to remove it: the table-scoped delete refuses.
   */
  @Test
  public void testWritersStillSeeNonTableRowsAtTheSameKey() {
    seedTypedRow(ENTITY_TYPE_DB, "shared_key", EntityType.VIEW);

    UserTableRow seenByWriter =
        htsRepository
            .findById(
                UserTableRowPrimaryKey.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId("shared_key")
                    .build())
            .orElseThrow(() -> new AssertionError("writer read must see the view row"));
    assertThat(seenByWriter.getEntityType()).isEqualTo(EntityType.VIEW);

    // deleteUserTable is table-scoped, so a view at the key is absent as far as it is concerned.
    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> userTablesService.deleteUserTable(ENTITY_TYPE_DB, "shared_key", false));
    assertThat(
            htsRepository
                .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(ENTITY_TYPE_DB, "shared_key")
                .isPresent())
        .isTrue();
    assertThat(findRow(ENTITY_TYPE_DB, "shared_key").getEntityType()).isEqualTo(EntityType.VIEW);
  }

  private UserTableRow entityTypeRow(String databaseId, String tableId, EntityType entityType) {
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

  /** The enum-typed entity cannot express a non-canonical spelling; only the column can. */
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

  /** Nullable column: an absent value is a real outcome, so callers state which they expect. */
  private Optional<String> readRawEntityType(String databaseId, String tableId) {
    return Optional.ofNullable(
        new JdbcTemplate(dataSource)
            .queryForObject(
                "SELECT entity_type FROM user_table_row WHERE database_id = ? AND table_id = ?",
                String.class,
                databaseId,
                tableId));
  }

  /**
   * Plants a pre-discriminator row, whose column holds SQL NULL. Separate from {@link
   * #seedTypedRow} rather than one helper with a nullable argument: these are two fixtures, not two
   * modes.
   */
  private void seedLegacyRow(String databaseId, String tableId) {
    insertRawEntityType(databaseId, tableId, null);
  }

  /** Plants a typed row through JPA, so the enum boundary is still under test. */
  private void seedTypedRow(String databaseId, String tableId, EntityType entityType) {
    htsRepository.save(entityTypeRow(databaseId, tableId, entityType));
  }

  private void seedCanonicalRows(String prefix) {
    seedLegacyRow(ENTITY_TYPE_DB, prefix + "t00_legacy");
    seedTypedRow(ENTITY_TYPE_DB, prefix + "t01_view", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, prefix + "t02_explicit", EntityType.TABLE);
    seedTypedRow(ENTITY_TYPE_DB, prefix + "t03_view", EntityType.VIEW);
    seedLegacyRow(ENTITY_TYPE_DB, prefix + "t04_legacy");
    seedTypedRow(ENTITY_TYPE_DB, prefix + "t05_view", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, prefix + "t06_explicit", EntityType.TABLE);
  }

  private static List<String> sortedIds(List<UserTableDto> dtos) {
    return dtos.stream().map(UserTableDto::getTableId).sorted().collect(Collectors.toList());
  }

  private static List<String> pageIds(Page<UserTableDto> page) {
    return page.getContent().stream().map(UserTableDto::getTableId).collect(Collectors.toList());
  }

  /** Plain per-database listing through the service hides views and keeps legacy NULL rows. */
  @Test
  public void testListTablesCallSiteFiltersViewsAndKeepsNullRows() {
    seedCanonicalRows("");

    List<UserTableDto> result =
        userTablesService.getAllUserTables(UserTable.builder().databaseId(ENTITY_TYPE_DB).build());

    assertThat(sortedIds(result)).isEqualTo(CANONICAL_TABLE_IDS);
  }

  /**
   * Anti-post-filter assertion at the service layer: a fetch-then-filter implementation yields a
   * 1-row page 0 with totalElements=7/totalPages=4.
   */
  @Test
  public void testListTablesCallSiteFiltersBeforePagination() {
    seedCanonicalRows("");
    UserTable searchBy = UserTable.builder().databaseId(ENTITY_TYPE_DB).build();

    Page<UserTableDto> page0 = userTablesService.getAllUserTables(searchBy, 0, 2, "tableId");
    Assertions.assertEquals(4, page0.getTotalElements());
    Assertions.assertEquals(2, page0.getTotalPages());
    Assertions.assertEquals(2, page0.getContent().size());
    assertThat(pageIds(page0)).containsExactly("t00_legacy", "t02_explicit");

    Page<UserTableDto> page1 = userTablesService.getAllUserTables(searchBy, 1, 2, "tableId");
    Assertions.assertEquals(4, page1.getTotalElements());
    Assertions.assertEquals(2, page1.getTotalPages());
    Assertions.assertEquals(2, page1.getContent().size());
    assertThat(pageIds(page1)).containsExactly("t04_legacy", "t06_explicit");

    assertThat(pageIds(page0)).doesNotContainAnyElementsOf(CANONICAL_VIEW_IDS);
    assertThat(pageIds(page1)).doesNotContainAnyElementsOf(CANONICAL_VIEW_IDS);
  }

  /** The pattern-listing call sites (plain and paged) apply the same predicate. */
  @Test
  public void testPatternCallSitesFilterViewsPlainAndPaged() {
    seedCanonicalRows("match_");
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "nomatch_table", EntityType.TABLE));
    UserTable searchBy = UserTable.builder().databaseId(ENTITY_TYPE_DB).tableId("match_%").build();

    assertThat(sortedIds(userTablesService.getAllUserTables(searchBy)))
        .containsExactly(
            "match_t00_legacy", "match_t02_explicit", "match_t04_legacy", "match_t06_explicit");

    Page<UserTableDto> page0 = userTablesService.getAllUserTables(searchBy, 0, 2, "tableId");
    Assertions.assertEquals(4, page0.getTotalElements());
    Assertions.assertEquals(2, page0.getTotalPages());
    assertThat(pageIds(page0)).containsExactly("match_t00_legacy", "match_t02_explicit");

    Page<UserTableDto> page1 = userTablesService.getAllUserTables(searchBy, 1, 2, "tableId");
    Assertions.assertEquals(4, page1.getTotalElements());
    Assertions.assertEquals(2, page1.getTotalPages());
    assertThat(pageIds(page1)).containsExactly("match_t04_legacy", "match_t06_explicit");
  }

  /** Regression: {@code entityType} is bound onto the request but never reaches a predicate. */
  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"VIEW", "view", "TABLE", "TaBlE"})
  public void testEntityTypeOnQueryIsIgnoredAndAlwaysReturnsTables(String entityType) {
    seedCanonicalRows("");

    assertThat(
            sortedIds(
                userTablesService.getAllUserTables(
                    UserTable.builder().databaseId(ENTITY_TYPE_DB).entityType(entityType).build())))
        .as("entityType=%s must still resolve to the four visible tables", entityType)
        .isEqualTo(CANONICAL_TABLE_IDS);
  }

  /** Regression: the paged table call site is equally type-blind. */
  @Test
  public void testEntityTypeOnPagedQueryIsIgnoredAndAlwaysReturnsTables() {
    seedCanonicalRows("");

    Page<UserTableDto> page0 =
        userTablesService.getAllUserTables(
            UserTable.builder().databaseId(ENTITY_TYPE_DB).entityType("VIEW").build(),
            0,
            2,
            "tableId");
    Assertions.assertEquals(4, page0.getTotalElements());
    Assertions.assertEquals(2, page0.getTotalPages());
    assertThat(pageIds(page0)).containsExactly("t00_legacy", "t02_explicit");
  }

  /**
   * Defense in depth for the shared key space: if a rename ever reaches the HTS storage layer with
   * an occupied destination, the primary-key violation must roll back cleanly and leave BOTH JPA
   * rows byte-identical — same numeric {@code version}, {@code metadataLocation} and {@code
   * entityType}. The table-service tests prove correct code never reaches this fallback; this pins
   * that the fallback itself is non-mutating.
   */
  @Test
  public void testRenameCollisionLeavesJPARowsUnchanged() {
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "rename_src_table", EntityType.TABLE));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "rename_dst_view", EntityType.VIEW));

    UserTableRow sourceBefore = findRow(ENTITY_TYPE_DB, "rename_src_table");
    UserTableRow destinationBefore = findRow(ENTITY_TYPE_DB, "rename_dst_view");

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            userTablesService.renameUserTable(
                ENTITY_TYPE_DB,
                "rename_src_table",
                ENTITY_TYPE_DB,
                "rename_dst_view",
                "/openhouse/entity_type_db/rename_dst_view/v1_metadata.json"));

    UserTableRow sourceAfter = findRow(ENTITY_TYPE_DB, "rename_src_table");
    UserTableRow destinationAfter = findRow(ENTITY_TYPE_DB, "rename_dst_view");

    Assertions.assertEquals(sourceBefore.getVersion(), sourceAfter.getVersion());
    Assertions.assertEquals(sourceBefore.getMetadataLocation(), sourceAfter.getMetadataLocation());
    Assertions.assertEquals(sourceBefore.getEntityType(), sourceAfter.getEntityType());

    Assertions.assertEquals(destinationBefore.getVersion(), destinationAfter.getVersion());
    Assertions.assertEquals(
        destinationBefore.getMetadataLocation(), destinationAfter.getMetadataLocation());
    Assertions.assertEquals(EntityType.VIEW, destinationAfter.getEntityType());
  }

  // ---------------------------------------------------------------------------------------------
  // neutral entity read
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testGetNeutralEntityReportsCanonicalTypeForEitherType() {
    seedTypedRow(ENTITY_TYPE_DB, "neutral_view", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, "neutral_table", EntityType.TABLE);
    seedLegacyRow(ENTITY_TYPE_DB, "neutral_legacy");

    assertThat(userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_view"))
        .hasValueSatisfying(dto -> assertThat(dto.getEntityType()).isEqualTo(EntityType.VIEW));
    assertThat(userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_table"))
        .hasValueSatisfying(dto -> assertThat(dto.getEntityType()).isEqualTo(EntityType.TABLE));
    assertThat(userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_legacy"))
        .hasValueSatisfying(dto -> assertThat(dto.getEntityType()).isEqualTo(EntityType.TABLE));

    // The stored null is reported as TABLE without being rewritten.
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "neutral_legacy")).isEmpty();

    // The key itself resolves case-insensitively, like every other point read.
    assertThat(userTablesService.getNeutralEntity(ENTITY_TYPE_DB.toUpperCase(), "NEUTRAL_VIEW"))
        .hasValueSatisfying(dto -> assertThat(dto.getEntityType()).isEqualTo(EntityType.VIEW));
  }

  /** The stored spelling is resolved in Java rather than left to the database collation. */
  @ParameterizedTest
  @CsvSource({
    "spell_lower_view,  view,  VIEW",
    "spell_mixed_view,  ViEw,  VIEW",
    "spell_lower_table, table, TABLE",
    "spell_mixed_table, TaBlE, TABLE"
  })
  public void testGetNeutralEntityResolvesEveryStoredSpelling(
      String tableId, String storedSpelling, EntityType expected) {
    insertRawEntityType(ENTITY_TYPE_DB, tableId, storedSpelling);

    assertThat(userTablesService.getNeutralEntity(ENTITY_TYPE_DB, tableId))
        .hasValueSatisfying(dto -> assertThat(dto.getEntityType()).isEqualTo(expected));
    assertThat(readRawEntityType(ENTITY_TYPE_DB, tableId)).hasValue(storedSpelling);
  }

  @Test
  public void testGetNeutralEntityMissingKeyIsEmpty() {
    assertThat(userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_absent")).isEmpty();
  }

  /** "The name is free" is the dangerous default: a failure here must not read as absence. */
  @Test
  public void testGetNeutralEntityPropagatesRepositoryFailure() {
    DataAccessResourceFailureException raw = new DataAccessResourceFailureException("injected");
    doThrow(raw)
        .when(htsRepository)
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(anyString(), anyString());

    assertThatThrownBy(() -> userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_boom"))
        .isSameAs(raw);
  }

  /**
   * A real row is seeded first, so the stubbed empty result is the only possible source of the
   * answer — and the stub intercepts only if the neutral read is what the service calls.
   */
  @Test
  public void testGetNeutralEntityOnlyEmptyOptionalBecomesAbsence() {
    seedTypedRow(ENTITY_TYPE_DB, "neutral_stubbed", EntityType.VIEW);
    doReturn(Optional.empty())
        .when(htsRepository)
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(anyString(), anyString());

    assertThat(userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_stubbed")).isEmpty();

    Mockito.verify(htsRepository)
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(ENTITY_TYPE_DB, "neutral_stubbed");
  }

  /** Storage corruption is a server-state failure whatever wrote it, not a bad request. */
  @Test
  public void testGetNeutralEntityAtCorruptKeyFailsLoudly() {
    insertRawEntityType(ENTITY_TYPE_DB, "neutral_corrupt", "UNKNOWN");

    assertThatThrownBy(() -> userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_corrupt"))
        .isInstanceOf(CorruptEntityTypeException.class)
        .hasStackTraceContaining("user_table_row.entity_type")
        .hasStackTraceContaining("UNKNOWN");

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "neutral_corrupt")).hasValue("UNKNOWN");
  }

  // ---------------------------------------------------------------------------------------------
  // view reads
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testGetUserViewHidesTablesAndLegacyNullRows() {
    seedTypedRow(ENTITY_TYPE_DB, "view_point", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, "table_point", EntityType.TABLE);
    seedLegacyRow(ENTITY_TYPE_DB, "legacy_point");

    assertThat(userTablesService.getUserView(ENTITY_TYPE_DB, "view_point"))
        .hasValueSatisfying(
            view -> {
              assertThat(view.getTableId()).isEqualTo("view_point");
              assertThat(view.getEntityType()).isEqualTo(EntityType.VIEW);
            });

    assertThat(userTablesService.getUserView(ENTITY_TYPE_DB, "table_point")).isEmpty();
    assertThat(userTablesService.getUserView(ENTITY_TYPE_DB, "legacy_point")).isEmpty();
    assertThat(userTablesService.getUserView(ENTITY_TYPE_DB, "absent_point")).isEmpty();

    // Hidden from the view read, not deleted.
    assertThat(findRow(ENTITY_TYPE_DB, "table_point").getEntityType()).isEqualTo(EntityType.TABLE);
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "legacy_point")).isEmpty();
  }

  /**
   * Absence, not failure, is correct here and not a softened expectation: {@code
   * VIEW_ROW_PREDICATE} excludes the row before hydration is attempted. Corruption can only reach a
   * view read under a folding collation, simulated in {@code JpaUserTableReadRepositoryTest}.
   */
  @Test
  public void testGetUserViewAtCorruptKeyIsAbsentBecauseThePredicateExcludesIt() {
    insertRawEntityType(ENTITY_TYPE_DB, "view_corrupt", "UNKNOWN");

    assertThat(userTablesService.getUserView(ENTITY_TYPE_DB, "view_corrupt")).isEmpty();

    // Excluded, not deleted, and not rewritten.
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "view_corrupt")).hasValue("UNKNOWN");
  }

  /** The neutral read has no type predicate, so it does select the row and must fail on it. */
  @Test
  public void testNeutralReadIsThePathThatSurfacesCorruption() {
    insertRawEntityType(ENTITY_TYPE_DB, "view_corrupt", "UNKNOWN");

    assertThatThrownBy(() -> userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "view_corrupt"))
        .isInstanceOf(CorruptEntityTypeException.class);
  }

  @Test
  public void testGetAllUserViewsWithEmptyQueryReturnsEveryView() {
    seedCanonicalRows("");

    List<UserTableDto> views = userTablesService.getAllUserViews(UserViewQuery.all());

    assertThat(sortedIds(views)).isEqualTo(CANONICAL_VIEW_IDS);
    // Not a database-name projection: every result is a fully identified view.
    assertThat(views).allSatisfy(v -> assertThat(v.getTableId()).isNotNull());
    assertThat(views).allSatisfy(v -> assertThat(v.getEntityType()).isEqualTo(EntityType.VIEW));

    Page<UserTableDto> page =
        userTablesService.getAllUserViews(UserViewQuery.all(), 0, 50, "tableId");
    Assertions.assertEquals(3, page.getTotalElements());
    assertThat(pageIds(page)).containsExactlyElementsOf(CANONICAL_VIEW_IDS);
  }

  @Test
  public void testGetAllUserViewsFiltersBeforePagination() {
    seedCanonicalRows("");
    UserViewQuery byDatabase = UserViewQuery.inDatabase(ENTITY_TYPE_DB);

    assertThat(sortedIds(userTablesService.getAllUserViews(byDatabase)))
        .isEqualTo(CANONICAL_VIEW_IDS);

    Page<UserTableDto> page0 = userTablesService.getAllUserViews(byDatabase, 0, 2, "tableId");
    Assertions.assertEquals(3, page0.getTotalElements());
    Assertions.assertEquals(2, page0.getTotalPages());
    Assertions.assertEquals(2, page0.getContent().size());
    assertThat(pageIds(page0)).containsExactly("t01_view", "t03_view");

    Page<UserTableDto> page1 = userTablesService.getAllUserViews(byDatabase, 1, 2, "tableId");
    Assertions.assertEquals(3, page1.getTotalElements());
    assertThat(pageIds(page1)).containsExactly("t05_view");

    assertThat(pageIds(page0)).doesNotContainAnyElementsOf(CANONICAL_TABLE_IDS);
    assertThat(pageIds(page1)).doesNotContainAnyElementsOf(CANONICAL_TABLE_IDS);
  }

  @Test
  public void testGetAllUserViewsWithPatternFiltersViews() {
    seedCanonicalRows("match_");
    seedTypedRow(ENTITY_TYPE_DB, "nomatch_view", EntityType.VIEW);
    UserViewQuery byPattern = UserViewQuery.matchingPattern(ENTITY_TYPE_DB, "match_%");

    assertThat(sortedIds(userTablesService.getAllUserViews(byPattern)))
        .containsExactly("match_t01_view", "match_t03_view", "match_t05_view");

    Page<UserTableDto> page0 = userTablesService.getAllUserViews(byPattern, 0, 2, "tableId");
    Assertions.assertEquals(3, page0.getTotalElements());
    Assertions.assertEquals(2, page0.getTotalPages());
    assertThat(pageIds(page0)).containsExactly("match_t01_view", "match_t03_view");
  }

  /**
   * Regression guard: {@code _} is a single-character wildcard, so {@code match_%} also matches
   * {@code matchXview}. Every other seeded id has a literal underscore there, so only a
   * differently-spelled row can demonstrate it. Pre-existing behaviour, pinned rather than
   * endorsed.
   */
  @Test
  public void testViewPatternQueryKeepsUnderscoreAsASqlWildcard() {
    seedTypedRow(ENTITY_TYPE_DB, "match_t01_view", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, "matchXview", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, "nomatchview", EntityType.VIEW);

    assertThat(
            sortedIds(
                userTablesService.getAllUserViews(
                    UserViewQuery.matchingPattern(ENTITY_TYPE_DB, "match_%"))))
        .as("_ matches any single character, so matchXview is included")
        .containsExactly("matchXview", "match_t01_view");
  }

  @Test
  public void testPagedViewQueryWithoutSortStillPages() {
    seedCanonicalRows("");

    Page<UserTableDto> page0 =
        userTablesService.getAllUserViews(UserViewQuery.inDatabase(ENTITY_TYPE_DB), 0, 2, null);

    Assertions.assertEquals(3, page0.getTotalElements());
    Assertions.assertEquals(2, page0.getContent().size());
  }

  /**
   * The query type has no field an {@code entityType} could arrive in, and no branch to re-route.
   */
  @Test
  public void testOwnedQueryTypeCannotCarryAnEntityTypeOrANonKeyFilter() {
    assertThat(
            Arrays.stream(UserViewQuery.class.getDeclaredFields())
                .filter(field -> !field.isSynthetic())
                .map(java.lang.reflect.Field::getName))
        .containsExactlyInAnyOrder("databaseId", "tableIdPattern");
    // The state the validator rejects is also unconstructible here.
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> UserViewQuery.matchingPattern(null, "t0%"));
  }

  @Test
  public void testViewListAndPageMetricsAreReported() {
    seedCanonicalRows("");
    UserViewQuery byDatabase = UserViewQuery.inDatabase(ENTITY_TYPE_DB);

    assertMetricsAdvance(
        UserTableMetricsConstant.HTS_LIST_VIEWS_REQUEST,
        UserTableMetricsConstant.HTS_LIST_VIEWS_TIME,
        () -> userTablesService.getAllUserViews(byDatabase));

    assertMetricsAdvance(
        UserTableMetricsConstant.HTS_PAGE_VIEWS_REQUEST,
        UserTableMetricsConstant.HTS_PAGE_VIEWS_TIME,
        () -> userTablesService.getAllUserViews(byDatabase, 0, 2, "tableId"));

    // The pattern forms share the same names as their database-scoped siblings.
    UserViewQuery byPattern = UserViewQuery.matchingPattern(ENTITY_TYPE_DB, "t0%");
    assertMetricsAdvance(
        UserTableMetricsConstant.HTS_LIST_VIEWS_REQUEST,
        UserTableMetricsConstant.HTS_LIST_VIEWS_TIME,
        () -> userTablesService.getAllUserViews(byPattern));
  }

  // ---------------------------------------------------------------------------------------------
  // type-scoped writes and deletes
  // ---------------------------------------------------------------------------------------------

  /** A view routed through the soft-delete primitive would restore as a table. */
  @Test
  public void testDeleteUserViewIsHardAndCreatesNoSoftDeletedRow() {
    seedTypedRow(ENTITY_TYPE_DB, "drop_view", EntityType.VIEW);
    UserTable searchByKey =
        UserTable.builder().databaseId(ENTITY_TYPE_DB).tableId("drop_view").build();

    Assertions.assertTrue(userTablesService.deleteUserView(ENTITY_TYPE_DB, "drop_view"));

    assertThat(
            htsRepository
                .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(ENTITY_TYPE_DB, "drop_view")
                .isPresent())
        .isFalse();
    Assertions.assertEquals(
        0, userTablesService.getAllSoftDeletedTables(searchByKey, 0, 10, null).getTotalElements());

    // Dropping it again removed nothing, which the handler reports as not-found.
    Assertions.assertFalse(userTablesService.deleteUserView(ENTITY_TYPE_DB, "drop_view"));
  }

  @Test
  public void testDeleteUserViewAtTableKeyReportsFailureAndRetainsTheTable() {
    seedTypedRow(ENTITY_TYPE_DB, "drop_table", EntityType.TABLE);
    seedLegacyRow(ENTITY_TYPE_DB, "drop_legacy");

    Assertions.assertFalse(userTablesService.deleteUserView(ENTITY_TYPE_DB, "drop_table"));
    Assertions.assertFalse(userTablesService.deleteUserView(ENTITY_TYPE_DB, "drop_legacy"));

    assertThat(findRow(ENTITY_TYPE_DB, "drop_table").getEntityType()).isEqualTo(EntityType.TABLE);
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "drop_legacy")).isEmpty();
  }

  @Test
  public void testDeleteUserViewNeverReadsOrWritesTheSoftDeletedStore() {
    seedTypedRow(ENTITY_TYPE_DB, "drop_view", EntityType.VIEW);

    userTablesService.deleteUserView(ENTITY_TYPE_DB, "drop_view");

    Mockito.verifyNoInteractions(softDeletedHtsJdbcRepository);
  }

  @Test
  public void testSoftDeleteAtViewKeyIsNotFoundAndCreatesNoSoftRow() {
    seedTypedRow(ENTITY_TYPE_DB, "soft_view", EntityType.VIEW);
    UserTable searchByKey =
        UserTable.builder().databaseId(ENTITY_TYPE_DB).tableId("soft_view").build();

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> userTablesService.deleteUserTable(ENTITY_TYPE_DB, "soft_view", true));

    Assertions.assertEquals(
        0, userTablesService.getAllSoftDeletedTables(searchByKey, 0, 10, null).getTotalElements());
    assertThat(findRow(ENTITY_TYPE_DB, "soft_view").getEntityType()).isEqualTo(EntityType.VIEW);
  }

  /** A view must never be copied into a store with no column to record what it was. */
  @Test
  public void testSoftDeleteReadsItsArchiveSourceThroughTheTableFinder() {
    userTablesService.deleteUserTable(
        TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId(), true);

    Mockito.verify(htsRepository)
        .findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
            TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId());
    Mockito.verify(htsRepository, Mockito.never()).findById(any());
  }

  @Test
  public void testTypedDeletesAtCorruptKeyAreNotFoundAndRetainTheRow() {
    insertRawEntityType(ENTITY_TYPE_DB, "delete_corrupt", "UNKNOWN");

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> userTablesService.deleteUserTable(ENTITY_TYPE_DB, "delete_corrupt", false));
    Assertions.assertFalse(userTablesService.deleteUserView(ENTITY_TYPE_DB, "delete_corrupt"));

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "delete_corrupt")).hasValue("UNKNOWN");
  }

  /**
   * The request carries a STALE version deliberately: a version-first implementation would answer
   * {@link EntityConcurrentModificationException}, so only a type-first one passes. That is what
   * makes this an ordering test rather than a restatement of the collision.
   */
  @Test
  public void testTypeCollisionIsDecidedBeforeVersionMapping() {
    seedTypedRow(ENTITY_TYPE_DB, "guard_view", EntityType.VIEW);
    UserTableRow before = findRow(ENTITY_TYPE_DB, "guard_view");

    AlreadyExistsException thrown =
        Assertions.assertThrows(
            AlreadyExistsException.class,
            () ->
                userTablesService.putUserTable(
                    UserTable.builder()
                        .databaseId(ENTITY_TYPE_DB)
                        .tableId("guard_view")
                        .tableVersion("/openhouse/entity_type_db/guard_view/stale_metadata.json")
                        .metadataLocation("/openhouse/entity_type_db/guard_view/v1_metadata.json")
                        .build()));
    // The occupant is what the conflict names, not the type that was requested.
    assertThat(thrown.getMessage()).contains("VIEW");
    assertThat(thrown.getMessage()).contains(ENTITY_TYPE_DB + ".guard_view");

    UserTableRow after = findRow(ENTITY_TYPE_DB, "guard_view");
    assertThat(after.getEntityType()).isEqualTo(EntityType.VIEW);
    assertThat(after.getVersion()).isEqualTo(before.getVersion());
    assertThat(after.getMetadataLocation()).isEqualTo(before.getMetadataLocation());
  }

  /**
   * View mutations expose a raw {@code DataAccessException}, exactly as table mutations always
   * have. Consistency between the two is the point; neither is wrapped.
   */
  @Test
  public void testViewAndTableMutationsBothExposeTheRawFailure() {
    DataAccessResourceFailureException raw =
        new DataAccessResourceFailureException("datasource down");
    doThrow(raw).when(htsRepository).deleteViewById(any());
    doThrow(raw).when(htsRepository).deleteTableById(any());

    assertThatThrownBy(() -> userTablesService.deleteUserView(ENTITY_TYPE_DB, "drop_when_down"))
        .isSameAs(raw);
    assertThatThrownBy(
            () -> userTablesService.deleteUserTable(ENTITY_TYPE_DB, "drop_when_down", false))
        .isSameAs(raw);
  }

  /** The write races still answer 409 rather than escaping as an infrastructure failure. */
  @Test
  public void testViewPutStillAnswersTheConflictOutcomes() {
    seedTypedRow(ENTITY_TYPE_DB, "conflict_view", EntityType.VIEW);
    UserTableRow before = findRow(ENTITY_TYPE_DB, "conflict_view");

    // Stale version at the same type: still a concurrency conflict.
    Assertions.assertThrows(
        EntityConcurrentModificationException.class,
        () ->
            userTablesService.putUserView(
                UserTable.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId("conflict_view")
                    .tableVersion("/openhouse/entity_type_db/conflict_view/stale_metadata.json")
                    .metadataLocation("/openhouse/entity_type_db/conflict_view/v1_metadata.json")
                    .build()));

    // Cross-type occupancy: still the occupancy conflict, naming the occupant.
    seedTypedRow(ENTITY_TYPE_DB, "conflict_table", EntityType.TABLE);
    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            userTablesService.putUserView(
                UserTable.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId("conflict_table")
                    .tableVersion(findRow(ENTITY_TYPE_DB, "conflict_table").getMetadataLocation())
                    .metadataLocation("/openhouse/entity_type_db/conflict_table/v1_metadata.json")
                    .build()));

    assertThat(findRow(ENTITY_TYPE_DB, "conflict_view").getMetadataLocation())
        .isEqualTo(before.getMetadataLocation());
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "conflict_table")).hasValue("TABLE");
  }

  @Test
  public void testViewDeleteTranslationDoesNotTurnAbsenceIntoAFailure() {
    Assertions.assertFalse(userTablesService.deleteUserView(ENTITY_TYPE_DB, "never_existed"));
  }

  /** The same answer at the current version, so the type is the only thing being rejected. */
  @Test
  public void testTablePutAtViewKeyIsAlreadyExistsAndLeavesTheViewUnchanged() {
    seedTypedRow(ENTITY_TYPE_DB, "guard_view", EntityType.VIEW);
    UserTableRow before = findRow(ENTITY_TYPE_DB, "guard_view");

    AlreadyExistsException thrown =
        Assertions.assertThrows(
            AlreadyExistsException.class,
            () ->
                userTablesService.putUserTable(
                    UserTable.builder()
                        .databaseId(ENTITY_TYPE_DB)
                        .tableId("guard_view")
                        .tableVersion(before.getMetadataLocation())
                        .metadataLocation("/openhouse/entity_type_db/guard_view/v1_metadata.json")
                        .build()));
    assertThat(thrown.getMessage()).contains("VIEW");

    UserTableRow after = findRow(ENTITY_TYPE_DB, "guard_view");
    assertThat(after.getEntityType()).isEqualTo(EntityType.VIEW);
    assertThat(after.getVersion()).isEqualTo(before.getVersion());
    assertThat(after.getMetadataLocation()).isEqualTo(before.getMetadataLocation());
  }

  /**
   * Regression guard: both racers legitimately read the key as free, so the shared primary key is
   * the only arbiter the application-level guard cannot replace. The race is simulated by pinning
   * the loser's occupancy read to the empty result it would have taken. Do not delete as redundant.
   */
  @Test
  public void testConcurrentCrossTypeFirstCreatesLeaveOneWinnerAndA409Loser() {
    Pair<UserTableDto, Boolean> winner =
        userTablesService.putUserTable(
            UserTable.builder()
                .databaseId(ENTITY_TYPE_DB)
                .tableId("race_key")
                .tableVersion(INITIAL_TABLE_VERSION)
                .metadataLocation("/openhouse/entity_type_db/race_key/v0_table_metadata.json")
                .build());
    assertThat(winner.getSecond()).as("the winner creates rather than updates").isFalse();
    assertThat(winner.getFirst().getEntityType()).isEqualTo(EntityType.TABLE);

    // The loser's occupancy read was taken before the winner committed.
    doReturn(Optional.empty())
        .when(userTableReadRepository)
        .findRowForWrite(anyString(), anyString());

    Assertions.assertThrows(
        EntityConcurrentModificationException.class,
        () ->
            userTablesService.putUserView(
                UserTable.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId("race_key")
                    .tableVersion(INITIAL_TABLE_VERSION)
                    .metadataLocation("/openhouse/entity_type_db/race_key/v0_view_metadata.json")
                    .build()));

    // Exactly one row, and it is the winner's, untouched.
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "race_key")).hasValue("TABLE");
    Assertions.assertEquals(
        "/openhouse/entity_type_db/race_key/v0_table_metadata.json",
        new JdbcTemplate(dataSource)
            .queryForObject(
                "SELECT metadata_location FROM user_table_row "
                    + "WHERE database_id = ? AND table_id = ?",
                String.class,
                ENTITY_TYPE_DB,
                "race_key"));
  }

  /** The symmetric direction, including the legacy-null occupant. */
  @Test
  public void testViewPutAtTableKeyIsAlreadyExistsAndLeavesTheTableUnchanged() {
    seedTypedRow(ENTITY_TYPE_DB, "guard_table", EntityType.TABLE);
    seedLegacyRow(ENTITY_TYPE_DB, "guard_legacy");
    UserTableRow tableBefore = findRow(ENTITY_TYPE_DB, "guard_table");
    UserTableRow legacyBefore = findRow(ENTITY_TYPE_DB, "guard_legacy");

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            userTablesService.putUserView(
                UserTable.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId("guard_table")
                    .tableVersion(tableBefore.getMetadataLocation())
                    .metadataLocation("/openhouse/entity_type_db/guard_table/v1_metadata.json")
                    .build()));

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            userTablesService.putUserView(
                UserTable.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId("guard_legacy")
                    .tableVersion(legacyBefore.getMetadataLocation())
                    .metadataLocation("/openhouse/entity_type_db/guard_legacy/v1_metadata.json")
                    .build()));

    assertThat(findRow(ENTITY_TYPE_DB, "guard_table").getMetadataLocation())
        .isEqualTo(tableBefore.getMetadataLocation());
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "guard_table")).hasValue("TABLE");
    // A rejected write must not migrate the legacy occupant either.
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "guard_legacy")).isEmpty();
  }

  /**
   * The named entry point owns the type: the discriminator is overwritten before the mapper's enum
   * conversion, so no transport value governs storage. Over HTTP an unrecognized spelling is still
   * a 400 because ingress rejects it first; {@code HtsControllerTest} pins that.
   */
  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"VIEW", "view", "TABLE", "TaBlE", "UNKNOWN"})
  public void testPutUserViewAlwaysPersistsViewWhateverTheTransportSays(String declared) {
    Pair<UserTableDto, Boolean> created =
        userTablesService.putUserView(
            UserTable.builder()
                .databaseId(ENTITY_TYPE_DB)
                .tableId("owned_view")
                .tableVersion(INITIAL_TABLE_VERSION)
                .metadataLocation("/openhouse/entity_type_db/owned_view/v0_metadata.json")
                .entityType(declared)
                .build());

    Assertions.assertFalse(created.getSecond());
    assertThat(created.getFirst().getEntityType()).isEqualTo(EntityType.VIEW);
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "owned_view")).hasValue("VIEW");
  }

  /** The mirror on the table entry point, so neither method can be talked out of its type. */
  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"TABLE", "table", "VIEW", "ViEw", "UNKNOWN"})
  public void testPutUserTableAlwaysPersistsTableWhateverTheTransportSays(String declared) {
    Pair<UserTableDto, Boolean> created =
        userTablesService.putUserTable(
            UserTable.builder()
                .databaseId(ENTITY_TYPE_DB)
                .tableId("owned_table")
                .tableVersion(INITIAL_TABLE_VERSION)
                .metadataLocation("/openhouse/entity_type_db/owned_table/v0_metadata.json")
                .entityType(declared)
                .build());

    Assertions.assertFalse(created.getSecond());
    assertThat(created.getFirst().getEntityType()).isEqualTo(EntityType.TABLE);
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "owned_table")).hasValue("TABLE");
  }

  /** Regression guard: the cross-type guard must not over-fire and swallow version semantics. */
  @Test
  public void testSameTypePutStillRunsVersionLogic() {
    seedTypedRow(ENTITY_TYPE_DB, "same_type", EntityType.TABLE);
    UserTableRow before = findRow(ENTITY_TYPE_DB, "same_type");

    Pair<UserTableDto, Boolean> updated =
        userTablesService.putUserTable(
            UserTable.builder()
                .databaseId(ENTITY_TYPE_DB)
                .tableId("same_type")
                .tableVersion(before.getMetadataLocation())
                .metadataLocation("/openhouse/entity_type_db/same_type/v1_metadata.json")
                .build());
    assertThat(updated.getSecond()).isTrue();
    assertThat(updated.getFirst().getEntityType()).isEqualTo(EntityType.TABLE);
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "same_type")).hasValue("TABLE");

    // A stale version at the same type is still a concurrency conflict, not a type conflict.
    Assertions.assertThrows(
        EntityConcurrentModificationException.class,
        () ->
            userTablesService.putUserTable(
                UserTable.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId("same_type")
                    .tableVersion(before.getMetadataLocation())
                    .metadataLocation("/openhouse/entity_type_db/same_type/v2_metadata.json")
                    .build()));
  }

  @Test
  public void testPutAtCorruptKeySurfacesFailureAndRetainsTheOccupant() {
    insertRawEntityType(ENTITY_TYPE_DB, "put_corrupt", "UNKNOWN");

    assertThatThrownBy(
            () ->
                userTablesService.putUserTable(
                    UserTable.builder()
                        .databaseId(ENTITY_TYPE_DB)
                        .tableId("put_corrupt")
                        .tableVersion(INITIAL_TABLE_VERSION)
                        .metadataLocation("/openhouse/entity_type_db/put_corrupt/v1_metadata.json")
                        .build()))
        .isInstanceOf(CorruptEntityTypeException.class)
        .hasStackTraceContaining("user_table_row.entity_type");

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "put_corrupt")).hasValue("UNKNOWN");
  }

  /** The soft-deleted store has no discriminator, so the restore must reconstruct one. */
  @Test
  public void testRestoreReconstructsTableType() {
    userTablesService.deleteUserTable(
        TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId(), true);

    UserTable searchByKey =
        UserTable.builder()
            .databaseId(TEST_TUPLE_1_0.getDatabaseId())
            .tableId(TEST_TUPLE_1_0.getTableId())
            .build();
    UserTableDto softDeleted =
        userTablesService
            .getAllSoftDeletedTables(searchByKey, 0, 1, null)
            .get()
            .findFirst()
            .orElseThrow(() -> new AssertionError("the soft delete must have archived a row"));

    UserTableDto restored =
        userTablesService.restoreUserTable(
            TEST_TUPLE_1_0.getDatabaseId(),
            TEST_TUPLE_1_0.getTableId(),
            softDeleted.getDeletedAtMs());

    assertThat(restored.getEntityType()).isEqualTo(EntityType.TABLE);
    assertThat(readRawEntityType(TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId()))
        .hasValue("TABLE");
  }

  /**
   * Regression guard: do not narrow restore's occupancy read to the table-scoped finder. It would
   * see a VIEW at the destination as free and clobber it — the drop → CREATE VIEW → restore case.
   */
  @Test
  public void testRestoreDoesNotOverwriteAViewOccupyingTheKey() {
    userTablesService.deleteUserTable(
        TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId(), true);

    UserTable searchByKey =
        UserTable.builder()
            .databaseId(TEST_TUPLE_1_0.getDatabaseId())
            .tableId(TEST_TUPLE_1_0.getTableId())
            .build();
    UserTableDto archived =
        userTablesService
            .getAllSoftDeletedTables(searchByKey, 0, 1, null)
            .get()
            .findFirst()
            .orElseThrow(() -> new AssertionError("the soft delete must have archived a row"));

    // The name is taken again, by a view this time.
    seedTypedRow(TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId(), EntityType.VIEW);
    UserTableRow viewBefore = findRow(TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId());

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            userTablesService.restoreUserTable(
                TEST_TUPLE_1_0.getDatabaseId(),
                TEST_TUPLE_1_0.getTableId(),
                archived.getDeletedAtMs()));

    // The view is untouched, and the archived row is still archived rather than consumed.
    UserTableRow viewAfter = findRow(TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId());
    assertThat(viewAfter.getEntityType()).isEqualTo(EntityType.VIEW);
    assertThat(viewAfter.getVersion()).isEqualTo(viewBefore.getVersion());
    assertThat(viewAfter.getMetadataLocation()).isEqualTo(viewBefore.getMetadataLocation());
    Assertions.assertEquals(
        1, userTablesService.getAllSoftDeletedTables(searchByKey, 0, 10, null).getTotalElements());
  }

  // ---------------------------------------------------------------------------------------------
  // table-scoped rename at the service
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testRenameUserTableRefusesViewSource() {
    seedTypedRow(ENTITY_TYPE_DB, "svc_rename_view", EntityType.VIEW);

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () ->
            userTablesService.renameUserTable(
                ENTITY_TYPE_DB,
                "svc_rename_view",
                ENTITY_TYPE_DB,
                "svc_rename_view_moved",
                "/openhouse/entity_type_db/svc_rename_view_moved/v1_metadata.json"));

    assertThat(findRow(ENTITY_TYPE_DB, "svc_rename_view").getEntityType())
        .isEqualTo(EntityType.VIEW);
    assertThat(
            htsRepository
                .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
                    ENTITY_TYPE_DB, "svc_rename_view_moved")
                .isPresent())
        .isFalse();
  }

  @Test
  public void testRenameUserTableAtCorruptSourceIsNotFound() {
    insertRawEntityType(ENTITY_TYPE_DB, "svc_rename_corrupt", "UNKNOWN");

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () ->
            userTablesService.renameUserTable(
                ENTITY_TYPE_DB,
                "svc_rename_corrupt",
                ENTITY_TYPE_DB,
                "svc_rename_corrupt_moved",
                "/openhouse/entity_type_db/svc_rename_corrupt_moved/v1_metadata.json"));

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "svc_rename_corrupt")).hasValue("UNKNOWN");
  }

  /**
   * Hydrated equality would be tautological because a SQL NULL already reads as TABLE; only the raw
   * column proves the literal was written.
   */
  @Test
  public void testRenameUserTableStampsCanonicalTableOnLegacyRow() {
    seedLegacyRow(ENTITY_TYPE_DB, "svc_rename_legacy");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "svc_rename_legacy")).isEmpty();

    userTablesService.renameUserTable(
        ENTITY_TYPE_DB,
        "svc_rename_legacy",
        ENTITY_TYPE_DB,
        "svc_rename_legacy_moved",
        "/openhouse/entity_type_db/svc_rename_legacy_moved/v1_metadata.json");

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "svc_rename_legacy_moved")).hasValue("TABLE");
  }

  /**
   * Regression guard: a corrupt destination is occupied, not free, under {@code
   * TABLE_ROW_PREDICATE}. The primary key rejects the move without reading the row, so the answer
   * is the ordinary conflict rather than the failure a hydration attempt would produce.
   */
  @Test
  public void testRenameIntoCorruptDestinationIsAlreadyExists() {
    seedTypedRow(ENTITY_TYPE_DB, "svc_rename_src_for_corrupt", EntityType.TABLE);
    insertRawEntityType(ENTITY_TYPE_DB, "svc_rename_dst_corrupt", "UNKNOWN");

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            userTablesService.renameUserTable(
                ENTITY_TYPE_DB,
                "svc_rename_src_for_corrupt",
                ENTITY_TYPE_DB,
                "svc_rename_dst_corrupt",
                "/openhouse/entity_type_db/svc_rename_dst_corrupt/v1_metadata.json"));

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "svc_rename_src_for_corrupt")).hasValue("TABLE");
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "svc_rename_dst_corrupt")).hasValue("UNKNOWN");
  }

  /** Reads the counter and timer deltas a single instrumented call must produce. */
  private void assertMetricsAdvance(String requestMetric, String timeMetric, Runnable call) {
    double requestsBefore = counterValue(requestMetric);
    long timesBefore = timerCount(timeMetric);

    call.run();

    assertThat(counterValue(requestMetric))
        .as("counter %s", requestMetric)
        .isEqualTo(requestsBefore + 1);
    assertThat(timerCount(timeMetric)).as("timer %s", timeMetric).isEqualTo(timesBefore + 1);
  }

  private static double counterValue(String metric) {
    Counter counter =
        Metrics.globalRegistry.find(MetricsConstant.HOUSETABLES_SERVICE + "_" + metric).counter();
    return counter == null ? 0.0 : counter.count();
  }

  private static long timerCount(String metric) {
    Timer timer =
        Metrics.globalRegistry.find(MetricsConstant.HOUSETABLES_SERVICE + "_" + metric).timer();
    return timer == null ? 0L : timer.count();
  }

  private UserTableRow findRow(String databaseId, String tableId) {
    return htsRepository
        .findById(UserTableRowPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build())
        .orElseThrow(
            () -> new AssertionError("Expected row " + databaseId + "." + tableId + " to exist"));
  }
}
