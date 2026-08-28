package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.NoSuchEntityException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.e2e.SpringH2HtsApplication;
import com.linkedin.openhouse.housetables.metrics.HouseTablesMetricsConstant;
import com.linkedin.openhouse.housetables.model.EntityType;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.SoftDeletedUserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.UserTablesService;
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

  @SpyBean SoftDeletedUserTableHtsJdbcRepository softDeletedHtsJdbcRepository;

  @Autowired DataSource dataSource;

  @BeforeEach
  public void setup() {
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    htsRepository.save(testUserTableRow);
    htsRepository.save(TEST_TUPLE_1_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_2_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_3_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_4_0.get_userTableRow());
    htsRepository.save(TEST_TUPLE_1_1.get_userTableRow());
    htsRepository.save(TEST_TUPLE_2_1.get_userTableRow());
    htsRepository.save(TEST_TUPLE_3_1.get_userTableRow());
    htsRepository.save(TEST_TUPLE_1_2.get_userTableRow());
    htsRepository.save(TEST_TUPLE_2_2.get_userTableRow());

    // delete candidate
    htsRepository.save(
        TEST_TUPLE_1_0
            .get_userTableRow()
            .toBuilder()
            .tableId(CASE_TBL_1)
            .databaseId(CASE_DB_1)
            .build());
    htsRepository.save(
        TEST_TUPLE_1_0
            .get_userTableRow()
            .toBuilder()
            .tableId(CASE_TBL_2)
            .databaseId(CASE_DB_2)
            .build());
    // Clear any mocks
    Mockito.reset(htsRepository);
    Mockito.reset(softDeletedHtsJdbcRepository);
  }

  @AfterEach
  public void tearDown() {
    // The JPA cleanup loads every row, so a planted non-canonical spelling must go first;
    // otherwise converter hydration during teardown poisons the rest of the class.
    new JdbcTemplate(dataSource)
        .update("DELETE FROM user_table_row WHERE entity_type NOT IN ('TABLE', 'VIEW')");
    htsRepository.deleteAll();
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
            .entityType(EntityType.TABLE.name())
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
    htsRepository.save(TEST_TUPLE_1_0.get_userTableRow());
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
    htsRepository.save(TEST_TUPLE_2_0.get_userTableRow());
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

  @Test
  public void testGetUserTableResolvesLegacyRowAsTable() {
    seedLegacyRow(ENTITY_TYPE_DB, "point_read");

    expectPointReadResolvesAsTable();
  }

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
   * The writers must still see a view at a shared key, otherwise a table create or delete would
   * silently act on a name another entity already holds. Seeing it is not licence to remove it: the
   * table-scoped delete refuses.
   */
  @Test
  public void testWritersStillSeeNonTableRowsAtTheSameKey() {
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "shared_key", EntityType.VIEW));

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

  /**
   * The strict converter refuses to write a null discriminator, so a legacy row can only be planted
   * through the column. A non-canonical spelling has the same constraint.
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
   * Plants a legacy row through the column, because the write path does not accept a null
   * discriminator.
   */
  private void seedLegacyRow(String databaseId, String tableId) {
    insertRawEntityType(databaseId, tableId, null);
  }

  /** Plants a typed row through JPA, so the enum boundary is still the thing under test. */
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

  /**
   * The query endpoint is table-scoped by path, so {@code entityType} is bound onto the request but
   * never reaches a predicate: an {@code entityType=VIEW} request is answered with tables, and
   * routing is unaffected by the field.
   */
  @Test
  public void testEntityTypeOnQueryIsIgnoredAndAlwaysReturnsTables() {
    seedCanonicalRows("");

    for (String entityType : new String[] {"VIEW", "view", "TABLE", "TaBlE", null}) {
      assertThat(
              sortedIds(
                  userTablesService.getAllUserTables(
                      UserTable.builder()
                          .databaseId(ENTITY_TYPE_DB)
                          .entityType(entityType)
                          .build())))
          .as("entityType=%s must still resolve to the four visible tables", entityType)
          .isEqualTo(CANONICAL_TABLE_IDS);
    }

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

  /** A legacy null is reported as TABLE because that is what the data means, not a guess. */
  @Test
  public void testGetNeutralEntityReportsCanonicalTypeForEitherType() {
    seedTypedRow(ENTITY_TYPE_DB, "neutral_view", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, "neutral_table", EntityType.TABLE);
    seedLegacyRow(ENTITY_TYPE_DB, "neutral_legacy");

    assertThat(userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_view").getEntityType())
        .isEqualTo(EntityType.VIEW);
    assertThat(userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_table").getEntityType())
        .isEqualTo(EntityType.TABLE);
    assertThat(userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_legacy").getEntityType())
        .isEqualTo(EntityType.TABLE);

    // The stored null is reported as TABLE without being rewritten.
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "neutral_legacy")).isEmpty();

    // The key itself resolves case-insensitively, like every other point read.
    assertThat(
            userTablesService
                .getNeutralEntity(ENTITY_TYPE_DB.toUpperCase(), "NEUTRAL_VIEW")
                .getEntityType())
        .isEqualTo(EntityType.VIEW);
  }

  /** The stored spelling is resolved in Java rather than left to the database collation. */
  @Test
  public void testGetNeutralEntityResolvesEveryStoredSpelling() {
    insertRawEntityType(ENTITY_TYPE_DB, "spell_lower_view", "view");
    insertRawEntityType(ENTITY_TYPE_DB, "spell_mixed_view", "ViEw");
    insertRawEntityType(ENTITY_TYPE_DB, "spell_lower_table", "table");
    insertRawEntityType(ENTITY_TYPE_DB, "spell_mixed_table", "TaBlE");

    assertThat(
            userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "spell_lower_view").getEntityType())
        .isEqualTo(EntityType.VIEW);
    assertThat(
            userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "spell_mixed_view").getEntityType())
        .isEqualTo(EntityType.VIEW);
    assertThat(
            userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "spell_lower_table").getEntityType())
        .isEqualTo(EntityType.TABLE);
    assertThat(
            userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "spell_mixed_table").getEntityType())
        .isEqualTo(EntityType.TABLE);
  }

  /** Genuine absence, and only genuine absence, is not-found. */
  @Test
  public void testGetNeutralEntityMissingKeyIsNotFound() {
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_absent"));
  }

  /**
   * "The name is free" is the dangerous default: an unfiltered catch would turn a repository
   * failure into a 404 and let a caller overwrite an occupied key.
   */
  @Test
  public void testGetNeutralEntityPropagatesRepositoryFailure() {
    doThrow(new DataAccessResourceFailureException("injected"))
        .when(htsRepository)
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(anyString(), anyString());

    assertThatThrownBy(() -> userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_boom"))
        .isInstanceOf(DataAccessResourceFailureException.class)
        .hasMessageContaining("injected");
  }

  /**
   * The complement: an empty Optional, and nothing else, is what becomes not-found. A real row is
   * seeded first, so the stubbed empty result is the only possible source of the exception — and
   * the stub only intercepts if the neutral read is the method the service actually calls.
   */
  @Test
  public void testGetNeutralEntityOnlyEmptyOptionalBecomesNotFound() {
    seedTypedRow(ENTITY_TYPE_DB, "neutral_stubbed", EntityType.VIEW);
    doReturn(Optional.empty())
        .when(htsRepository)
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(anyString(), anyString());

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_stubbed"));

    Mockito.verify(htsRepository)
        .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(ENTITY_TYPE_DB, "neutral_stubbed");
  }

  /** A corrupt discriminator must fail, never read as free. */
  @Test
  public void testGetNeutralEntityAtCorruptKeyFailsLoudly() {
    insertRawEntityType(ENTITY_TYPE_DB, "neutral_corrupt", "UNKNOWN");

    assertThatThrownBy(() -> userTablesService.getNeutralEntity(ENTITY_TYPE_DB, "neutral_corrupt"))
        .hasStackTraceContaining("user_table_row.entity_type")
        .hasStackTraceContaining("UNKNOWN");

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "neutral_corrupt")).hasValue("UNKNOWN");
  }

  // ---------------------------------------------------------------------------------------------
  // view reads
  // ---------------------------------------------------------------------------------------------

  /** A table at the key reads as absent, mirroring the table point read. */
  @Test
  public void testGetUserViewHidesTablesAndLegacyNullRows() {
    seedTypedRow(ENTITY_TYPE_DB, "view_point", EntityType.VIEW);
    seedTypedRow(ENTITY_TYPE_DB, "table_point", EntityType.TABLE);
    seedLegacyRow(ENTITY_TYPE_DB, "legacy_point");

    UserTableDto view = userTablesService.getUserView(ENTITY_TYPE_DB, "view_point");
    assertThat(view.getTableId()).isEqualTo("view_point");
    assertThat(view.getEntityType()).isEqualTo(EntityType.VIEW);

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userTablesService.getUserView(ENTITY_TYPE_DB, "table_point"));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userTablesService.getUserView(ENTITY_TYPE_DB, "legacy_point"));

    // Hidden from the view read, not deleted.
    assertThat(findRow(ENTITY_TYPE_DB, "table_point").getEntityType()).isEqualTo(EntityType.TABLE);
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "legacy_point")).isEmpty();
  }

  /** An empty view query returns every view, not the table query's database-name projection. */
  @Test
  public void testGetAllUserViewsWithEmptyQueryReturnsEveryView() {
    seedCanonicalRows("");

    List<UserTableDto> views = userTablesService.getAllUserViews(UserTable.builder().build());

    assertThat(sortedIds(views)).isEqualTo(CANONICAL_VIEW_IDS);
    // Not a database-name projection: every result is a fully identified view.
    assertThat(views).allSatisfy(v -> assertThat(v.getTableId()).isNotNull());
    assertThat(views).allSatisfy(v -> assertThat(v.getEntityType()).isEqualTo(EntityType.VIEW));

    Page<UserTableDto> page =
        userTablesService.getAllUserViews(UserTable.builder().build(), 0, 50, "tableId");
    Assertions.assertEquals(3, page.getTotalElements());
    assertThat(pageIds(page)).containsExactlyElementsOf(CANONICAL_VIEW_IDS);
  }

  /** The database and pattern view call sites filter before they page, exactly like the tables. */
  @Test
  public void testGetAllUserViewsFiltersBeforePagination() {
    seedCanonicalRows("");
    UserTable searchBy = UserTable.builder().databaseId(ENTITY_TYPE_DB).build();

    assertThat(sortedIds(userTablesService.getAllUserViews(searchBy)))
        .isEqualTo(CANONICAL_VIEW_IDS);

    Page<UserTableDto> page0 = userTablesService.getAllUserViews(searchBy, 0, 2, "tableId");
    Assertions.assertEquals(3, page0.getTotalElements());
    Assertions.assertEquals(2, page0.getTotalPages());
    Assertions.assertEquals(2, page0.getContent().size());
    assertThat(pageIds(page0)).containsExactly("t01_view", "t03_view");

    Page<UserTableDto> page1 = userTablesService.getAllUserViews(searchBy, 1, 2, "tableId");
    Assertions.assertEquals(3, page1.getTotalElements());
    assertThat(pageIds(page1)).containsExactly("t05_view");

    assertThat(pageIds(page0)).doesNotContainAnyElementsOf(CANONICAL_TABLE_IDS);
    assertThat(pageIds(page1)).doesNotContainAnyElementsOf(CANONICAL_TABLE_IDS);
  }

  /** The pattern view call sites (plain and paged) apply the same predicate. */
  @Test
  public void testGetAllUserViewsWithPatternFiltersViews() {
    seedCanonicalRows("match_");
    seedTypedRow(ENTITY_TYPE_DB, "nomatch_view", EntityType.VIEW);
    UserTable searchBy = UserTable.builder().databaseId(ENTITY_TYPE_DB).tableId("match_%").build();

    assertThat(sortedIds(userTablesService.getAllUserViews(searchBy)))
        .containsExactly("match_t01_view", "match_t03_view", "match_t05_view");

    Page<UserTableDto> page0 = userTablesService.getAllUserViews(searchBy, 0, 2, "tableId");
    Assertions.assertEquals(3, page0.getTotalElements());
    Assertions.assertEquals(2, page0.getTotalPages());
    assertThat(pageIds(page0)).containsExactly("match_t01_view", "match_t03_view");
  }

  /**
   * The view query is view-scoped by the method that serves it, so an {@code entityType} property
   * bound onto the request is tolerated and ignored — it can never re-route to tables.
   */
  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"TABLE", "table", "VIEW", "ViEw"})
  public void testEntityTypeOnViewQueryIsIgnoredAndAlwaysReturnsViews(String entityType) {
    seedCanonicalRows("");

    assertThat(
            sortedIds(
                userTablesService.getAllUserViews(
                    UserTable.builder().databaseId(ENTITY_TYPE_DB).entityType(entityType).build())))
        .as("entityType=%s must still resolve to the three views", entityType)
        .isEqualTo(CANONICAL_VIEW_IDS);
  }

  /** The paged view call site is equally type-blind. */
  @Test
  public void testEntityTypeOnPagedViewQueryIsIgnoredAndAlwaysReturnsViews() {
    seedCanonicalRows("");

    Page<UserTableDto> page0 =
        userTablesService.getAllUserViews(
            UserTable.builder().databaseId(ENTITY_TYPE_DB).entityType("TABLE").build(),
            0,
            2,
            "tableId");
    Assertions.assertEquals(3, page0.getTotalElements());
    assertThat(pageIds(page0)).containsExactly("t01_view", "t03_view");
  }

  /** Every view read path is instrumented the same way its table sibling is. */
  @Test
  public void testViewListAndSearchMetricsAreReported() {
    seedCanonicalRows("");
    UserTable byDatabase = UserTable.builder().databaseId(ENTITY_TYPE_DB).build();

    assertMetricsAdvance(
        HouseTablesMetricsConstant.HTS_LIST_VIEWS_REQUEST,
        HouseTablesMetricsConstant.HTS_LIST_VIEWS_TIME,
        () -> userTablesService.getAllUserViews(byDatabase));

    assertMetricsAdvance(
        HouseTablesMetricsConstant.HTS_PAGE_VIEWS_REQUEST,
        HouseTablesMetricsConstant.HTS_PAGE_VIEWS_TIME,
        () -> userTablesService.getAllUserViews(byDatabase, 0, 2, "tableId"));

    // A non-key filter falls through to the general search branch.
    UserTable generalFilter = UserTable.builder().creationTime(TEST_CREATION_TIME).build();

    assertMetricsAdvance(
        HouseTablesMetricsConstant.HTS_GENERAL_SEARCH_VIEWS_REQUEST,
        HouseTablesMetricsConstant.HTS_SEARCH_VIEWS_TIME,
        () -> userTablesService.getAllUserViews(generalFilter));

    assertMetricsAdvance(
        HouseTablesMetricsConstant.HTS_PAGE_SEARCH_VIEWS_REQUEST,
        HouseTablesMetricsConstant.HTS_PAGE_SEARCH_VIEWS_TIME,
        () -> userTablesService.getAllUserViews(generalFilter, 0, 2, "tableId"));
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

    Assertions.assertDoesNotThrow(
        () -> userTablesService.deleteUserView(ENTITY_TYPE_DB, "drop_view"));

    assertThat(
            htsRepository
                .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(ENTITY_TYPE_DB, "drop_view")
                .isPresent())
        .isFalse();
    Assertions.assertEquals(
        0, userTablesService.getAllSoftDeletedTables(searchByKey, 0, 10, null).getTotalElements());

    // Dropping it again is a plain not-found, not a second delete.
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userTablesService.deleteUserView(ENTITY_TYPE_DB, "drop_view"));
  }

  /** The other direction of the cross-type delete guard: a view drop cannot remove a table. */
  @Test
  public void testDeleteUserViewAtTableKeyIsNotFoundAndRetainsTheTable() {
    seedTypedRow(ENTITY_TYPE_DB, "drop_table", EntityType.TABLE);
    seedLegacyRow(ENTITY_TYPE_DB, "drop_legacy");

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userTablesService.deleteUserView(ENTITY_TYPE_DB, "drop_table"));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userTablesService.deleteUserView(ENTITY_TYPE_DB, "drop_legacy"));

    assertThat(findRow(ENTITY_TYPE_DB, "drop_table").getEntityType()).isEqualTo(EntityType.TABLE);
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "drop_legacy")).isEmpty();
  }

  /** A soft table delete at a view key fails before anything is copied. */
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

  /** A typed delete at a corrupt key reports not-found and leaves the row for an operator. */
  @Test
  public void testTypedDeletesAtCorruptKeyAreNotFoundAndRetainTheRow() {
    insertRawEntityType(ENTITY_TYPE_DB, "delete_corrupt", "UNKNOWN");

    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () -> userTablesService.deleteUserTable(ENTITY_TYPE_DB, "delete_corrupt", false));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userTablesService.deleteUserView(ENTITY_TYPE_DB, "delete_corrupt"));

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "delete_corrupt")).hasValue("UNKNOWN");
  }

  /**
   * The request states the version the occupant actually holds, so only the type can reject it —
   * which is what pins the guard as running before version mapping.
   */
  @Test
  public void testTablePutAtViewKeyIsAlreadyExistsAndLeavesTheViewUnchanged() {
    seedTypedRow(ENTITY_TYPE_DB, "guard_view", EntityType.VIEW);
    UserTableRow before = findRow(ENTITY_TYPE_DB, "guard_view");

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            userTablesService.putUserTable(
                UserTable.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId("guard_view")
                    .tableVersion(before.getMetadataLocation())
                    .metadataLocation("/openhouse/entity_type_db/guard_view/v1_metadata.json")
                    .entityType(EntityType.TABLE.name())
                    .build()));

    UserTableRow after = findRow(ENTITY_TYPE_DB, "guard_view");
    assertThat(after.getEntityType()).isEqualTo(EntityType.VIEW);
    assertThat(after.getVersion()).isEqualTo(before.getVersion());
    assertThat(after.getMetadataLocation()).isEqualTo(before.getMetadataLocation());
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
            userTablesService.putUserTable(
                UserTable.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId("guard_table")
                    .tableVersion(tableBefore.getMetadataLocation())
                    .metadataLocation("/openhouse/entity_type_db/guard_table/v1_metadata.json")
                    .entityType(EntityType.VIEW.name())
                    .build()));

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            userTablesService.putUserTable(
                UserTable.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId("guard_legacy")
                    .tableVersion(legacyBefore.getMetadataLocation())
                    .metadataLocation("/openhouse/entity_type_db/guard_legacy/v1_metadata.json")
                    .entityType(EntityType.VIEW.name())
                    .build()));

    assertThat(findRow(ENTITY_TYPE_DB, "guard_table").getMetadataLocation())
        .isEqualTo(tableBefore.getMetadataLocation());
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "guard_table")).hasValue("TABLE");
    // A rejected write must not migrate the legacy occupant either.
    assertThat(readRawEntityType(ENTITY_TYPE_DB, "guard_legacy")).isEmpty();
  }

  /** If both the type and the version are wrong, the type collision is the one that is reported. */
  @Test
  public void testTypeCollisionWinsOverStaleVersion() {
    seedTypedRow(ENTITY_TYPE_DB, "guard_both_wrong", EntityType.VIEW);

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            userTablesService.putUserTable(
                UserTable.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId("guard_both_wrong")
                    .tableVersion("/openhouse/entity_type_db/guard_both_wrong/stale_metadata.json")
                    .metadataLocation("/openhouse/entity_type_db/guard_both_wrong/v1_metadata.json")
                    .entityType(EntityType.TABLE.name())
                    .build()));

    assertThat(findRow(ENTITY_TYPE_DB, "guard_both_wrong").getEntityType())
        .isEqualTo(EntityType.VIEW);
  }

  /**
   * A same-type write still runs the ordinary version logic, whatever spelling it arrived as.
   *
   * <p>Regression guard: the cross-type guard must not over-fire and swallow version semantics.
   */
  @Test
  public void testSameTypeMixedCasePutStillRunsVersionLogic() {
    seedTypedRow(ENTITY_TYPE_DB, "same_type", EntityType.TABLE);
    UserTableRow before = findRow(ENTITY_TYPE_DB, "same_type");

    Pair<UserTableDto, Boolean> updated =
        userTablesService.putUserTable(
            UserTable.builder()
                .databaseId(ENTITY_TYPE_DB)
                .tableId("same_type")
                .tableVersion(before.getMetadataLocation())
                .metadataLocation("/openhouse/entity_type_db/same_type/v1_metadata.json")
                .entityType("TaBlE")
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
                    .entityType("TABLE")
                    .build()));
  }

  /** A write at a corrupt key surfaces the failure; it must never read the key as free. */
  @Test
  public void testPutAtCorruptKeySurfacesFailureAndRetainsTheOccupant() {
    insertRawEntityType(ENTITY_TYPE_DB, "put_corrupt", "UNKNOWN");

    assertThatThrownBy(
            () ->
                userTablesService.putUserTable(
                    UserTable.builder()
                        .databaseId(ENTITY_TYPE_DB)
                        .tableId("put_corrupt")
                        .tableVersion("INITIAL_VERSION")
                        .metadataLocation("/openhouse/entity_type_db/put_corrupt/v1_metadata.json")
                        .entityType(EntityType.TABLE.name())
                        .build()))
        .hasStackTraceContaining("user_table_row.entity_type");

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "put_corrupt")).hasValue("UNKNOWN");
  }

  /**
   * Without an explicit constant the restore would write a null straight back, reintroducing the
   * legacy-null population the strict converter keeps closed.
   */
  @Test
  public void testRestoreReconstructsTableType() {
    Assertions.assertDoesNotThrow(
        () ->
            userTablesService.deleteUserTable(
                TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId(), true));

    UserTable searchByKey =
        UserTable.builder()
            .databaseId(TEST_TUPLE_1_0.getDatabaseId())
            .tableId(TEST_TUPLE_1_0.getTableId())
            .build();
    UserTableDto softDeleted =
        userTablesService.getAllSoftDeletedTables(searchByKey, 0, 1, null).get().findFirst().get();

    UserTableDto restored =
        userTablesService.restoreUserTable(
            TEST_TUPLE_1_0.getDatabaseId(),
            TEST_TUPLE_1_0.getTableId(),
            softDeleted.getDeletedAtMs());

    assertThat(restored.getEntityType()).isEqualTo(EntityType.TABLE);
    assertThat(readRawEntityType(TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId()))
        .hasValue("TABLE");
  }

  // ---------------------------------------------------------------------------------------------
  // table-scoped rename at the service
  // ---------------------------------------------------------------------------------------------

  /** A rename scoped to tables reports the missing source as not-found and moves nothing. */
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

  /** A corrupt source is equally unreachable, and is retained. */
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

  /** Hydrated equality would be tautological; the raw column proves the constant was written. */
  @Test
  public void testRenameUserTableStampsCanonicalTableOnLegacyRow() {
    insertRawEntityType(ENTITY_TYPE_DB, "svc_rename_legacy", null);

    Assertions.assertDoesNotThrow(
        () ->
            userTablesService.renameUserTable(
                ENTITY_TYPE_DB,
                "svc_rename_legacy",
                ENTITY_TYPE_DB,
                "svc_rename_legacy_moved",
                "/openhouse/entity_type_db/svc_rename_legacy_moved/v1_metadata.json"));

    assertThat(readRawEntityType(ENTITY_TYPE_DB, "svc_rename_legacy_moved")).hasValue("TABLE");
  }

  /** A missing source is the same not-found the zero-row count produces. */
  @Test
  public void testRenameUserTableMissingSourceIsNotFound() {
    Assertions.assertThrows(
        NoSuchUserTableException.class,
        () ->
            userTablesService.renameUserTable(
                ENTITY_TYPE_DB,
                "svc_rename_absent",
                ENTITY_TYPE_DB,
                "svc_rename_absent_moved",
                "/openhouse/entity_type_db/svc_rename_absent_moved/v1_metadata.json"));
  }

  /**
   * A corrupt destination is occupied, not free. The rename must not read it — the shared primary
   * key is what rejects the move — so the answer is the ordinary 409 rather than the 500 a
   * hydration attempt would produce, and neither row is touched.
   *
   * <p>Regression guard: a corrupt-typed destination must stay "occupied" (409) and never read as
   * "free" under {@code TABLE_ROW_PREDICATE}. Do not delete it as redundant.
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

  /**
   * Two first-creates of different types racing for one key. The cross-type guard cannot arbitrate
   * this: both racers legitimately read the key as free, so only the shared primary key is left to
   * pick a winner. There is no concurrency idiom in these tests, so the race is simulated
   * deterministically — the loser's occupancy read is pinned to the empty result it would have
   * taken before the winner committed, and the write then meets the constraint for real.
   *
   * <p>Regression guard: the shared primary key is the only arbiter of a cross-type race the
   * application-level guard cannot observe, because the losing writer's occupancy read is stale. Do
   * not delete it as redundant.
   */
  @Test
  public void testConcurrentCrossTypeFirstCreatesLeaveOneWinnerAndA409Loser() {
    UserTable tableCreate =
        UserTable.builder()
            .databaseId(ENTITY_TYPE_DB)
            .tableId("race_key")
            .tableVersion("INITIAL_VERSION")
            .metadataLocation("/openhouse/entity_type_db/race_key/v0_table_metadata.json")
            .entityType(EntityType.TABLE.name())
            .build();

    Pair<UserTableDto, Boolean> winner = userTablesService.putUserTable(tableCreate);
    assertThat(winner.getSecond()).as("the winner creates rather than updates").isFalse();
    assertThat(winner.getFirst().getEntityType()).isEqualTo(EntityType.TABLE);

    // The loser's occupancy read was taken before the winner committed. putUserTable reads through
    // findById, and a default method calls its siblings on the repository proxy rather than back
    // through the spy, so the stub has to be placed on findById itself to intercept.
    doReturn(Optional.empty()).when(htsRepository).findById(any());

    UserTable viewCreate =
        UserTable.builder()
            .databaseId(ENTITY_TYPE_DB)
            .tableId("race_key")
            .tableVersion("INITIAL_VERSION")
            .metadataLocation("/openhouse/entity_type_db/race_key/v0_view_metadata.json")
            .entityType(EntityType.VIEW.name())
            .build();

    Assertions.assertThrows(
        EntityConcurrentModificationException.class,
        () -> userTablesService.putUserTable(viewCreate));

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
