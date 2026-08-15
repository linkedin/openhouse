package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.e2e.SpringH2HtsApplication;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.SoftDeletedUserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.UserTablesService;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.data.domain.Page;
import org.springframework.data.util.Pair;

@SpringBootTest(classes = SpringH2HtsApplication.class)
public class UserTablesServiceTest {

  private static final String CASE_DB_1 = "deleteDb1";
  private static final String CASE_DB_2 = "deleteDb2";
  private static final String CASE_TBL_1 = "deleteTb1";
  private static final String CASE_TBL_2 = "deleteTb2";

  @Autowired UserTablesService userTablesService;

  @SpyBean UserTableHtsJdbcRepository htsRepository;

  @SpyBean SoftDeletedUserTableHtsJdbcRepository softDeletedHtsJdbcRepository;

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
    htsRepository.deleteAll();
    softDeletedHtsJdbcRepository.deleteAll();
  }

  @Test
  public void testUserTableGet() {
    // TODO: Use service layer function to create/update the repository.
    Assertions.assertTrue(
        isUserTableDtoEqual(
            TestHouseTableModelConstants.TEST_USER_TABLE_DTO,
            userTablesService.getUserTable(
                TestHouseTableModelConstants.TEST_DB_ID,
                TestHouseTableModelConstants.TEST_TABLE_ID)));
    Assertions.assertTrue(
        isUserTableDtoEqual(
            TEST_TUPLE_1_0.get_userTableDto(),
            userTablesService.getUserTable(
                TEST_TUPLE_1_0.getDatabaseId(), TEST_TUPLE_1_0.getTableId())));
    Assertions.assertTrue(
        isUserTableDtoEqual(
            TestHouseTableModelConstants.TEST_TUPLE_1_1.get_userTableDto(),
            userTablesService.getUserTable(
                TestHouseTableModelConstants.TEST_TUPLE_1_1.getDatabaseId(),
                TestHouseTableModelConstants.TEST_TUPLE_1_1.getTableId())));
    // testing case insensitivity when lookup by repeating the lookup again
    Assertions.assertTrue(
        isUserTableDtoEqual(
            TestHouseTableModelConstants.TEST_USER_TABLE_DTO,
            userTablesService.getUserTable(
                TestHouseTableModelConstants.TEST_DB_ID.toLowerCase(),
                TestHouseTableModelConstants.TEST_TABLE_ID.toLowerCase())));
    Assertions.assertTrue(
        isUserTableDtoEqual(
            TEST_TUPLE_1_0.get_userTableDto(),
            userTablesService.getUserTable(
                TEST_TUPLE_1_0.getDatabaseId().toLowerCase(),
                TEST_TUPLE_1_0.getTableId().toLowerCase())));
    Assertions.assertTrue(
        isUserTableDtoEqual(
            TestHouseTableModelConstants.TEST_TUPLE_1_1.get_userTableDto(),
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
    results.add(
        TEST_TUPLE_1_0
            .get_userTableDto()
            .toBuilder()
            .tableVersion(TEST_TUPLE_1_0.get_userTableDto().getMetadataLocation())
            .build());
    results.add(
        TEST_TUPLE_2_0
            .get_userTableDto()
            .toBuilder()
            .tableVersion(TEST_TUPLE_2_0.get_userTableDto().getMetadataLocation())
            .build());
    results.add(
        TEST_TUPLE_3_0
            .get_userTableDto()
            .toBuilder()
            .tableVersion(TEST_TUPLE_3_0.get_userTableDto().getMetadataLocation())
            .build());
    results.add(
        TEST_TUPLE_4_0
            .get_userTableDto()
            .toBuilder()
            .tableVersion(TEST_TUPLE_4_0.get_userTableDto().getMetadataLocation())
            .build());
    results.add(
        TEST_USER_TABLE_DTO
            .toBuilder()
            .tableVersion(TEST_USER_TABLE_DTO.getMetadataLocation())
            .build());

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
    assertThat(isUserTableDtoEqual(actual.get(0), TEST_TUPLE_2_0.get_userTableDto())).isTrue();
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
        .deleteById(any());

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
  @ParameterizedTest
  @ValueSource(strings = {"VIEW", "view", "ViEw", "UNKNOWN"})
  public void testGetUserTableHidesNonTableRows(String entityType) {
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "point_read", entityType));

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

  @ParameterizedTest
  @CsvSource(
      nullValues = "NULL",
      value = {"NULL", "TABLE", "table", "TaBlE"})
  public void testGetUserTableResolvesNullAndTableRows(String entityType) {
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "point_read", entityType));

    UserTableDto dto = userTablesService.getUserTable(ENTITY_TYPE_DB, "point_read");
    assertThat(dto.getTableId()).isEqualTo("point_read");
    assertThat(dto.getEntityType()).isEqualTo(entityType);
  }

  /**
   * The writers must still see a view at a shared key, otherwise a table create or delete would
   * silently act on a name another entity already holds.
   */
  @Test
  public void testWritersStillSeeNonTableRowsAtTheSameKey() {
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "shared_key", "VIEW"));

    UserTableRow seenByWriter =
        htsRepository
            .findById(
                UserTableRowPrimaryKey.builder()
                    .databaseId(ENTITY_TYPE_DB)
                    .tableId("shared_key")
                    .build())
            .orElseThrow(() -> new AssertionError("writer read must see the view row"));
    assertThat(seenByWriter.getEntityType()).isEqualTo("VIEW");

    // deleteUserTable resolves through the same neutral read, so it can still remove the row.
    userTablesService.deleteUserTable(ENTITY_TYPE_DB, "shared_key", false);
    assertThat(
            htsRepository
                .findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(ENTITY_TYPE_DB, "shared_key")
                .isPresent())
        .isFalse();
  }

  private UserTableRow entityTypeRow(String databaseId, String tableId, String entityType) {
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

  private void seedCanonicalRows(String prefix) {
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t00_legacy", null));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t01_view", "VIEW"));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t02_explicit", "TABLE"));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t03_view", "VIEW"));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t04_legacy", null));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t05_view", "VIEW"));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, prefix + "t06_explicit", "TABLE"));
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
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "nomatch_table", "TABLE"));
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
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "rename_src_table", "TABLE"));
    htsRepository.save(entityTypeRow(ENTITY_TYPE_DB, "rename_dst_view", "VIEW"));

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
    Assertions.assertEquals("VIEW", destinationAfter.getEntityType());
  }

  private UserTableRow findRow(String databaseId, String tableId) {
    return htsRepository
        .findById(UserTableRowPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build())
        .orElseThrow(
            () -> new AssertionError("Expected row " + databaseId + "." + tableId + " to exist"));
  }
}
