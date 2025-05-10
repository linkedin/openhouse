package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static org.assertj.core.api.Assertions.*;

import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.e2e.SpringH2HtsApplication;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import com.linkedin.openhouse.housetables.services.UserTablesService;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.util.Pair;

@SpringBootTest(classes = SpringH2HtsApplication.class)
public class UserTablesServiceTest {

  private static final String CASE_DB_1 = "deleteDb1";
  private static final String CASE_DB_2 = "deleteDb2";
  private static final String CASE_TBL_1 = "deleteTb1";
  private static final String CASE_TBL_2 = "deleteTb2";

  @Autowired UserTablesService userTablesService;

  // USE THIS ONLY FOR SETUP AND TEAR-DOWN
  @Autowired HtsRepository<UserTableRow, UserTableRowPrimaryKey> htsRepository;

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
  }

  @AfterEach
  public void tearDown() {
    htsRepository.deleteAll();
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
    Assertions.assertDoesNotThrow(() -> userTablesService.deleteUserTable(databaseId, tableId));
    NoSuchUserTableException noSuchUserTableException =
        Assertions.assertThrows(
            NoSuchUserTableException.class,
            () -> userTablesService.deleteUserTable(databaseId, tableId));
    Assertions.assertEquals(noSuchUserTableException.getTableId(), tableId);
    Assertions.assertEquals(noSuchUserTableException.getDatabaseId(), databaseId);
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

  private Boolean isUserTableDtoEqual(UserTableDto expected, UserTableDto actual) {
    return expected
        .toBuilder()
        .tableVersion("")
        .build()
        .equals(actual.toBuilder().tableVersion("").build());
  }
}
