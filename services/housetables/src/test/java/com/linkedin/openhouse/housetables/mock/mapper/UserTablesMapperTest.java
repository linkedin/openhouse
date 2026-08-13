package com.linkedin.openhouse.housetables.mock.mapper;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class UserTablesMapperTest {
  @Autowired UserTablesMapper userTablesMapper;

  @Test
  void toUserTableDto() {
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    UserTableDto dtoAfterMapping = userTablesMapper.toUserTableDto(testUserTableRow);
    // Assert objects are equal ignoring versions
    Assertions.assertEquals(
        TestHouseTableModelConstants.TEST_USER_TABLE_DTO.toBuilder().tableVersion("").build(),
        dtoAfterMapping.toBuilder().tableVersion("").build());
    // Assert After Mapping version is same as the source's metadataLocation
    Assertions.assertEquals(
        TestHouseTableModelConstants.TEST_USER_TABLE_DTO.getMetadataLocation(),
        dtoAfterMapping.getTableVersion());
  }

  @Test
  void toUserTable() {
    Assertions.assertEquals(
        TestHouseTableModelConstants.TEST_USER_TABLE,
        userTablesMapper.toUserTable(TestHouseTableModelConstants.TEST_USER_TABLE_DTO));
  }

  @Test
  void toUserTableRowNullStorageType() {
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    Assertions.assertEquals(
        testUserTableRow,
        userTablesMapper.toUserTableRow(
            TestHouseTableModelConstants.TEST_USER_TABLE.toBuilder().storageType(null).build(),
            Optional.empty()));
  }

  @Test
  void toUserTableRowCustomStorageType() {
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    Assertions.assertEquals(
        testUserTableRow.toBuilder().storageType("blobfs").build(),
        userTablesMapper.toUserTableRow(
            TestHouseTableModelConstants.TEST_USER_TABLE.toBuilder().storageType("blobfs").build(),
            Optional.empty()));
  }

  @Test
  void fromUserTable() {
    Assertions.assertEquals(
        TestHouseTableModelConstants.TEST_USER_TABLE_DTO,
        userTablesMapper.fromUserTable(TestHouseTableModelConstants.TEST_USER_TABLE));
  }

  /**
   * The entityType discriminator must survive every hop of the HTS mapping chain (API -> JPA row ->
   * DTO -> API). A mapper that silently drops it would let a view pointer be persisted as a legacy
   * table row.
   */
  @Test
  void entityTypeRoundTripsAcrossUserTableRowDtoAndApi() {
    UserTable viewUserTable =
        TestHouseTableModelConstants.TEST_USER_TABLE.toBuilder().entityType("VIEW").build();

    UserTableRow row = userTablesMapper.toUserTableRow(viewUserTable, Optional.empty());
    Assertions.assertEquals("VIEW", row.getEntityType());

    UserTableDto dto = userTablesMapper.toUserTableDto(row);
    Assertions.assertEquals("VIEW", dto.getEntityType());

    UserTable roundTripped = userTablesMapper.toUserTable(dto);
    Assertions.assertEquals("VIEW", roundTripped.getEntityType());

    // Every other field is untouched by the new discriminator.
    Assertions.assertEquals(viewUserTable.getTableId(), roundTripped.getTableId());
    Assertions.assertEquals(viewUserTable.getDatabaseId(), roundTripped.getDatabaseId());
    Assertions.assertEquals(
        viewUserTable.getMetadataLocation(), roundTripped.getMetadataLocation());
    Assertions.assertEquals(viewUserTable.getStorageType(), roundTripped.getStorageType());
    Assertions.assertEquals(viewUserTable.getCreationTime(), roundTripped.getCreationTime());

    // fromUserTable is the other API -> DTO direction and must carry it too.
    Assertions.assertEquals("VIEW", userTablesMapper.fromUserTable(viewUserTable).getEntityType());
  }

  /**
   * Backward compatibility: legacy writers omit the field entirely. No layer may default it to
   * "TABLE", because that would start stamping a value on every existing table write and mask the
   * null-means-table compatibility contract.
   */
  @Test
  void nullEntityTypeRemainsNullAcrossLegacyMappings() {
    UserTable legacyUserTable = TestHouseTableModelConstants.TEST_USER_TABLE;
    Assertions.assertNull(legacyUserTable.getEntityType());

    UserTableRow row = userTablesMapper.toUserTableRow(legacyUserTable, Optional.empty());
    Assertions.assertNull(row.getEntityType());

    UserTableDto dto = userTablesMapper.toUserTableDto(row);
    Assertions.assertNull(dto.getEntityType());

    Assertions.assertNull(userTablesMapper.toUserTable(dto).getEntityType());
    Assertions.assertNull(userTablesMapper.fromUserTable(legacyUserTable).getEntityType());

    // The legacy fixture row (built without the field) must still map cleanly.
    UserTableRow legacyRow = new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    Assertions.assertNull(legacyRow.getEntityType());
    Assertions.assertNull(userTablesMapper.toUserTableDto(legacyRow).getEntityType());
  }

  /**
   * The /hts query endpoint hands raw request parameters to {@code mapToUserTable}. If entityType
   * is not recognized there, an {@code entityType=VIEW} query silently degrades to an unfiltered
   * table listing.
   */
  @Test
  void mapToUserTableRecognizesEntityType() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("databaseId", "test_db0");
    parameters.put("entityType", "VIEW");

    UserTable mapped = userTablesMapper.mapToUserTable(parameters);

    Assertions.assertEquals("test_db0", mapped.getDatabaseId());
    Assertions.assertEquals("VIEW", mapped.getEntityType());
    Assertions.assertNull(mapped.getTableId());
  }
}
