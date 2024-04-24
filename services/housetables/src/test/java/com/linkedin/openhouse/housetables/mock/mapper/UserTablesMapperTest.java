package com.linkedin.openhouse.housetables.mock.mapper;

import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
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
    UserTableDto dtoAfterMapping =
        userTablesMapper.toUserTableDto(TestHouseTableModelConstants.TEST_USER_TABLE_ROW);
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
    Assertions.assertEquals(
        TestHouseTableModelConstants.TEST_USER_TABLE_ROW,
        userTablesMapper.toUserTableRow(
            TestHouseTableModelConstants.TEST_USER_TABLE.toBuilder().storageType(null).build(),
            Optional.empty()));
  }

  @Test
  void toUserTableRowCustomStorageType() {
    Assertions.assertEquals(
        TestHouseTableModelConstants.TEST_USER_TABLE_ROW.toBuilder().storageType("blobfs").build(),
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
}
