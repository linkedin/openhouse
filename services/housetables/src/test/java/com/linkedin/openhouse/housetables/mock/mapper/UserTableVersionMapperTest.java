package com.linkedin.openhouse.housetables.mock.mapper;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.housetables.dto.mapper.UserTableVersionMapper;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class UserTableVersionMapperTest {

  @Autowired UserTableVersionMapper versionMapper;

  @Test
  void testToVersionWithNoExistingRow() {
    Assertions.assertEquals(
        null,
        versionMapper.toVersion(TestHouseTableModelConstants.TEST_USER_TABLE, Optional.empty()));
  }

  @Test
  void testToVersionWithNoExistingRowAndIncorrectTableVersion() {
    Assertions.assertThrows(
        EntityConcurrentModificationException.class,
        () ->
            versionMapper.toVersion(
                TestHouseTableModelConstants.TEST_USER_TABLE.toBuilder().tableVersion("v1").build(),
                Optional.empty()));
  }

  @Test
  void testToVersionWithExistingRowAndCorrectMetadataLocation() {
    Assertions.assertEquals(
        versionMapper.toVersion(
            TestHouseTableModelConstants.TEST_USER_TABLE
                .toBuilder()
                .tableVersion(
                    TestHouseTableModelConstants.TEST_USER_TABLE_ROW.getMetadataLocation())
                .build(),
            Optional.of(TestHouseTableModelConstants.TEST_USER_TABLE_ROW)),
        TestHouseTableModelConstants.TEST_USER_TABLE_ROW.getVersion());
  }

  @Test
  void testToVersionWithExistingRowAndIncorrectMetadataLocation() {
    Assertions.assertThrows(
        EntityConcurrentModificationException.class,
        () ->
            versionMapper.toVersion(
                TestHouseTableModelConstants.TEST_USER_TABLE,
                Optional.of(TestHouseTableModelConstants.TEST_USER_TABLE_ROW)));
  }
}
