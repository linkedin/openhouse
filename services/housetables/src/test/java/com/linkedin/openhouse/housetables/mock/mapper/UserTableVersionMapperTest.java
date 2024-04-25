package com.linkedin.openhouse.housetables.mock.mapper;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.housetables.dto.mapper.UserTableVersionMapper;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
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
        versionMapper.toVersion(TestHouseTableModelConstants.TEST_USER_TABLE, Optional.empty()),
        1L);
  }

  @Test
  void testToVersionWithExistingRowAndCorrectMetadataLocation() {
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    Assertions.assertEquals(
        versionMapper.toVersion(
            TestHouseTableModelConstants.TEST_USER_TABLE
                .toBuilder()
                .tableVersion(testUserTableRow.getMetadataLocation())
                .build(),
            Optional.of(testUserTableRow)),
        testUserTableRow.getVersion());
  }

  @Test
  void testToVersionWithExistingRowAndIncorrectMetadataLocation() {
    UserTableRow testUserTableRow =
        new TestHouseTableModelConstants.TestTuple(0).get_userTableRow();
    Assertions.assertThrows(
        EntityConcurrentModificationException.class,
        () ->
            versionMapper.toVersion(
                TestHouseTableModelConstants.TEST_USER_TABLE, Optional.of(testUserTableRow)));
  }
}
