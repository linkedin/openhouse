package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static org.assertj.core.api.Assertions.*;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
public class HtsRepositoryTest {

  @Autowired HtsRepository<UserTableRow, UserTableRowPrimaryKey> htsRepository;

  @AfterEach
  public void tearDown() {
    htsRepository.deleteAll();
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
    htsRepository.save(testTuple1_1.get_userTableRow());
    UserTableRowPrimaryKey key =
        UserTableRowPrimaryKey.builder()
            .tableId(testTuple1_1.getTableId())
            .databaseId(testTuple1_1.getDatabaseId())
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
}
