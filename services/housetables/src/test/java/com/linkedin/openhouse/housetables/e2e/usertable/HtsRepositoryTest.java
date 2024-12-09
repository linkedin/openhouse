package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static org.assertj.core.api.Assertions.*;

import com.google.common.collect.Lists;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import java.util.List;
import java.util.stream.Collectors;
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

  @Autowired UserTableHtsJdbcRepository htsRepository;

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
        Lists.newArrayList(htsRepository.findAllByDatabaseIdIgnoreCase("test_db0"));
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
            htsRepository.findAllByDatabaseIdAndTableIdLikeAllIgnoreCase(
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
            htsRepository.findAllByDatabaseIdAndTableIdLikeAllIgnoreCase(
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

    Assertions.assertEquals(testUserTableRow, actual);
    htsRepository.delete(actual);
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
