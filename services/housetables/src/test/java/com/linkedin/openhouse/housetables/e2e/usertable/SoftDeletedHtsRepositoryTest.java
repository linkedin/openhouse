package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRow;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.SoftDeletedUserTableHtsJdbcRepository;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
public class SoftDeletedHtsRepositoryTest {

  long deletedTimestamp = System.currentTimeMillis();

  SoftDeletedUserTableRow softDeletedUserTableRowTestTuple1_1 =
      SoftDeletedUserTableRow.builder()
          .databaseId(TEST_TUPLE_1_1.getDatabaseId())
          .tableId(TEST_TUPLE_1_1.getTableId())
          .version(0L)
          .metadataLocation(TEST_TUPLE_1_1.getTableLoc())
          .storageType(TEST_TUPLE_1_1.getStorageType())
          .deletedAtMs(deletedTimestamp)
          .purgeAfterMs(Instant.ofEpochMilli(deletedTimestamp).plusSeconds(3600).toEpochMilli())
          .build();

  @Autowired private SoftDeletedUserTableHtsJdbcRepository softDeletedUserTableHtsJdbcRepository;

  @AfterEach
  public void tearDown() {
    softDeletedUserTableHtsJdbcRepository.deleteAll();
  }

  @Test
  public void testSoftDeleteExists() {
    softDeletedUserTableHtsJdbcRepository.save(softDeletedUserTableRowTestTuple1_1);

    // Validate is case insensitive
    SoftDeletedUserTableRowPrimaryKey key =
        SoftDeletedUserTableRowPrimaryKey.builder()
            .tableId(TEST_TUPLE_1_1.getTableId().toUpperCase())
            .databaseId(TEST_TUPLE_1_1.getDatabaseId())
            .deletedAtMs(deletedTimestamp)
            .build();
    assertThat(softDeletedUserTableHtsJdbcRepository.existsById(key)).isTrue();
  }

  @Test
  public void testSoftDeleteFindById() {
    softDeletedUserTableHtsJdbcRepository.save(softDeletedUserTableRowTestTuple1_1);

    // Validate is case insensitive
    SoftDeletedUserTableRowPrimaryKey key =
        SoftDeletedUserTableRowPrimaryKey.builder()
            .tableId(TEST_TUPLE_1_1.getTableId().toUpperCase())
            .databaseId(TEST_TUPLE_1_1.getDatabaseId())
            .deletedAtMs(deletedTimestamp)
            .build();
    assertThat(softDeletedUserTableHtsJdbcRepository.findById(key)).isPresent();
  }

  @Test
  public void testPurgeSoftDeletedTables() {
    softDeletedUserTableHtsJdbcRepository.save(softDeletedUserTableRowTestTuple1_1);
    SoftDeletedUserTableRow tableWithLongerTTL =
        softDeletedUserTableRowTestTuple1_1
            .toBuilder()
            .purgeAfterMs(Instant.ofEpochMilli(deletedTimestamp).plusSeconds(10000).toEpochMilli())
            .deletedAtMs(deletedTimestamp + 1)
            .build();
    softDeletedUserTableHtsJdbcRepository.save(tableWithLongerTTL);
    String otherTableId = TEST_TUPLE_1_1.getTableId() + "_2";
    SoftDeletedUserTableRow tableWithSeparateTableName =
        softDeletedUserTableRowTestTuple1_1.toBuilder().tableId(otherTableId).build();
    softDeletedUserTableHtsJdbcRepository.save(tableWithSeparateTableName);
    // Validate is case insensitive
    SoftDeletedUserTableRowPrimaryKey key =
        SoftDeletedUserTableRowPrimaryKey.builder()
            .tableId(TEST_TUPLE_1_1.getTableId().toUpperCase())
            .databaseId(TEST_TUPLE_1_1.getDatabaseId())
            .deletedAtMs(deletedTimestamp)
            .build();

    // Does not delete any soft deleted user tables because purgeFromMs
    Assertions.assertDoesNotThrow(
        () ->
            softDeletedUserTableHtsJdbcRepository.deleteByDatabaseIdTableIdPurgeAfterMs(
                TEST_TUPLE_1_1.getDatabaseId(), TEST_TUPLE_1_1.getTableId(), deletedTimestamp));

    Assertions.assertEquals(
        3,
        softDeletedUserTableHtsJdbcRepository
            .findAllByFilters(TEST_TUPLE_1_1.getDatabaseId(), null, null, Pageable.ofSize(10))
            .getTotalElements());
    // Does not delete any soft deleted user tables because purgeFromMs
    Assertions.assertDoesNotThrow(
        () ->
            softDeletedUserTableHtsJdbcRepository.deleteByDatabaseIdTableIdPurgeAfterMs(
                TEST_TUPLE_1_1.getDatabaseId(),
                otherTableId,
                Instant.ofEpochMilli(deletedTimestamp).plusSeconds(3601).toEpochMilli()));

    Assertions.assertEquals(
        0,
        softDeletedUserTableHtsJdbcRepository
            .findAllByFilters(
                TEST_TUPLE_1_1.getDatabaseId(), otherTableId, null, Pageable.ofSize(10))
            .getTotalElements());

    Assertions.assertDoesNotThrow(
        () ->
            softDeletedUserTableHtsJdbcRepository.deleteByDatabaseIdTableIdPurgeAfterMs(
                TEST_TUPLE_1_1.getDatabaseId(),
                TEST_TUPLE_1_1.getTableId(),
                Instant.ofEpochMilli(deletedTimestamp).plusSeconds(100000).toEpochMilli()));

    Assertions.assertEquals(
        0,
        softDeletedUserTableHtsJdbcRepository
            .findAllByFilters(TEST_TUPLE_1_1.getDatabaseId(), null, null, Pageable.ofSize(10))
            .getTotalElements());
  }

  @Test
  public void searchSoftDeletedTablesByFilters() {
    softDeletedUserTableHtsJdbcRepository.save(softDeletedUserTableRowTestTuple1_1);
    SoftDeletedUserTableRow secondSoftDeletedUserTableRow =
        SoftDeletedUserTableRow.builder()
            .databaseId(TEST_TUPLE_1_1.getDatabaseId())
            .tableId(TEST_TUPLE_1_1.getTableId() + "_2")
            .version(0L)
            .metadataLocation(TEST_TUPLE_1_1.getTableLoc() + "_2")
            .storageType(TEST_TUPLE_1_1.getStorageType())
            .deletedAtMs(deletedTimestamp)
            .purgeAfterMs(Instant.ofEpochMilli(deletedTimestamp).plusSeconds(7200).toEpochMilli())
            .build();
    softDeletedUserTableHtsJdbcRepository.save(secondSoftDeletedUserTableRow);

    assertThat(
            softDeletedUserTableHtsJdbcRepository
                .findAllByFilters(
                    TEST_TUPLE_1_1.getDatabaseId(),
                    TEST_TUPLE_1_1.getTableId(),
                    null,
                    Pageable.ofSize(10))
                .getTotalElements())
        .isEqualTo(1);

    assertThat(
            softDeletedUserTableHtsJdbcRepository
                .findAllByFilters(TEST_TUPLE_1_1.getDatabaseId(), null, null, Pageable.ofSize(10))
                .getTotalElements())
        .isEqualTo(2);

    // Test filtering by TTL
    assertThat(
            softDeletedUserTableHtsJdbcRepository
                .findAllByFilters(null, null, 0L, Pageable.ofSize(10))
                .getTotalElements())
        .isEqualTo(0);

    assertThat(
            softDeletedUserTableHtsJdbcRepository
                .findAllByFilters(
                    null,
                    null,
                    Instant.ofEpochMilli(deletedTimestamp).plusSeconds(100000).toEpochMilli(),
                    Pageable.ofSize(10))
                .getTotalElements())
        .isEqualTo(2);

    Page<SoftDeletedUserTableRow> page =
        softDeletedUserTableHtsJdbcRepository.findAllByFilters(
            null,
            null,
            Instant.ofEpochMilli(deletedTimestamp).plusSeconds(7000).toEpochMilli(),
            Pageable.ofSize(10));
    assertThat(page.getTotalElements()).isEqualTo(1);
    assertThat(page.get().findFirst().isPresent()).isTrue();
    // Should return the first soft deleted table as it has a TTL of one hour
    assertThat(page.get().findFirst().get().getTableId().equals(TEST_TUPLE_1_1.getTableId()))
        .isTrue();
  }
}
