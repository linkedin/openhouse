package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRow;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.SoftDeletedUserTableHtsJdbcRepository;
import java.sql.Timestamp;
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
          .timeToLive(Timestamp.from(Instant.ofEpochMilli(deletedTimestamp).plusSeconds(3600)))
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

    // Validate is case insensitive
    SoftDeletedUserTableRowPrimaryKey key =
        SoftDeletedUserTableRowPrimaryKey.builder()
            .tableId(TEST_TUPLE_1_1.getTableId().toUpperCase())
            .databaseId(TEST_TUPLE_1_1.getDatabaseId())
            .deletedAtMs(deletedTimestamp)
            .build();

    Assertions.assertDoesNotThrow(() -> softDeletedUserTableHtsJdbcRepository.deleteById(key));
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
            .timeToLive(Timestamp.from(Instant.ofEpochMilli(deletedTimestamp).plusSeconds(7200)))
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
                .findAllByFilters(
                    null, null, Timestamp.from(Instant.ofEpochMilli(0)), Pageable.ofSize(10))
                .getTotalElements())
        .isEqualTo(0);

    assertThat(
            softDeletedUserTableHtsJdbcRepository
                .findAllByFilters(
                    null,
                    null,
                    Timestamp.from(Instant.ofEpochMilli(deletedTimestamp).plusSeconds(100000)),
                    Pageable.ofSize(10))
                .getTotalElements())
        .isEqualTo(2);

    Page<SoftDeletedUserTableRow> page =
        softDeletedUserTableHtsJdbcRepository.findAllByFilters(
            null,
            null,
            Timestamp.from(Instant.ofEpochMilli(deletedTimestamp).plusSeconds(7000)),
            Pageable.ofSize(10));
    assertThat(page.getTotalElements()).isEqualTo(1);
    assertThat(page.get().findFirst().isPresent()).isTrue();
    // Should return the first soft deleted table as it has a TTL of one hour
    assertThat(page.get().findFirst().get().getTableId().equals(TEST_TUPLE_1_1.getTableId()))
        .isTrue();
  }
}
