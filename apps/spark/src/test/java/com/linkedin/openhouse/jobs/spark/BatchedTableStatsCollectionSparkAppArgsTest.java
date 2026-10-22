package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.jobs.util.AppConstants;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pure-Java unit tests for {@link BatchedTableStatsCollectionSparkApp#buildEntries}. No Spark
 * session, no HTTP — exercises the CLI-parsing edges that decide whether the app can even start.
 */
public class BatchedTableStatsCollectionSparkAppArgsTest {

  @Test
  public void buildEntriesParsesParallelLists() {
    List<BatchedTableStatsCollectionSparkApp.BatchEntry> entries =
        BatchedTableStatsCollectionSparkApp.buildEntries(
            "db1.t1,db2.t2", "op-1,op-2", "uuid-1,uuid-2");

    Assertions.assertEquals(2, entries.size());
    Assertions.assertEquals("db1.t1", entries.get(0).getFqtn());
    Assertions.assertEquals("db1", entries.get(0).getDatabaseName());
    Assertions.assertEquals("t1", entries.get(0).getTableName());
    Assertions.assertEquals(Optional.of("op-1"), entries.get(0).getOperationId());
    Assertions.assertEquals(Optional.of("uuid-1"), entries.get(0).getTableUuid());
    Assertions.assertEquals("db2.t2", entries.get(1).getFqtn());
    Assertions.assertEquals(Optional.of("op-2"), entries.get(1).getOperationId());
  }

  @Test
  public void buildEntriesTrimsWhitespaceInEachEntry() {
    List<BatchedTableStatsCollectionSparkApp.BatchEntry> entries =
        BatchedTableStatsCollectionSparkApp.buildEntries(
            " db1.t1 , db2.t2 ", " op-1 , op-2 ", " uuid-1 , uuid-2 ");

    Assertions.assertEquals("db1.t1", entries.get(0).getFqtn());
    Assertions.assertEquals(Optional.of("op-1"), entries.get(0).getOperationId());
    Assertions.assertEquals(Optional.of("uuid-1"), entries.get(0).getTableUuid());
  }

  @Test
  public void buildEntriesAllowsMissingOptionalParallelLists() {
    List<BatchedTableStatsCollectionSparkApp.BatchEntry> entries =
        BatchedTableStatsCollectionSparkApp.buildEntries("db1.t1,db2.t2", null, null);

    Assertions.assertEquals(2, entries.size());
    Assertions.assertEquals(Optional.empty(), entries.get(0).getOperationId());
    Assertions.assertEquals(Optional.empty(), entries.get(0).getTableUuid());
  }

  @Test
  public void buildEntriesRejectsMismatchedLengths() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            BatchedTableStatsCollectionSparkApp.buildEntries("db.a,db.b", "op-1", "uuid-1,uuid-2"));
  }

  @Test
  public void buildEntriesRejectsNullOrEmptyTableNames() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> BatchedTableStatsCollectionSparkApp.buildEntries(null, "op-1", "uuid-1"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> BatchedTableStatsCollectionSparkApp.buildEntries("", "op-1", "uuid-1"));
  }

  @Test
  public void buildEntriesRejectsNonQualifiedTableName() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> BatchedTableStatsCollectionSparkApp.buildEntries("not_qualified", null, null));
  }

  @Test
  public void buildEntriesRejectsBatchExceedingMaxSize() {
    String tooMany =
        IntStream.rangeClosed(0, AppConstants.STATS_MAX_BATCH_SIZE)
            .mapToObj(i -> "db.t" + i)
            .collect(Collectors.joining(","));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> BatchedTableStatsCollectionSparkApp.buildEntries(tooMany, null, null));
  }
}
