package com.linkedin.openhouse.jobs.spark;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pure-Java unit tests for {@link BatchedOrphanFilesDeletionSparkApp#buildEntries}. No Spark
 * session, no HTTP — exercises the CLI-parsing edges that decide whether the app can even start.
 */
public class BatchedOrphanFilesDeletionSparkAppArgsTest {

  @Test
  public void buildEntriesParsesParallelLists() {
    List<BatchedOrphanFilesDeletionSparkApp.BatchEntry> entries =
        BatchedOrphanFilesDeletionSparkApp.buildEntries(
            "db1.t1,db2.t2", "op-1,op-2", "uuid-1,uuid-2");

    Assertions.assertEquals(2, entries.size());
    Assertions.assertEquals("db1.t1", entries.get(0).getFqtn());
    Assertions.assertEquals("db1", entries.get(0).getDatabaseName());
    Assertions.assertEquals("t1", entries.get(0).getTableName());
    Assertions.assertEquals("op-1", entries.get(0).getOperationId());
    Assertions.assertEquals("uuid-1", entries.get(0).getTableUuid());
    Assertions.assertEquals("db2.t2", entries.get(1).getFqtn());
    Assertions.assertEquals("op-2", entries.get(1).getOperationId());
  }

  @Test
  public void buildEntriesTrimsWhitespaceInEachEntry() {
    List<BatchedOrphanFilesDeletionSparkApp.BatchEntry> entries =
        BatchedOrphanFilesDeletionSparkApp.buildEntries(
            " db1.t1 , db2.t2 ", " op-1 , op-2 ", " uuid-1 , uuid-2 ");

    Assertions.assertEquals("db1.t1", entries.get(0).getFqtn());
    Assertions.assertEquals("op-1", entries.get(0).getOperationId());
    Assertions.assertEquals("uuid-1", entries.get(0).getTableUuid());
  }

  @Test
  public void buildEntriesRejectsMismatchedLengths() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            BatchedOrphanFilesDeletionSparkApp.buildEntries("db.a,db.b", "op-1", "uuid-1,uuid-2"));
  }

  @Test
  public void buildEntriesRejectsNullArguments() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> BatchedOrphanFilesDeletionSparkApp.buildEntries(null, "op-1", "uuid-1"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> BatchedOrphanFilesDeletionSparkApp.buildEntries("db.a", null, "uuid-1"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> BatchedOrphanFilesDeletionSparkApp.buildEntries("db.a", "op-1", null));
  }

  @Test
  public void buildEntriesRejectsEmptyStrings() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> BatchedOrphanFilesDeletionSparkApp.buildEntries("", "op-1", "uuid-1"));
  }

  @Test
  public void buildEntriesRejectsNonFqtn() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> BatchedOrphanFilesDeletionSparkApp.buildEntries("just_a_table", "op-1", "uuid-1"));
  }
}
