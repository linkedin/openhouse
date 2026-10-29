package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.OtelEmitter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class OperationsFileDeletionMetricsTest {
  private static final String LOCATION = "/warehouse/db/table";
  private final SparkSession spark = Mockito.mock(SparkSession.class);
  private final SparkActions actions = Mockito.mock(SparkActions.class, Mockito.RETURNS_DEEP_STUBS);
  private DeleteOrphanFiles action;
  private final DeleteOrphanFiles.Result result = Mockito.mock(DeleteOrphanFiles.Result.class);
  private final FileSystem fs = Mockito.mock(FileSystem.class);
  private final Table table = Mockito.mock(Table.class);
  private final AtomicReference<Consumer<String>> deleteHandler = new AtomicReference<>();
  private final Operations.FileDeletionMetrics metrics = new Operations.FileDeletionMetrics();
  private Operations ops;

  @BeforeEach
  void setUp() throws IOException {
    ops = Mockito.spy(Operations.of(spark, Mockito.mock(OtelEmitter.class)));
    Mockito.doReturn(fs).when(ops).fs();
    Mockito.when(table.location()).thenReturn(LOCATION);
    action = actions.deleteOrphanFiles(table);
    Mockito.when(action.option(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
        .thenReturn(action);
    Mockito.when(action.olderThan(ArgumentMatchers.anyLong())).thenReturn(action);
    Mockito.when(action.deleteWith(ArgumentMatchers.any()))
        .thenAnswer(
            invocation -> {
              deleteHandler.set(invocation.getArgument(0));
              return action;
            });
    Mockito.when(fs.delete(ArgumentMatchers.any(Path.class), ArgumentMatchers.eq(false)))
        .thenReturn(true);
    Mockito.when(fs.getFileStatus(ArgumentMatchers.any(Path.class)))
        .thenAnswer(
            invocation -> new FileStatus(100, false, 1, 1024, 0, invocation.getArgument(0)));
    Mockito.when(fs.rename(ArgumentMatchers.any(Path.class), ArgumentMatchers.any(Path.class)))
        .thenReturn(true);
    Mockito.when(result.orphanFileLocations())
        .thenReturn(Collections.singletonList("sampled-path"));
  }

  @Test
  void countsSuccessfulDeletesButNotBackupsOrSkippedFiles() throws IOException {
    String deletedData = LOCATION + "/data/deleted/file.parquet";
    String deletedManifest = LOCATION + "/metadata/old.avro";
    String backedUpData = LOCATION + "/data/backed-up/file.parquet";
    String metadata = LOCATION + "/metadata/version.metadata.json";
    String backup = LOCATION + "/.backup/data/file.parquet";
    Mockito.when(fs.globStatus(new Path(LOCATION + "/.backup/data/backed-up/data_manifest*")))
        .thenReturn(new FileStatus[] {new FileStatus()});
    executeFiles(Arrays.asList(deletedData, deletedManifest, backedUpData, metadata, backup));

    Assertions.assertSame(result, runWithMetrics());

    Assertions.assertEquals(2, metrics.getDeletedFiles());
    Assertions.assertEquals(200, metrics.getDeletedBytes());
    Assertions.assertEquals(0, metrics.getUnknownFileSizes());
    Mockito.verify(fs)
        .rename(
            new Path(backedUpData), new Path(LOCATION + "/.backup/data/backed-up/file.parquet"));
    for (String skipped : Arrays.asList(backedUpData, metadata, backup)) {
      Mockito.verify(fs, Mockito.never()).delete(new Path(skipped), false);
      Mockito.verify(fs, Mockito.never()).getFileStatus(new Path(skipped));
    }
  }

  @Test
  void failedDeletesAreNotReclaimedAndMissingSizesAreExplicit() throws IOException {
    String deleted = LOCATION + "/metadata/deleted.avro";
    String failed = LOCATION + "/metadata/failed.avro";
    String absent = LOCATION + "/metadata/absent.avro";
    String unknownSize = LOCATION + "/metadata/unknown.avro";
    Mockito.when(fs.delete(new Path(failed), false)).thenThrow(new IOException("Delete failed"));
    Mockito.when(fs.delete(new Path(absent), false)).thenReturn(false);
    Mockito.when(fs.getFileStatus(new Path(unknownSize)))
        .thenThrow(new IOException("Size lookup failed"));
    executeFiles(Arrays.asList(deleted, failed, absent, unknownSize));

    runWithMetrics();

    Assertions.assertEquals(2, metrics.getDeletedFiles());
    Assertions.assertEquals(100, metrics.getDeletedBytes());
    Assertions.assertEquals(1, metrics.getUnknownFileSizes());
    Mockito.verify(fs).delete(new Path(unknownSize), false);
  }

  @Test
  void partialProgressSurvivesActionFailure() {
    String deleted = LOCATION + "/metadata/deleted.avro";
    Mockito.when(action.execute())
        .thenAnswer(
            invocation -> {
              deleteHandler.get().accept(deleted);
              throw new IllegalStateException("Action failed after deletion");
            });

    Assertions.assertThrows(IllegalStateException.class, this::runWithMetrics);

    Assertions.assertEquals(1, metrics.getDeletedFiles());
    Assertions.assertEquals(100, metrics.getDeletedBytes());
  }

  @Test
  void legacyCallPreservesResultsWithoutAdditionalSizeLookups() throws IOException {
    String deleted = LOCATION + "/metadata/deleted.avro";
    executeFiles(Collections.singletonList(deleted));
    try (MockedStatic<SparkActions> sparkActions = Mockito.mockStatic(SparkActions.class)) {
      sparkActions.when(() -> SparkActions.get(spark)).thenReturn(actions);

      Assertions.assertSame(result, ops.deleteOrphanFiles(table, 1, ".backup", 1, true, 1));
    }

    Mockito.verify(fs).delete(new Path(deleted), false);
    Mockito.verify(fs, Mockito.never()).getFileStatus(ArgumentMatchers.any(Path.class));
  }

  @Test
  void noOrphansProducesZeroReclamation() {
    executeFiles(Collections.emptyList());

    runWithMetrics();

    Assertions.assertEquals(0, metrics.getDeletedFiles());
    Assertions.assertEquals(0, metrics.getDeletedBytes());
    Assertions.assertEquals(0, metrics.getUnknownFileSizes());
    Mockito.verifyNoInteractions(fs);
  }

  @Test
  void concurrentDeleteCallbacksDoNotLoseCounts() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      Callable<Void> deletions =
          () -> {
            for (int i = 0; i < 1000; i++) {
              metrics.recordDeletion(OptionalLong.of(10));
              metrics.recordDeletion(OptionalLong.empty());
            }
            return null;
          };
      List<Future<Void>> tasks =
          pool.invokeAll(Arrays.asList(deletions, deletions, deletions, deletions));
      for (Future<Void> task : tasks) {
        task.get();
      }
    } finally {
      pool.shutdownNow();
    }

    Assertions.assertEquals(8000, metrics.getDeletedFiles());
    Assertions.assertEquals(40000, metrics.getDeletedBytes());
    Assertions.assertEquals(4000, metrics.getUnknownFileSizes());
  }

  private void executeFiles(List<String> files) {
    Mockito.when(action.execute())
        .thenAnswer(
            invocation -> {
              files.forEach(deleteHandler.get());
              return result;
            });
  }

  private DeleteOrphanFiles.Result runWithMetrics() {
    try (MockedStatic<SparkActions> sparkActions = Mockito.mockStatic(SparkActions.class)) {
      sparkActions.when(() -> SparkActions.get(spark)).thenReturn(actions);
      return ops.deleteOrphanFiles(table, 1, ".backup", 1, true, 1, metrics);
    }
  }
}
