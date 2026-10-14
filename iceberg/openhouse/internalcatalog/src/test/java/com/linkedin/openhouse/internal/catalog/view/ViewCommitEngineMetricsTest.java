package com.linkedin.openhouse.internal.catalog.view;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.view.ViewMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

class ViewCommitEngineMetricsTest {
  @TempDir Path root;

  private final MockClock clock = new MockClock();
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);
  private final HouseTableRepository repository = mock(HouseTableRepository.class);
  private final FileIOManager fileIOManager = mock(FileIOManager.class);
  private final FileIO fileIO = mock(FileIO.class);
  private final ViewMetadataCodec codec = mock(ViewMetadataCodec.class);
  private final InputFile inputFile = mock(InputFile.class);
  private final OutputFile outputFile = mock(OutputFile.class);
  private ViewCommitEngine engine;
  private ViewMetadata storedMetadata;
  private HouseTable storedRow;

  @BeforeEach
  void setUp() {
    engine =
        new ViewCommitEngineImpl(
            repository,
            fileIOManager,
            codec,
            new StorageType(),
            new MetricsReporter(registry, "catalog", Collections.emptyList()));
    when(fileIOManager.getFileIO(StorageType.LOCAL)).thenReturn(fileIO);
    when(fileIO.newInputFile(anyString()))
        .thenAnswer(
            invocation -> {
              clock.add(Duration.ofMillis(2));
              return inputFile;
            });
    when(fileIO.newOutputFile(anyString()))
        .thenAnswer(
            invocation -> {
              clock.add(Duration.ofMillis(3));
              return outputFile;
            });
    when(codec.read(inputFile))
        .thenAnswer(
            invocation -> {
              clock.add(Duration.ofMillis(5));
              return storedMetadata;
            });
    doAnswer(
            invocation -> {
              clock.add(Duration.ofMillis(7));
              storedMetadata = invocation.getArgument(0);
              return null;
            })
        .when(codec)
        .write(any(ViewMetadata.class), any(OutputFile.class));
    when(repository.saveView(any(HouseTable.class)))
        .thenAnswer(
            invocation -> {
              clock.add(Duration.ofMillis(11));
              HouseTable proposed = invocation.getArgument(0);
              storedRow = proposed.toBuilder().entityType("VIEW").build();
              return storedRow;
            });
  }

  @AfterEach
  void closeRegistry() {
    registry.close();
  }

  @Test
  void createTimesWriteAndPublicationWithoutReading() {
    ViewCommitResult result = engine.commit(ViewTestFixtures.createIntent(root, null));
    assertEquals(storedRow.getTableLocation(), result.getPointer().getMetadataLocation());
    assertTimer("commit_latency", 1, 21);
    assertTimer("metadata_update_latency", 1, 10);
    assertAbsent("metadata_retrieval_latency", "load_latency");
    InOrder order = inOrder(codec, repository);
    order.verify(codec).write(any(), any());
    order.verify(repository).saveView(any());
    order.verifyNoMoreInteractions();
    verify(repository, never()).findViewById(any());
    verify(repository, never()).findEntityById(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void replacementsTimeSingleCapturedReadAndWriteOnlyWhenChanged(boolean changed) {
    createAndReset();
    HouseTable captured = storedRow;
    ViewCommitResult result =
        engine.commit(
            changed
                ? ViewTestFixtures.changedReplaceIntent(root, captured).build()
                : ViewTestFixtures.replaceIntent(root, captured));
    assertEquals(changed, result.isMetadataChanged());
    assertTimer("commit_latency", 1, changed ? 28 : 7);
    assertTimer("metadata_retrieval_latency", 1, 7);
    if (changed) {
      assertTimer("metadata_update_latency", 1, 10);
      verify(repository, times(1)).saveView(any());
    } else {
      assertAbsent("metadata_update_latency");
      verify(repository, never()).saveView(any());
    }
    verify(fileIO, times(1)).newInputFile(captured.getTableLocation());
    verify(codec, times(1)).read(inputFile);
    verify(repository, never()).findViewById(any());
    verify(repository, never()).findEntityById(any());
  }

  /** Input-file creation is inside the retrieval timer, so its failure is timed as a read. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void replacementReadFailureRecordsCommitAndReadWithoutWritingOrPublishing(
      boolean failInFileCreation) {
    createAndReset();
    IllegalStateException failure = new IllegalStateException("read failed");
    if (failInFileCreation) {
      when(fileIO.newInputFile(anyString()))
          .thenAnswer(
              invocation -> {
                clock.add(Duration.ofMillis(2));
                throw failure;
              });
    } else {
      when(codec.read(inputFile))
          .thenAnswer(
              invocation -> {
                clock.add(Duration.ofMillis(5));
                throw failure;
              });
    }
    HouseTable captured = storedRow;
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () -> engine.commit(ViewTestFixtures.changedReplaceIntent(root, captured).build())));
    long readMillis = failInFileCreation ? 2 : 7;
    assertTimer("commit_latency", 1, readMillis);
    assertTimer("metadata_retrieval_latency", 1, readMillis);
    assertAbsent("metadata_update_latency", "load_latency");
    verify(fileIO, times(1)).newInputFile(captured.getTableLocation());
    verify(codec, times(failInFileCreation ? 0 : 1)).read(inputFile);
    verify(fileIO, never()).newOutputFile(anyString());
    verify(codec, never()).write(any(), any());
    verifyNoInteractions(repository);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void writeFailureIsTimedWithoutPublishingOrSwallowing(boolean failInFileCreation) {
    IllegalStateException failure = new IllegalStateException("write failed");
    if (failInFileCreation) {
      when(fileIO.newOutputFile(anyString()))
          .thenAnswer(
              invocation -> {
                clock.add(Duration.ofMillis(3));
                throw failure;
              });
    } else {
      doAnswer(
              invocation -> {
                clock.add(Duration.ofMillis(7));
                throw failure;
              })
          .when(codec)
          .write(any(), any());
    }
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () -> engine.commit(ViewTestFixtures.createIntent(root, null))));
    long writeMillis = failInFileCreation ? 3 : 10;
    assertTimer("commit_latency", 1, writeMillis);
    assertTimer("metadata_update_latency", 1, writeMillis);
    verifyNoInteractions(repository);
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void publicationFailuresRetainTranslationAndSingleAttempt(boolean unknown, boolean create) {
    createAndReset();
    RuntimeException failure =
        unknown
            ? new HouseTableRepositoryStateUnknownException("unknown", new RuntimeException())
            : new HouseTableConcurrentUpdateException("conflict", new RuntimeException());
    when(repository.saveView(any()))
        .thenAnswer(
            invocation -> {
              clock.add(Duration.ofMillis(11));
              throw failure;
            });
    Class<? extends RuntimeException> expected =
        unknown
            ? CommitStateUnknownException.class
            : create ? AlreadyExistsException.class : CommitFailedException.class;
    RuntimeException thrown =
        assertThrows(
            expected,
            () ->
                engine.commit(
                    create
                        ? ViewTestFixtures.createIntent(root, null)
                        : ViewTestFixtures.changedReplaceIntent(root, storedRow).build()));
    assertSame(failure, thrown.getCause());
    assertTimer("commit_latency", 1, create ? 21 : 28);
    if (create) {
      assertAbsent("metadata_retrieval_latency");
    } else {
      assertTimer("metadata_retrieval_latency", 1, 7);
    }
    assertTimer("metadata_update_latency", 1, 10);
    verify(repository, times(1)).saveView(any());
    verify(repository, never()).findViewById(any());
    verify(repository, never()).findEntityById(any());
  }

  @Test
  void validationAndOccupiedNameRecordTotalButNoStorage() {
    assertThrows(
        BadRequestException.class,
        () -> engine.commit(ViewTestFixtures.baseIntent(root, null, null).build()));
    assertThrows(
        AlreadyExistsException.class,
        () ->
            engine.commit(ViewTestFixtures.createIntent(root, ViewTestFixtures.tableRow("/base"))));
    assertTimer("commit_latency", 2, 0);
    assertAbsent("metadata_retrieval_latency", "metadata_update_latency", "load_latency");
    verifyNoInteractions(repository, fileIOManager, codec);
  }

  private void createAndReset() {
    engine.commit(ViewTestFixtures.createIntent(root, null));
    registry.clear();
    clearInvocations(repository, fileIOManager, fileIO, codec);
  }

  private void assertAbsent(String... suffixes) {
    for (String suffix : suffixes) {
      assertNull(registry.find("catalog_view_" + suffix).timer());
    }
  }

  private void assertTimer(String suffix, long count, long millis) {
    Timer timer = registry.find("catalog_view_" + suffix).timer();
    assertNotNull(timer);
    assertEquals(count, timer.count());
    assertEquals((double) millis, timer.totalTime(TimeUnit.MILLISECONDS), 0.000001);
    assertFalse(timer.getId().getTags().iterator().hasNext(), "No per-entity metric tags");
  }
}
