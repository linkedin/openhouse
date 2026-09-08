package com.linkedin.openhouse.internal.catalog.view;

import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.DB;
import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.VIEW;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.view.model.LoadedView;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Races arbitrated by the single compare-and-swap. A barrier inside the swap puts both threads in
 * the window before either can win, so no outcome depends on scheduling.
 */
public class ViewCommitEngineConcurrencyTest {

  private ViewCommitEngineHarness harness;
  private ExecutorService executor;
  private Path root;

  @BeforeEach
  void setUp(@TempDir Path tempDir) {
    root = tempDir;
    harness = new ViewCommitEngineHarness(tempDir);
    executor = Executors.newFixedThreadPool(2);
  }

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
  }

  /** The advisory occupancy read cannot prevent this race, so the swap has to. */
  @Test
  void concurrentCreatesProduceExactlyOneWinnerAndOnePointer() throws Exception {
    CyclicBarrier bothInsideSwapWindow = new CyclicBarrier(2);
    harness.getHouseTableRepository().setBeforeCas(() -> await(bothInsideSwapWindow));

    ViewCommitIntent first = ViewTestFixtures.createIntent(root);
    ViewCommitIntent second =
        ViewTestFixtures.baseIntent(root)
            .schema(ViewTestFixtures.schemaV2())
            .representations(
                Collections.singletonList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
            .viewUuid(ViewTestFixtures.SECOND_VIEW_UUID)
            .viewLocation(
                ViewTestFixtures.allocatedViewLocation(
                    root, DB, VIEW, ViewTestFixtures.SECOND_VIEW_UUID))
            .build();
    Assertions.assertNotEquals(
        first.getViewLocation(),
        second.getViewLocation(),
        "two service requests allocate two roots, or this test proves nothing about identity");

    Outcome outcome = runBoth(first, second);

    Assertions.assertEquals(1, outcome.successes(), "exactly one create may win");
    Assertions.assertEquals(1, outcome.failures(), "exactly one create must lose");
    Assertions.assertTrue(
        outcome.failure() instanceof AlreadyExistsException,
        "a losing create is a name collision, not a failed commit: " + outcome.failure());

    List<Path> files = harness.metadataFiles();
    Assertions.assertEquals(
        2, files.size(), "both writers wrote a candidate file before racing for the pointer");
    Assertions.assertEquals(
        2,
        files.stream().map(path -> path.getFileName().toString()).distinct().count(),
        "each candidate file name must be unique: " + files);

    HouseTable pointer = harness.getHouseTableRepository().peek(DB, VIEW).get();
    Assertions.assertEquals(
        outcome.success().getPointer().getMetadataLocation(), pointer.getTableLocation());
    Assertions.assertTrue(outcome.success().isCreated());

    // The published metadata carries the winner's supplied identity.
    Assertions.assertEquals(
        outcome.success().getViewUuid(),
        harness.readMetadata(pointer.getTableLocation()).uuid(),
        "the published identity belongs to the request that won the swap");
    Assertions.assertTrue(
        pointer
            .getTableLocation()
            .startsWith(
                ViewTestFixtures.viewLocation(root, DB, VIEW, outcome.success().getViewUuid())),
        "the published file lives under the winner's own allocated root: "
            + pointer.getTableLocation());
  }

  @Test
  void concurrentReplacesFromTheSameBaseLeaveExactlyOneWinner() throws Exception {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));
    String base = created.getPointer().getMetadataLocation();

    CyclicBarrier bothInsideSwapWindow = new CyclicBarrier(2);
    harness.getHouseTableRepository().setBeforeCas(() -> await(bothInsideSwapWindow));

    ViewCommitIntent left =
        ViewTestFixtures.baseIntent(root)
            .schema(ViewTestFixtures.schemaV2())
            .representations(
                Collections.singletonList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
            .baseViewVersion(base)
            .build();
    ViewCommitIntent right =
        ViewTestFixtures.baseIntent(root)
            .representations(
                Collections.singletonList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V3, ViewTestFixtures.SPARK_DIALECT)))
            .baseViewVersion(base)
            .build();

    Outcome outcome = runBoth(left, right);

    Assertions.assertEquals(1, outcome.successes());
    Assertions.assertEquals(1, outcome.failures());
    Assertions.assertTrue(
        outcome.failure() instanceof CommitFailedException,
        "a losing replace is a concurrent-modification failure: " + outcome.failure());

    harness.getHouseTableRepository().setBeforeCas(() -> {});
    HouseTable pointer = harness.getHouseTableRepository().peek(DB, VIEW).get();
    Assertions.assertEquals(
        outcome.success().getPointer().getMetadataLocation(), pointer.getTableLocation());

    LoadedView reloaded = harness.newEngineInstance().loadView(DB, VIEW);
    Assertions.assertEquals(
        outcome.success().getPointer().getMetadataLocation(),
        reloaded.getPointer().getMetadataLocation());
  }

  /** Staling the base up front would reject before the write and assert nothing. */
  @Test
  void aFailedSwapAfterTheCandidateWriteLeavesThatFileUnreachable() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));
    String base = created.getPointer().getMetadataLocation();
    Set<Path> filesBeforeLosingAttempt = new HashSet<>(harness.metadataFiles());
    int savesBeforeLosingAttempt = harness.getHouseTableRepository().getSaveViewCalls();

    AtomicReference<ViewCommitResult> interloper = new AtomicReference<>();
    AtomicReference<HouseTable> pointerAfterInterloper = new AtomicReference<>();
    harness
        .getHouseTableRepository()
        .runOnceBeforeNextCas(
            () -> {
              interloper.set(
                  harness
                      .getViewCommitEngine()
                      .commit(
                          ViewTestFixtures.baseIntent(root)
                              .schema(ViewTestFixtures.schemaV2())
                              .representations(
                                  Collections.singletonList(
                                      ViewTestFixtures.sql(
                                          ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
                              .baseViewVersion(base)
                              .build()));
              pointerAfterInterloper.set(
                  harness.getHouseTableRepository().peek(DB, VIEW).orElse(null));
            });

    Assertions.assertThrows(
        CommitFailedException.class,
        () ->
            harness
                .getViewCommitEngine()
                .commit(
                    ViewTestFixtures.baseIntent(root)
                        .representations(
                            Collections.singletonList(
                                ViewTestFixtures.sql(
                                    ViewTestFixtures.SQL_V3, ViewTestFixtures.SPARK_DIALECT)))
                        .baseViewVersion(base)
                        .build()));

    Assertions.assertNotNull(interloper.get(), "the competing commit must have landed");
    String winnerPath = interloper.get().getPointer().getMetadataLocation();

    // Reaching the swap means the loser had already written its candidate.
    Assertions.assertEquals(
        savesBeforeLosingAttempt + 2,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "the competing commit and the losing attempt each published exactly once");

    List<Path> orphaned =
        harness.metadataFiles().stream()
            .filter(path -> !filesBeforeLosingAttempt.contains(path))
            .filter(path -> !path.toString().equals(winnerPath))
            .collect(Collectors.toList());
    Assertions.assertEquals(
        1, orphaned.size(), "the loser must leave exactly one unreferenced candidate: " + orphaned);

    HouseTable pointerNow = harness.getHouseTableRepository().peek(DB, VIEW).get();
    Assertions.assertEquals(winnerPath, pointerNow.getTableLocation());
    Assertions.assertNotEquals(orphaned.get(0).toString(), pointerNow.getTableLocation());
    Assertions.assertEquals(
        pointerAfterInterloper.get(),
        pointerNow,
        "the losing attempt must not have altered the pointer row in any way");

    // A fresh engine resolves the winner's version.
    LoadedView reloaded = harness.newEngineInstance().loadView(DB, VIEW);
    Assertions.assertEquals(winnerPath, reloaded.getPointer().getMetadataLocation());
  }

  private Outcome runBoth(ViewCommitIntent first, ViewCommitIntent second) throws Exception {
    AtomicReference<ViewCommitResult> success = new AtomicReference<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();

    Callable<Void> left = commitTask(first, success, failure);
    Callable<Void> right = commitTask(second, success, failure);

    Future<Void> leftFuture = executor.submit(left);
    Future<Void> rightFuture = executor.submit(right);
    leftFuture.get(60, TimeUnit.SECONDS);
    rightFuture.get(60, TimeUnit.SECONDS);

    return new Outcome(success.get(), failure.get());
  }

  private Callable<Void> commitTask(
      ViewCommitIntent intent,
      AtomicReference<ViewCommitResult> success,
      AtomicReference<Throwable> failure) {
    return () -> {
      try {
        ViewCommitResult result = harness.getViewCommitEngine().commit(intent);
        Assertions.assertTrue(success.compareAndSet(null, result), "more than one commit won");
      } catch (Throwable t) {
        Assertions.assertTrue(failure.compareAndSet(null, t), "more than one commit lost");
      }
      return null;
    };
  }

  private static void await(CyclicBarrier barrier) {
    try {
      barrier.await(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (BrokenBarrierException | java.util.concurrent.TimeoutException e) {
      throw new IllegalStateException(e);
    }
  }

  private static final class Outcome {
    private final ViewCommitResult success;
    private final Throwable failure;

    Outcome(ViewCommitResult success, Throwable failure) {
      this.success = success;
      this.failure = failure;
    }

    ViewCommitResult success() {
      return success;
    }

    Throwable failure() {
      return failure;
    }

    int successes() {
      return success == null ? 0 : 1;
    }

    int failures() {
      return failure == null ? 0 : 1;
    }
  }
}
