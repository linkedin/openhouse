package com.linkedin.openhouse.internal.catalog.view;

import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.DB;
import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.VIEW;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.view.model.LoadedView;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitOperation;
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

  /** The upstream neutral lookup a caller performs once, before invoking the engine. */
  private HouseTable captureBase() {
    return harness
        .getHouseTableRepository()
        .findEntityById(ViewTestFixtures.key(DB, VIEW))
        .orElse(null);
  }

  /** The swap is the only arbiter, so two creates that both captured absence still leave one. */
  @Test
  void concurrentCreatesProduceExactlyOneWinnerAndOnePointer() throws Exception {
    CyclicBarrier bothInsideSwapWindow = new CyclicBarrier(2);
    harness.getHouseTableRepository().setBeforeCas(() -> await(bothInsideSwapWindow));

    // Both requests captured absence and kept distinct prepared identities and roots.
    ViewCommitIntent first = ViewTestFixtures.createIntent(root, null);
    ViewCommitIntent second =
        ViewTestFixtures.baseIntent(root, ViewCommitOperation.CREATE, null)
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
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    // Both replacements share one intentionally captured VIEW snapshot.
    HouseTable base = captureBase();

    CyclicBarrier bothInsideSwapWindow = new CyclicBarrier(2);
    harness.getHouseTableRepository().setBeforeCas(() -> await(bothInsideSwapWindow));

    ViewCommitIntent left =
        ViewTestFixtures.baseIntent(root, ViewCommitOperation.REPLACE, base)
            .schema(ViewTestFixtures.schemaV2())
            .representations(
                Collections.singletonList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
            .build();
    ViewCommitIntent right =
        ViewTestFixtures.baseIntent(root, ViewCommitOperation.REPLACE, base)
            .representations(
                Collections.singletonList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V3, ViewTestFixtures.SPARK_DIALECT)))
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

  /** No pre-write compare any more: the loser writes its candidate, then loses at the swap. */
  @Test
  void aFailedSwapAfterTheCandidateWriteLeavesThatFileUnreachable() {
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureBase();
    Set<Path> filesBeforeLosingAttempt = new HashSet<>(harness.metadataFiles());
    int savesBeforeLosingAttempt = harness.getHouseTableRepository().getSaveViewCalls();

    // Both are prebuilt from the shared base, so the callback captures nothing mid-swap.
    ViewCommitIntent interloperIntent =
        ViewTestFixtures.baseIntent(root, ViewCommitOperation.REPLACE, base)
            .schema(ViewTestFixtures.schemaV2())
            .representations(
                Collections.singletonList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
            .build();
    ViewCommitIntent loserIntent =
        ViewTestFixtures.baseIntent(root, ViewCommitOperation.REPLACE, base)
            .representations(
                Collections.singletonList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V3, ViewTestFixtures.SPARK_DIALECT)))
            .build();

    AtomicReference<ViewCommitResult> interloper = new AtomicReference<>();
    AtomicReference<HouseTable> pointerAfterInterloper = new AtomicReference<>();
    harness
        .getHouseTableRepository()
        .runOnceBeforeNextCas(
            () -> {
              interloper.set(harness.getViewCommitEngine().commit(interloperIntent));
              pointerAfterInterloper.set(
                  harness.getHouseTableRepository().peek(DB, VIEW).orElse(null));
            });

    Assertions.assertThrows(
        CommitFailedException.class, () -> harness.getViewCommitEngine().commit(loserIntent));

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

  /** A create that captured absence still loses at the swap when the name is taken before the PUT. */
  @Test
  void aCreateThatCapturedAbsenceLosesAtTheSwapWhenTheNameIsTaken() {
    HouseTable rival = ViewTestFixtures.viewRow("/rival/00001-rival.metadata.json");
    harness
        .getHouseTableRepository()
        .runOnceBeforeNextCas(() -> harness.getHouseTableRepository().seed(rival));

    AlreadyExistsException thrown =
        Assertions.assertThrows(
            AlreadyExistsException.class,
            () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null)));

    Assertions.assertEquals("View already exists: " + DB + "." + VIEW, thrown.getMessage());
    Assertions.assertEquals(
        1,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "exactly one swap is attempted, and it is not retried");
    Assertions.assertEquals(
        1, harness.metadataFiles().size(), "the losing candidate is written and left in place");
    Assertions.assertEquals(
        "/rival/00001-rival.metadata.json",
        harness.getHouseTableRepository().peek(DB, VIEW).get().getTableLocation(),
        "the rival that won the name is left untouched");
  }

  /** The base is deleted before the swap: the changed replace loses and never recreates it. */
  @Test
  void aChangedReplaceLosesTheSwapWhenItsBaseIsDeletedBeforeThePut() {
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureBase();
    int filesBefore = harness.metadataFiles().size();
    ViewCommitIntent loser =
        ViewTestFixtures.baseIntent(root, ViewCommitOperation.REPLACE, base)
            .schema(ViewTestFixtures.schemaV2())
            .representations(
                Collections.singletonList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
            .build();
    harness
        .getHouseTableRepository()
        .runOnceBeforeNextCas(() -> harness.getViewCommitEngine().dropView(DB, VIEW));

    CommitFailedException thrown =
        Assertions.assertThrows(
            CommitFailedException.class, () -> harness.getViewCommitEngine().commit(loser));
    Assertions.assertEquals(
        "Cannot replace view " + DB + "." + VIEW + ": it was modified concurrently",
        thrown.getMessage());

    Assertions.assertEquals(
        filesBefore + 1,
        harness.metadataFiles().size(),
        "the losing candidate is written and retained, never cleaned up");
    Assertions.assertFalse(
        harness.getHouseTableRepository().peek(DB, VIEW).isPresent(),
        "the delete stands; a losing replace never resurrects the row");
  }

  /** Deleted and recreated before the swap: the loser must not promote its stale base. */
  @Test
  void aChangedReplaceLosesTheSwapWhenItsBaseIsDeletedAndRecreatedBeforeThePut() {
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureBase();
    ViewCommitIntent loser =
        ViewTestFixtures.baseIntent(root, ViewCommitOperation.REPLACE, base)
            .schema(ViewTestFixtures.schemaV2())
            .representations(
                Collections.singletonList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
            .build();
    AtomicReference<HouseTable> recreated = new AtomicReference<>();
    harness
        .getHouseTableRepository()
        .runOnceBeforeNextCas(
            () -> {
              harness.getViewCommitEngine().dropView(DB, VIEW);
              harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
              recreated.set(harness.getHouseTableRepository().peek(DB, VIEW).orElse(null));
            });

    Assertions.assertThrows(
        CommitFailedException.class, () -> harness.getViewCommitEngine().commit(loser));

    Assertions.assertNotNull(recreated.get(), "a fresh row must have been recreated at the key");
    HouseTable now = harness.getHouseTableRepository().peek(DB, VIEW).get();
    Assertions.assertEquals(recreated.get(), now, "the recreated row must be left exactly as it was");
    Assertions.assertNotEquals(
        base.getTableLocation(),
        now.getTableLocation(),
        "the loser must never promote its stale base over the recreated row");
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
