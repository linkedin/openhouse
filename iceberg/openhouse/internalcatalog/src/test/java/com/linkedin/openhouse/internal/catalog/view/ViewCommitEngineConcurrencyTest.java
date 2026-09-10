package com.linkedin.openhouse.internal.catalog.view;

import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.DB;
import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.VIEW;

import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
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
    // Both callers looked up the name and found absence before racing.
    HouseTable capturedAbsence = captureBase();
    Assertions.assertNull(capturedAbsence, "both creates captured absence before racing");
    int readsAfterCapture = harness.readCalls();

    CyclicBarrier bothInsideSwapWindow = new CyclicBarrier(2);
    harness.getHouseTableRepository().setBeforeCas(() -> await(bothInsideSwapWindow));

    // They keep distinct prepared identities and roots.
    ViewCommitIntent first = ViewTestFixtures.createIntent(root, capturedAbsence);
    ViewCommitIntent second =
        ViewTestFixtures.baseIntent(root, Boolean.TRUE, capturedAbsence)
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

    int savesBefore = harness.getHouseTableRepository().getSaveViewCalls();
    int eventBaseline = harness.events().size();
    Outcome outcome = runBoth(first, second);

    Assertions.assertEquals(1, outcome.successes(), "exactly one create may win");
    Assertions.assertEquals(1, outcome.failures(), "exactly one create must lose");
    Assertions.assertTrue(
        outcome.failure() instanceof AlreadyExistsException,
        "a losing create is a name collision, not a failed commit: " + outcome.failure());

    // Neither racing commit read House Table; both wrote a candidate before the swap.
    Assertions.assertEquals(
        readsAfterCapture,
        harness.readCalls(),
        "the racing creates classify their captured absence and read nothing");
    Assertions.assertEquals(
        2, harness.codecWrites(), "both writers built one candidate: " + harness.events());

    // The commit-window suffix: no reads, two candidates, two INITIAL saves.
    List<String> raceSuffix = suffixSince(eventBaseline);
    Assertions.assertEquals(
        0, htsReadEvents(raceSuffix), "no HTS read during the race: " + raceSuffix);
    Assertions.assertEquals(
        0,
        countEvents(raceSuffix, RecordingViewMetadataCodec.READ),
        "creates read no prior file: " + raceSuffix);
    Assertions.assertEquals(
        2,
        countEvents(raceSuffix, RecordingViewMetadataCodec.WRITE),
        "both wrote one candidate: " + raceSuffix);
    Assertions.assertEquals(
        2,
        countEvents(raceSuffix, InMemoryViewHouseTableRepository.SAVE_VIEW),
        "two save attempts: " + raceSuffix);
    Assertions.assertEquals(
        2,
        saveCountCarryingToken(raceSuffix, CatalogConstants.INITIAL_VERSION),
        "both saves are INITIAL claims: " + raceSuffix);
    Assertions.assertEquals(
        savesBefore + 2,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "each racing create attempted exactly one swap");

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
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int filesAfterCreate = harness.metadataFiles().size();
    int writesAfterCreate = harness.codecWrites();
    int readsAfterCapture = harness.readCalls();

    CyclicBarrier bothInsideSwapWindow = new CyclicBarrier(2);
    harness.getHouseTableRepository().setBeforeCas(() -> await(bothInsideSwapWindow));

    ViewCommitIntent left = ViewTestFixtures.changedReplaceIntent(root, base).build();
    ViewCommitIntent right =
        ViewTestFixtures.baseIntent(root, Boolean.FALSE, base)
            .representations(
                Collections.singletonList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V3, ViewTestFixtures.SPARK_DIALECT)))
            .build();

    int eventBaseline = harness.events().size();
    Outcome outcome = runBoth(left, right);

    Assertions.assertEquals(1, outcome.successes());
    Assertions.assertEquals(1, outcome.failures());
    Assertions.assertTrue(
        outcome.failure() instanceof CommitFailedException,
        "a losing replace is a concurrent-modification failure: " + outcome.failure());

    // One save attempt each, one candidate each, both retained; neither read House Table.
    Assertions.assertEquals(
        savesAfterCreate + 2,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "each replacement attempted exactly one swap");
    Assertions.assertEquals(
        writesAfterCreate + 2,
        harness.codecWrites(),
        "each replacement wrote exactly one candidate: " + harness.events());
    Assertions.assertEquals(
        readsAfterCapture,
        harness.readCalls(),
        "both replacements work from the shared captured snapshot and read nothing");

    // The commit-window suffix: no HTS read, both read A once, two candidates, both saves token A.
    List<String> raceSuffix = suffixSince(eventBaseline);
    Assertions.assertEquals(
        0, htsReadEvents(raceSuffix), "no HTS read during the race: " + raceSuffix);
    Assertions.assertEquals(
        2,
        countEvents(raceSuffix, RecordingViewMetadataCodec.READ),
        "both replacements read the captured file once: " + raceSuffix);
    Assertions.assertEquals(
        2,
        readCountOfPath(raceSuffix, base.getTableLocation()),
        "both codec reads are of the captured A path: " + raceSuffix);
    Assertions.assertEquals(
        2,
        countEvents(raceSuffix, RecordingViewMetadataCodec.WRITE),
        "both wrote one candidate: " + raceSuffix);
    Assertions.assertEquals(
        2,
        countEvents(raceSuffix, InMemoryViewHouseTableRepository.SAVE_VIEW),
        "two save attempts: " + raceSuffix);
    Assertions.assertEquals(
        2,
        saveCountCarryingToken(raceSuffix, base.getTableLocation()),
        "both saves carry A's captured token: " + raceSuffix);
    List<Path> newCandidates =
        harness.metadataFiles().stream()
            .filter(path -> !path.toString().equals(base.getTableLocation()))
            .collect(Collectors.toList());
    Assertions.assertEquals(
        filesAfterCreate + 2,
        harness.metadataFiles().size(),
        "the winner and loser candidates are both retained on storage");
    Assertions.assertEquals(
        2,
        newCandidates.stream().map(path -> path.getFileName().toString()).distinct().count(),
        "the two candidate files are distinct: " + newCandidates);

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
    int readsBeforeLosingAttempt = harness.readCalls();

    // Both are prebuilt from the shared base, so the callback captures nothing mid-swap.
    ViewCommitIntent interloperIntent = ViewTestFixtures.changedReplaceIntent(root, base).build();
    ViewCommitIntent loserIntent =
        ViewTestFixtures.baseIntent(root, Boolean.FALSE, base)
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
    Assertions.assertEquals(
        readsBeforeLosingAttempt,
        harness.readCalls(),
        "neither the interloper nor the loser read House Table; both used the shared base");

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

  /** A create that captured absence loses at the swap when a VIEW takes the name before entry. */
  @Test
  void aStaleCapturedAbsenceCreateLosesToAViewInstalledBeforeEntry() {
    assertStaleCreateAbsenceLosesTo(ViewTestFixtures.viewRow("/rival/00001-rival.metadata.json"));
  }

  /** The same, when a TABLE takes the name: still the generic write-time AlreadyExists. */
  @Test
  void aStaleCapturedAbsenceCreateLosesToATableInstalledBeforeEntry() {
    assertStaleCreateAbsenceLosesTo(ViewTestFixtures.tableRow("/rival/00001-rival.metadata.json"));
  }

  private void assertStaleCreateAbsenceLosesTo(HouseTable occupantInstalledBeforeEntry) {
    HouseTable capturedAbsence = captureBase();
    Assertions.assertNull(capturedAbsence, "the caller captured absence before the state changed");

    // The name is taken before the measured commit even begins.
    harness.getHouseTableRepository().seed(occupantInstalledBeforeEntry);
    int readsBeforeCommit = harness.readCalls();
    harness.clearEvents();

    AlreadyExistsException thrown =
        Assertions.assertThrows(
            AlreadyExistsException.class,
            () ->
                harness
                    .getViewCommitEngine()
                    .commit(ViewTestFixtures.createIntent(root, capturedAbsence)));

    Assertions.assertEquals("View already exists: " + DB + "." + VIEW, thrown.getMessage());
    Assertions.assertTrue(
        thrown.getCause() instanceof HouseTableConcurrentUpdateException,
        "the generic write-time conflict carries the direct repository cause: "
            + thrown.getCause());
    Assertions.assertEquals(
        readsBeforeCommit,
        harness.readCalls(),
        "the create trusted its captured absence and read nothing");
    List<String> events = harness.events();
    Assertions.assertEquals(0, htsReadEvents(events), "no House Table read: " + events);
    Assertions.assertEquals(
        0,
        countEvents(events, RecordingViewMetadataCodec.READ),
        "a create reads no prior file: " + events);
    Assertions.assertEquals(
        1, countEvents(events, RecordingViewMetadataCodec.WRITE), "one candidate: " + events);
    int writeAt = indexOfEvent(events, RecordingViewMetadataCodec.WRITE);
    int saveAt = indexOfEvent(events, InMemoryViewHouseTableRepository.SAVE_VIEW);
    Assertions.assertEquals(
        1,
        countEvents(events, InMemoryViewHouseTableRepository.SAVE_VIEW),
        "one save attempt: " + events);
    Assertions.assertTrue(writeAt < saveAt, "the candidate is written before the swap: " + events);
    Assertions.assertTrue(
        events.get(saveAt).contains("expected=" + CatalogConstants.INITIAL_VERSION),
        "the create's single save is an INITIAL claim: " + events);
    Assertions.assertEquals(
        1, harness.metadataFiles().size(), "the losing candidate is written and retained");
    Assertions.assertEquals(
        occupantInstalledBeforeEntry.getTableLocation(),
        harness.getHouseTableRepository().peek(DB, VIEW).get().getTableLocation(),
        "the occupant that won the name is left untouched");
  }

  /** A stale captured base whose row was deleted before entry loses at the swap. */
  @Test
  void aStaleCapturedReplaceLosesWhenItsBaseWasDeletedBeforeEntry() {
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureBase();
    ViewCommitIntent loser = changedReplaceIntentOf(base);

    // The base is deleted before the measured commit even begins.
    Assertions.assertTrue(harness.getViewCommitEngine().dropView(DB, VIEW));
    int filesBeforeCommit = harness.metadataFiles().size();
    int readsBeforeCommit = harness.readCalls();
    harness.clearEvents();

    CommitFailedException thrown =
        Assertions.assertThrows(
            CommitFailedException.class, () -> harness.getViewCommitEngine().commit(loser));

    assertLosingReplaceReadAWroteOneCarryingItsToken(
        base, readsBeforeCommit, filesBeforeCommit, thrown);
    Assertions.assertFalse(
        harness.getHouseTableRepository().peek(DB, VIEW).isPresent(),
        "the delete stands; a losing replace never resurrects the row");
  }

  /** A stale captured base deleted and recreated as a distinct C before entry loses at the swap. */
  @Test
  void aStaleCapturedReplaceLosesWhenItsBaseWasDeletedAndRecreatedBeforeEntry() {
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureBase();
    ViewCommitIntent loser = changedReplaceIntentOf(base);

    // Deleted and a distinct C recreated before the measured commit even begins.
    Assertions.assertTrue(harness.getViewCommitEngine().dropView(DB, VIEW));
    harness
        .getViewCommitEngine()
        .commit(
            ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
                .viewUuid(ViewTestFixtures.SECOND_VIEW_UUID)
                .viewLocation(
                    ViewTestFixtures.allocatedViewLocation(
                        root, DB, VIEW, ViewTestFixtures.SECOND_VIEW_UUID))
                .build());
    HouseTable pointerAtC = harness.getHouseTableRepository().peek(DB, VIEW).get();
    int filesBeforeCommit = harness.metadataFiles().size();
    int readsBeforeCommit = harness.readCalls();
    harness.clearEvents();

    CommitFailedException thrown =
        Assertions.assertThrows(
            CommitFailedException.class, () -> harness.getViewCommitEngine().commit(loser));

    assertLosingReplaceReadAWroteOneCarryingItsToken(
        base, readsBeforeCommit, filesBeforeCommit, thrown);
    Assertions.assertEquals(
        pointerAtC,
        harness.getHouseTableRepository().peek(DB, VIEW).get(),
        "the recreated C row is left exactly as it was");
    Assertions.assertNotEquals(
        base.getTableLocation(),
        harness.getHouseTableRepository().peek(DB, VIEW).get().getTableLocation(),
        "the loser never promotes its stale A over the recreated C");
  }

  private ViewCommitIntent changedReplaceIntentOf(HouseTable base) {
    return ViewTestFixtures.changedReplaceIntent(root, base).build();
  }

  private void assertLosingReplaceReadAWroteOneCarryingItsToken(
      HouseTable base, int readsBeforeCommit, int filesBeforeCommit, CommitFailedException thrown) {
    Assertions.assertEquals(
        "Cannot replace view " + DB + "." + VIEW + ": it was modified concurrently",
        thrown.getMessage());
    Assertions.assertTrue(
        thrown.getCause() instanceof HouseTableConcurrentUpdateException,
        "the generic conflict carries the direct repository cause: " + thrown.getCause());
    Assertions.assertEquals(
        readsBeforeCommit, harness.readCalls(), "a losing replace reads no House Table row");
    List<String> events = harness.events();
    Assertions.assertEquals(0, htsReadEvents(events), "no House Table read: " + events);
    int readAt = indexOfEvent(events, RecordingViewMetadataCodec.READ);
    Assertions.assertEquals(
        1,
        countEvents(events, RecordingViewMetadataCodec.READ),
        "the captured file is read exactly once: " + events);
    Assertions.assertTrue(
        readAt >= 0 && events.get(readAt).contains(base.getTableLocation()),
        "the read is of A's captured path: " + events);
    Assertions.assertEquals(
        1, countEvents(events, RecordingViewMetadataCodec.WRITE), "one candidate: " + events);
    int writeAt = indexOfEvent(events, RecordingViewMetadataCodec.WRITE);
    int saveAt = indexOfEvent(events, InMemoryViewHouseTableRepository.SAVE_VIEW);
    Assertions.assertEquals(
        1,
        countEvents(events, InMemoryViewHouseTableRepository.SAVE_VIEW),
        "one save attempt: " + events);
    Assertions.assertTrue(
        readAt < writeAt && writeAt < saveAt,
        "the captured file is read, then a candidate written, then the swap: " + events);
    Assertions.assertTrue(
        events.get(saveAt).contains("expected=" + base.getTableLocation()),
        "the swap carries A's exact captured token: " + events);
    Assertions.assertEquals(
        filesBeforeCommit + 1,
        harness.metadataFiles().size(),
        "the losing candidate is written and retained");
  }

  private static int countEvents(List<String> events, String prefix) {
    return (int) events.stream().filter(event -> event.startsWith(prefix)).count();
  }

  /** The commit-window event suffix from an index recorded before the measured call. */
  private List<String> suffixSince(int eventBaseline) {
    List<String> all = harness.events();
    return all.subList(eventBaseline, all.size());
  }

  /** How many save events carry the given expected-version token. */
  private static int saveCountCarryingToken(List<String> events, String token) {
    return (int)
        events.stream()
            .filter(event -> event.startsWith(InMemoryViewHouseTableRepository.SAVE_VIEW))
            .filter(event -> event.contains("expected=" + token))
            .count();
  }

  /** How many codec read events are of the given path. */
  private static int readCountOfPath(List<String> events, String path) {
    return (int)
        events.stream()
            .filter(event -> event.startsWith(RecordingViewMetadataCodec.READ))
            .filter(event -> event.contains(path))
            .count();
  }

  private static int indexOfEvent(List<String> events, String prefix) {
    for (int i = 0; i < events.size(); i++) {
      if (events.get(i).startsWith(prefix)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Every House Table read/scan event; {@code findAll} covers both the raw and typed-list scans.
   */
  private static int htsReadEvents(List<String> events) {
    int reads = 0;
    for (String event : events) {
      if (event.startsWith(InMemoryViewHouseTableRepository.FIND_ENTITY)
          || event.startsWith(InMemoryViewHouseTableRepository.FIND_VIEW)
          || event.startsWith(InMemoryViewHouseTableRepository.FIND_BY_ID)
          || event.startsWith(InMemoryViewHouseTableRepository.FIND_ALL)) {
        reads++;
      }
    }
    return reads;
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
