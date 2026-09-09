package com.linkedin.openhouse.internal.catalog.view;

import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.DB;
import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.VIEW;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitOperation;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** How adapter failures become caller outcomes, and that none triggers a second write. */
public class ViewCommitEngineFailureTranslationTest {

  private ViewCommitEngineHarness harness;
  private Path root;

  @BeforeEach
  void setUp(@TempDir Path tempDir) {
    root = tempDir;
    harness = new ViewCommitEngineHarness(tempDir);
  }

  private static HouseTableRepositoryStateUnknownException unknownState() {
    return new HouseTableRepositoryStateUnknownException(
        "Cannot determine if HTS has persisted the proposed change", new RuntimeException("504"));
  }

  private static HouseTableConcurrentUpdateException conflict() {
    return new HouseTableConcurrentUpdateException("", new RuntimeException("409"));
  }

  /** It may have landed, so reporting failure would invite a double-applying retry. */
  @Test
  void ambiguousPublishOnCreateBecomesCommitStateUnknown() {
    int readsBefore = harness.getHouseTableRepository().getTotalReadCalls();
    harness.getHouseTableRepository().clearEvents();
    harness.getHouseTableRepository().failNextSaveViewWith(unknownState());

    Assertions.assertThrows(
        CommitStateUnknownException.class,
        () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null)));

    Assertions.assertEquals(1, harness.getHouseTableRepository().getSaveViewCalls());
    assertOneCreateAttemptWithNoReads(readsBefore);
    assertNothingHappenedAfterThePublishAttempt();
    assertTheCandidateWasWrittenOnceAndLeftAlone();
    Assertions.assertFalse(
        harness.getHouseTableRepository().peek(DB, VIEW).isPresent(),
        "an ambiguous create must not have moved this engine's view of the pointer");
  }

  @Test
  void ambiguousPublishOnReplaceBecomesCommitStateUnknownAndLeavesThePointerAlone() {
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable pointerBefore = harness.getHouseTableRepository().peek(DB, VIEW).get();
    HouseTable base = captureBase();
    int savesBefore = harness.getHouseTableRepository().getSaveViewCalls();
    int filesBefore = harness.metadataFiles().size();
    int readsBefore = harness.getHouseTableRepository().getTotalReadCalls();
    harness.getHouseTableRepository().clearEvents();
    harness.getHouseTableRepository().failNextSaveViewWith(unknownState());

    Assertions.assertThrows(
        CommitStateUnknownException.class,
        () -> harness.getViewCommitEngine().commit(changedReplaceOf(base)));

    Assertions.assertEquals(
        savesBefore + 1,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "an ambiguous publish must never be followed by a second write");
    assertOneReplaceAttemptWithNoReads(readsBefore, base.getTableLocation());
    assertNothingHappenedAfterThePublishAttempt();
    Assertions.assertEquals(
        filesBefore + 1,
        harness.metadataFiles().size(),
        "the candidate written before an ambiguous publish must survive: the write may have landed,"
            + " so deleting it could strand a pointer that now references it");
    Assertions.assertEquals(
        1,
        harness.codecWrites(),
        "an ambiguous publish must not be followed by a rebuild: " + harness.events());
    Assertions.assertTrue(
        harness.metadataFiles().stream()
            .anyMatch(path -> path.toString().equals(pointerBefore.getTableLocation())),
        "the previously published file must also still be there");
    Assertions.assertEquals(
        pointerBefore,
        harness.getHouseTableRepository().peek(DB, VIEW).get(),
        "the pointer row must be untouched after an ambiguous publish");
  }

  /** Cleanup is invisible to the event log, so it is asserted against the file system. */
  private void assertTheCandidateWasWrittenOnceAndLeftAlone() {
    Assertions.assertEquals(
        1,
        harness.codecWrites(),
        "exactly one candidate is built for one attempt: " + harness.events());
    Assertions.assertEquals(
        1,
        harness.metadataFiles().size(),
        "an ambiguous outcome is not a cleanable failure, so the candidate must remain on storage");
  }

  @Test
  void conflictOnCreateBecomesAlreadyExists() {
    HouseTableConcurrentUpdateException injectedConflict = conflict();
    int readsBefore = harness.getHouseTableRepository().getTotalReadCalls();
    harness.getHouseTableRepository().clearEvents();
    harness.getHouseTableRepository().failNextSaveViewWith(injectedConflict);

    AlreadyExistsException thrown =
        Assertions.assertThrows(
            AlreadyExistsException.class,
            () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null)));

    Assertions.assertEquals("View already exists: " + DB + "." + VIEW, thrown.getMessage());
    Assertions.assertSame(injectedConflict, thrown.getCause());
    Assertions.assertEquals(1, harness.getHouseTableRepository().getSaveViewCalls());
    assertOneCreateAttemptWithNoReads(readsBefore);
    assertNothingHappenedAfterThePublishAttempt();
  }

  @Test
  void conflictOnReplaceBecomesCommitFailed() {
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable pointerBefore = harness.getHouseTableRepository().peek(DB, VIEW).get();
    HouseTable base = captureBase();
    int savesBefore = harness.getHouseTableRepository().getSaveViewCalls();
    int readsBefore = harness.getHouseTableRepository().getTotalReadCalls();
    harness.getHouseTableRepository().clearEvents();
    HouseTableConcurrentUpdateException injectedConflict = conflict();
    harness.getHouseTableRepository().failNextSaveViewWith(injectedConflict);

    CommitFailedException thrown =
        Assertions.assertThrows(
            CommitFailedException.class,
            () -> harness.getViewCommitEngine().commit(changedReplaceOf(base)));

    Assertions.assertEquals(
        "Cannot replace view " + DB + "." + VIEW + ": it was modified concurrently",
        thrown.getMessage());
    Assertions.assertSame(injectedConflict, thrown.getCause());
    Assertions.assertEquals(
        savesBefore + 1,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "a conflicting publish is one attempt, never retried");
    assertOneReplaceAttemptWithNoReads(readsBefore, base.getTableLocation());
    assertNothingHappenedAfterThePublishAttempt();
    Assertions.assertEquals(pointerBefore, harness.getHouseTableRepository().peek(DB, VIEW).get());
  }

  @Test
  void callerFailureOnPublishIsNotReclassifiedAsUnknownState() {
    int readsBefore = harness.getHouseTableRepository().getTotalReadCalls();
    harness.getHouseTableRepository().clearEvents();
    harness
        .getHouseTableRepository()
        .failNextSaveViewWith(
            new HouseTableCallerException(
                "[Client side failure]Error status code for HTS:400", new RuntimeException("400")));

    Assertions.assertThrows(
        HouseTableCallerException.class,
        () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null)));

    Assertions.assertEquals(1, harness.getHouseTableRepository().getSaveViewCalls());
    assertOneCreateAttemptWithNoReads(readsBefore);
    assertNothingHappenedAfterThePublishAttempt();
  }

  @Test
  void ambiguousDropBecomesCommitStateUnknown() {
    harness.getHouseTableRepository().seed(ViewTestFixtures.viewRow("/loc/00001-a.metadata.json"));
    harness.getHouseTableRepository().failNextDeleteViewWith(unknownState());

    Assertions.assertThrows(
        CommitStateUnknownException.class, () -> harness.getViewCommitEngine().dropView(DB, VIEW));

    Assertions.assertEquals(
        1,
        harness.getHouseTableRepository().getDeleteViewByIdCalls(),
        "an ambiguous delete must not be retried");
  }

  /**
   * A commit works from the captured snapshot, so it never reaches the neutral reader: an armed
   * read failure is left un-consumed. Upstream, a failed lookup propagates to the caller instead.
   */
  @Test
  void aCommitNeverConsumesTheNeutralReaderBecauseItWorksFromTheCapturedSnapshot() {
    harness
        .getHouseTableRepository()
        .seed(ViewTestFixtures.viewRow("/existing/00001-a.metadata.json"));
    HouseTable occupant = captureBase();
    // Armed AFTER the capture: if the commit made a neutral read, it would trip this.
    harness.getHouseTableRepository().failNextFindEntityWith(unknownState());

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, occupant)));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertTrue(
        harness.metadataFiles().isEmpty(), "a classified occupant writes no candidate");

    // The armed failure is still pending, proving the commit made no neutral read of its own.
    Assertions.assertThrows(
        HouseTableRepositoryStateUnknownException.class,
        () -> harness.getHouseTableRepository().findEntityById(ViewTestFixtures.key(DB, VIEW)));
  }

  /** Silence after the single publish: a re-read would guess at the outcome. */
  private void assertNothingHappenedAfterThePublishAttempt() {
    List<String> events = harness.getHouseTableRepository().getEvents();
    int lastSave = -1;
    for (int i = 0; i < events.size(); i++) {
      if (events.get(i).startsWith(InMemoryViewHouseTableRepository.SAVE_VIEW)) {
        lastSave = i;
      }
    }
    Assertions.assertTrue(lastSave >= 0, "expected a publish attempt, saw " + events);
    Assertions.assertEquals(
        events.size() - 1,
        lastSave,
        "no House Table interaction may follow the single publish attempt: " + events);
  }

  private ViewCommitIntent changedReplaceOf(HouseTable base) {
    return ViewTestFixtures.baseIntent(root, ViewCommitOperation.REPLACE, base)
        .schema(ViewTestFixtures.schemaV2())
        .representations(
            Collections.singletonList(
                ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
        .build();
  }

  /** The upstream neutral lookup a caller performs once, before invoking the engine. */
  private HouseTable captureBase() {
    return harness
        .getHouseTableRepository()
        .findEntityById(ViewTestFixtures.key(DB, VIEW))
        .orElse(null);
  }

  /** A failed CREATE publish still wrote exactly one candidate after zero reads, then one PUT. */
  private void assertOneCreateAttemptWithNoReads(int readsBefore) {
    Assertions.assertEquals(
        readsBefore,
        harness.getHouseTableRepository().getTotalReadCalls(),
        "a create attempt makes no House Table read");
    List<String> events = harness.getHouseTableRepository().getEvents();
    Assertions.assertEquals(0, htsReadEvents(events), "no House Table read event: " + events);
    Assertions.assertEquals(
        0,
        countEvents(events, RecordingViewMetadataCodec.READ),
        "a create has no prior file to read: " + events);
    Assertions.assertEquals(
        1,
        countEvents(events, RecordingViewMetadataCodec.WRITE),
        "exactly one candidate is written: " + events);
    Assertions.assertEquals(
        1,
        countEvents(events, InMemoryViewHouseTableRepository.SAVE_VIEW),
        "exactly one PUT is attempted: " + events);
    Assertions.assertTrue(
        indexOfEvent(events, RecordingViewMetadataCodec.WRITE)
            < indexOfEvent(events, InMemoryViewHouseTableRepository.SAVE_VIEW),
        "the candidate is written before the single PUT: " + events);
  }

  /** A failed REPLACE publish read the captured file once, wrote one candidate, then one PUT. */
  private void assertOneReplaceAttemptWithNoReads(int readsBefore, String capturedPath) {
    Assertions.assertEquals(
        readsBefore,
        harness.getHouseTableRepository().getTotalReadCalls(),
        "a replace attempt makes no House Table read");
    List<String> events = harness.getHouseTableRepository().getEvents();
    Assertions.assertEquals(0, htsReadEvents(events), "no House Table read event: " + events);
    Assertions.assertEquals(
        1,
        countEvents(events, RecordingViewMetadataCodec.READ),
        "the captured file is read exactly once: " + events);
    int readAt = indexOfEvent(events, RecordingViewMetadataCodec.READ);
    Assertions.assertTrue(
        readAt >= 0 && events.get(readAt).contains(capturedPath),
        "the one read is of the captured path: " + events);
    Assertions.assertEquals(
        1,
        countEvents(events, RecordingViewMetadataCodec.WRITE),
        "exactly one candidate is written: " + events);
    Assertions.assertEquals(
        1,
        countEvents(events, InMemoryViewHouseTableRepository.SAVE_VIEW),
        "exactly one PUT is attempted: " + events);
    Assertions.assertTrue(
        readAt < indexOfEvent(events, RecordingViewMetadataCodec.WRITE),
        "the captured file is read before the candidate is written: " + events);
    Assertions.assertTrue(
        indexOfEvent(events, RecordingViewMetadataCodec.WRITE)
            < indexOfEvent(events, InMemoryViewHouseTableRepository.SAVE_VIEW),
        "the candidate is written before the single PUT: " + events);
  }

  private static int countEvents(List<String> events, String prefix) {
    return (int) events.stream().filter(event -> event.startsWith(prefix)).count();
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
}
