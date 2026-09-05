package com.linkedin.openhouse.internal.catalog.view;

import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.DB;
import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.VIEW;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
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

/**
 * How adapter failures become caller-meaningful outcomes. Separate from the race tests: these
 * inject the failure directly and prove no failure triggers a second write, a re-read, or a
 * rebuild.
 */
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
    harness.getHouseTableRepository().failNextSaveViewWith(unknownState());

    Assertions.assertThrows(
        CommitStateUnknownException.class,
        () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root)));

    Assertions.assertEquals(1, harness.getHouseTableRepository().getSaveViewCalls());
    assertNothingHappenedAfterThePublishAttempt();
    Assertions.assertFalse(
        harness.getHouseTableRepository().peek(DB, VIEW).isPresent(),
        "an ambiguous create must not have moved this engine's view of the pointer");
  }

  @Test
  void ambiguousPublishOnReplaceBecomesCommitStateUnknownAndLeavesThePointerAlone() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));
    HouseTable pointerBefore = harness.getHouseTableRepository().peek(DB, VIEW).get();
    int savesBefore = harness.getHouseTableRepository().getSaveViewCalls();
    harness.getHouseTableRepository().clearEvents();
    harness.getHouseTableRepository().failNextSaveViewWith(unknownState());

    Assertions.assertThrows(
        CommitStateUnknownException.class,
        () -> harness.getViewCommitEngine().commit(changedReplaceOf(created)));

    Assertions.assertEquals(
        savesBefore + 1,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "an ambiguous publish must never be followed by a second write");
    assertNothingHappenedAfterThePublishAttempt();
    Assertions.assertEquals(
        pointerBefore,
        harness.getHouseTableRepository().peek(DB, VIEW).get(),
        "the pointer row must be untouched after an ambiguous publish");
  }

  /** A create that loses the swap is a name collision from the caller's point of view. */
  @Test
  void conflictOnCreateBecomesAlreadyExists() {
    harness.getHouseTableRepository().failNextSaveViewWith(conflict());

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root)));

    Assertions.assertEquals(1, harness.getHouseTableRepository().getSaveViewCalls());
    assertNothingHappenedAfterThePublishAttempt();
  }

  /** The same 409 in a replace means someone else committed, which is a commit failure. */
  @Test
  void conflictOnReplaceBecomesCommitFailed() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));
    HouseTable pointerBefore = harness.getHouseTableRepository().peek(DB, VIEW).get();
    harness.getHouseTableRepository().clearEvents();
    harness.getHouseTableRepository().failNextSaveViewWith(conflict());

    Assertions.assertThrows(
        CommitFailedException.class,
        () -> harness.getViewCommitEngine().commit(changedReplaceOf(created)));

    assertNothingHappenedAfterThePublishAttempt();
    Assertions.assertEquals(pointerBefore, harness.getHouseTableRepository().peek(DB, VIEW).get());
  }

  /**
   * A classified caller failure keeps its classification so the later layer can preserve status.
   */
  @Test
  void callerFailureOnPublishIsNotReclassifiedAsUnknownState() {
    harness
        .getHouseTableRepository()
        .failNextSaveViewWith(
            new HouseTableCallerException(
                "[Client side failure]Error status code for HTS:400", new RuntimeException("400")));

    Assertions.assertThrows(
        HouseTableCallerException.class,
        () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root)));

    Assertions.assertEquals(1, harness.getHouseTableRepository().getSaveViewCalls());
  }

  /** An ambiguous delete is unknown state too; a view may or may not still be there. */
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

  /** A transport failure is not evidence the name is free. */
  @Test
  void transportFailureOnTheOccupancyReadNeverReadsAsAFreeName() {
    harness.getHouseTableRepository().failNextFindEntityWith(unknownState());

    // The classified transport failure must propagate; anything else would either invent an answer
    // or hide the fact that occupancy is unknown.
    Assertions.assertThrows(
        HouseTableRepositoryStateUnknownException.class,
        () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root)));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertTrue(
        harness.metadataFiles().isEmpty(), "no candidate file may be written after a failed probe");
    Assertions.assertFalse(harness.getHouseTableRepository().peek(DB, VIEW).isPresent());
  }

  /** Everything after the single publish must be silence: a re-read would guess at the outcome. */
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

  private ViewCommitIntent changedReplaceOf(ViewCommitResult created) {
    return ViewTestFixtures.baseIntent(root)
        .schema(ViewTestFixtures.schemaV2())
        .representations(
            Collections.singletonList(
                ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
        .baseViewVersion(created.getPointer().getMetadataLocation())
        .build();
  }
}
