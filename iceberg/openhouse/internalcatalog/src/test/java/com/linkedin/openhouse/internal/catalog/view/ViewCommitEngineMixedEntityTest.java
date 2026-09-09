package com.linkedin.openhouse.internal.catalog.view;

import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.DB;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitOperation;
import com.linkedin.openhouse.internal.catalog.view.model.ViewPointer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

/**
 * Typed list, load, drop, and create-collision over a key space holding both kinds of entity: a
 * views-only fixture could not tell a typed route from an untyped one.
 */
public class ViewCommitEngineMixedEntityTest {

  private ViewCommitEngineHarness harness;
  private Path root;

  @BeforeEach
  void setUp(@TempDir Path tempDir) {
    root = tempDir;
    harness = new ViewCommitEngineHarness(tempDir);
    harness.getHouseTableRepository().seed(row("view_a", "VIEW"));
    harness.getHouseTableRepository().seed(row("view_b", "VIEW"));
    harness.getHouseTableRepository().seed(row("table_a", "TABLE"));
    harness.getHouseTableRepository().seed(row("legacy_a", null));
  }

  private static HouseTable row(String id, String entityType) {
    return HouseTable.builder()
        .databaseId(DB)
        .tableId(id)
        .tableLocation("/loc/" + id + "/00001-a.metadata.json")
        .tableVersion("INITIAL_VERSION")
        .storageType(ViewTestFixtures.LOCAL_STORAGE_TYPE)
        .entityType(entityType)
        .build();
  }

  @Test
  void listReturnsOnlyViewsFromAMixedKeySpace() {
    Page<ViewPointer> page = harness.getViewCommitEngine().listViews(DB, PageRequest.of(0, 10));

    List<String> ids =
        page.getContent().stream()
            .map(ViewPointer::getViewId)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(Arrays.asList("view_a", "view_b"), ids);
    Assertions.assertEquals(2L, page.getTotalElements());
    Assertions.assertEquals(1, page.getTotalPages());
  }

  @Test
  void listPaginatesOverViewsOnlyAndNeverParsesMetadata() {
    Page<ViewPointer> firstPage = harness.getViewCommitEngine().listViews(DB, PageRequest.of(0, 1));
    Assertions.assertEquals(1, firstPage.getContent().size());
    Assertions.assertEquals(2L, firstPage.getTotalElements());
    Assertions.assertEquals(2, firstPage.getTotalPages());

    Page<ViewPointer> secondPage =
        harness.getViewCommitEngine().listViews(DB, PageRequest.of(1, 1));
    Assertions.assertEquals(1, secondPage.getContent().size());
    Assertions.assertNotEquals(
        firstPage.getContent().get(0).getViewId(), secondPage.getContent().get(0).getViewId());

    Assertions.assertTrue(
        harness.events().stream().noneMatch(event -> event.startsWith("codec.")),
        "listing must never open a metadata file: " + harness.events());
  }

  @Test
  void droppingAViewLeavesTableAndLegacyRowsUntouched() {
    Assertions.assertTrue(harness.getViewCommitEngine().dropView(DB, "view_a"));

    Assertions.assertFalse(harness.getHouseTableRepository().peek(DB, "view_a").isPresent());
    Assertions.assertTrue(harness.getHouseTableRepository().peek(DB, "view_b").isPresent());
    Assertions.assertEquals(
        row("table_a", "TABLE"), harness.getHouseTableRepository().peek(DB, "table_a").get());
    Assertions.assertEquals(
        row("legacy_a", null), harness.getHouseTableRepository().peek(DB, "legacy_a").get());
  }

  @Test
  void droppingATableThroughTheViewPathReportsFalseAndDeletesNothing() {
    Assertions.assertFalse(harness.getViewCommitEngine().dropView(DB, "table_a"));

    Assertions.assertEquals(
        row("table_a", "TABLE"), harness.getHouseTableRepository().peek(DB, "table_a").get());
  }

  @Test
  void droppingALegacyRowThroughTheViewPathReportsFalseAndDeletesNothing() {
    Assertions.assertFalse(harness.getViewCommitEngine().dropView(DB, "legacy_a"));

    Assertions.assertEquals(
        row("legacy_a", null), harness.getHouseTableRepository().peek(DB, "legacy_a").get());
  }

  @Test
  void loadingATableThroughTheViewPathIsNoSuchView() {
    Assertions.assertThrows(
        NoSuchViewException.class, () -> harness.getViewCommitEngine().loadView(DB, "table_a"));
    Assertions.assertThrows(
        NoSuchViewException.class, () -> harness.getViewCommitEngine().loadView(DB, "legacy_a"));
  }

  /** A legacy row hydrates to TABLE, so the collision is a clean 409 rather than a guess. */
  @Test
  void createCollidingWithALegacyRowIsACleanTableCollisionAndNotAnIntegrityFailure() {
    // The caller passes the hydrated neutral read (TABLE), never the raw legacy null.
    HouseTable occupant =
        harness.getHouseTableRepository().findEntityById(key("legacy_a")).orElse(null);
    Assertions.assertEquals(
        ViewTestFixtures.ENTITY_TYPE_TABLE,
        occupant.getEntityType(),
        "the neutral read hydrates the legacy null to TABLE before the engine ever sees it");
    int readsBeforeCommit = harness.readCalls();

    ViewNameOccupiedException thrown =
        Assertions.assertThrows(
            ViewNameOccupiedException.class,
            () ->
                harness
                    .getViewCommitEngine()
                    .commit(
                        ViewTestFixtures.baseIntent(root, ViewCommitOperation.CREATE, occupant)
                            .viewId("legacy_a")
                            .viewLocation(
                                ViewTestFixtures.allocatedViewLocation(
                                    root, DB, "legacy_a", ViewTestFixtures.VIEW_UUID))
                            .build()));

    Assertions.assertEquals(ViewTestFixtures.ENTITY_TYPE_TABLE, thrown.getOccupantEntityType());
    Assertions.assertEquals("legacy_a", thrown.getViewId());
    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        readsBeforeCommit,
        harness.readCalls(),
        "the occupant is classified from the supplied snapshot, without a further read");
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  /** The double is only useful if faithful, so its finder split is asserted directly. */
  @Test
  void theDoubleReproducesTheServerFinderSplitAndItsLegacyHydration() {
    Assertions.assertFalse(
        harness.getHouseTableRepository().findById(key("view_a")).isPresent(),
        "a view must be absent from the table point read");
    Assertions.assertEquals(
        "TABLE", harness.getHouseTableRepository().findById(key("table_a")).get().getEntityType());
    Assertions.assertEquals(
        "TABLE",
        harness.getHouseTableRepository().findById(key("legacy_a")).get().getEntityType(),
        "a legacy row reads back as a table");

    Assertions.assertTrue(
        harness.getHouseTableRepository().findViewById(key("view_a")).isPresent());
    Assertions.assertFalse(
        harness.getHouseTableRepository().findViewById(key("table_a")).isPresent());
    Assertions.assertFalse(
        harness.getHouseTableRepository().findViewById(key("legacy_a")).isPresent());

    Assertions.assertEquals(
        "VIEW",
        harness.getHouseTableRepository().findEntityById(key("view_a")).get().getEntityType());
    Assertions.assertEquals(
        "TABLE",
        harness.getHouseTableRepository().findEntityById(key("table_a")).get().getEntityType());
    Assertions.assertEquals(
        "TABLE",
        harness.getHouseTableRepository().findEntityById(key("legacy_a")).get().getEntityType(),
        "the neutral read hydrates a legacy null to TABLE, so no engine null check is needed");
    Assertions.assertFalse(
        harness.getHouseTableRepository().findEntityById(key("absent")).isPresent());

    Assertions.assertNull(
        harness.getHouseTableRepository().peek(DB, "legacy_a").get().getEntityType(),
        "the raw stored value is still null: hydration happens on read, not on write");

    // Positive controls: every instrumented reader increments its counter, the aggregate, and emits
    // its event, so the zero-read assertions elsewhere (counter and event based) are not vacuous.
    InMemoryViewHouseTableRepository repo = harness.getHouseTableRepository();

    int entityBefore = repo.getFindEntityByIdCalls();
    int aggBefore = repo.getTotalReadCalls();
    harness.clearEvents();
    repo.findEntityById(key("view_a"));
    Assertions.assertEquals(entityBefore + 1, repo.getFindEntityByIdCalls(), "neutral counter");
    Assertions.assertEquals(aggBefore + 1, repo.getTotalReadCalls(), "neutral aggregate");
    assertEmitted(InMemoryViewHouseTableRepository.FIND_ENTITY);

    int viewBefore = repo.getFindViewByIdCalls();
    harness.clearEvents();
    repo.findViewById(key("view_a"));
    Assertions.assertEquals(viewBefore + 1, repo.getFindViewByIdCalls(), "typed view counter");
    assertEmitted(InMemoryViewHouseTableRepository.FIND_VIEW);

    int byIdBefore = repo.getFindByIdCalls();
    harness.clearEvents();
    repo.findById(key("table_a"));
    Assertions.assertEquals(
        byIdBefore + 1, repo.getFindByIdCalls(), "raw table point-read counter");
    assertEmitted(InMemoryViewHouseTableRepository.FIND_BY_ID);

    int allBefore = repo.getFindAllCalls();
    harness.clearEvents();
    repo.findAll();
    Assertions.assertEquals(allBefore + 1, repo.getFindAllCalls(), "no-arg scan counter");
    assertEmitted(InMemoryViewHouseTableRepository.FIND_ALL);

    // The separately instrumented paged overload must also be a positive control.
    int allPagedBefore = repo.getFindAllCalls();
    harness.clearEvents();
    repo.findAll(PageRequest.of(0, 10));
    Assertions.assertEquals(allPagedBefore + 1, repo.getFindAllCalls(), "paged scan counter");
    assertEmitted(InMemoryViewHouseTableRepository.FIND_ALL);

    // The typed list scan is now in the aggregate too, so an accidental list during commit is
    // caught.
    int listBefore = repo.getFindAllViewsByDatabaseIdCalls();
    int aggBeforeList = repo.getTotalReadCalls();
    harness.clearEvents();
    repo.findAllViewsByDatabaseId(DB, PageRequest.of(0, 10));
    Assertions.assertEquals(
        listBefore + 1, repo.getFindAllViewsByDatabaseIdCalls(), "typed list-scan counter");
    Assertions.assertEquals(aggBeforeList + 1, repo.getTotalReadCalls(), "list-scan aggregate");
    assertEmitted(InMemoryViewHouseTableRepository.LIST_VIEWS);
  }

  private void assertEmitted(String eventPrefix) {
    Assertions.assertTrue(
        harness.events().stream().anyMatch(event -> event.startsWith(eventPrefix)),
        "an instrumented read must emit its event " + eventPrefix + ": " + harness.events());
  }

  private static HouseTablePrimaryKey key(String tableId) {
    return HouseTablePrimaryKey.builder().databaseId(DB).tableId(tableId).build();
  }
}
