package com.linkedin.openhouse.internal.catalog.view;

import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.DB;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
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
 * Typed list, load, drop, and create-collision over a key space holding both kinds of entity. Views
 * and tables share one key space, so a filtered page or a views-only fixture cannot tell a typed
 * route from an untyped one.
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

  /** The typed drop must decline rather than delete the wrong entity. */
  @Test
  void droppingATableThroughTheViewPathReportsFalseAndDeletesNothing() {
    Assertions.assertFalse(harness.getViewCommitEngine().dropView(DB, "table_a"));

    Assertions.assertEquals(
        row("table_a", "TABLE"), harness.getHouseTableRepository().peek(DB, "table_a").get());
  }

  /** A legacy row means TABLE, so it is equally out of reach. */
  @Test
  void droppingALegacyRowThroughTheViewPathReportsFalseAndDeletesNothing() {
    Assertions.assertFalse(harness.getViewCommitEngine().dropView(DB, "legacy_a"));

    Assertions.assertEquals(
        row("legacy_a", null), harness.getHouseTableRepository().peek(DB, "legacy_a").get());
  }

  /** A miss, not a mis-typed success. */
  @Test
  void loadingATableThroughTheViewPathIsNoSuchView() {
    Assertions.assertThrows(
        NoSuchViewException.class, () -> harness.getViewCommitEngine().loadView(DB, "table_a"));
    Assertions.assertThrows(
        NoSuchViewException.class, () -> harness.getViewCommitEngine().loadView(DB, "legacy_a"));
  }

  /**
   * A create colliding with a row written before the discriminator existed must be a clean 409
   * naming TABLE. House Table resolves the legacy null on hydration, so the engine classifies a
   * real value rather than guessing at a missing one.
   */
  @Test
  void createCollidingWithALegacyRowIsACleanTableCollisionAndNotAnIntegrityFailure() {
    ViewNameOccupiedException thrown =
        Assertions.assertThrows(
            ViewNameOccupiedException.class,
            () ->
                harness
                    .getViewCommitEngine()
                    .commit(
                        ViewTestFixtures.baseIntent(root)
                            .viewId("legacy_a")
                            .viewLocation(
                                ViewTestFixtures.allocatedViewLocation(
                                    root, DB, "legacy_a", ViewTestFixtures.VIEW_UUID))
                            .build()));

    Assertions.assertEquals(ViewTestFixtures.ENTITY_TYPE_TABLE, thrown.getOccupantEntityType());
    Assertions.assertEquals("legacy_a", thrown.getViewId());
    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  /**
   * The double is only useful if it is faithful, so assert its finder split directly: a view is
   * invisible to the table read, a table and a legacy row are invisible to the view read, and the
   * neutral read reports a legacy row as TABLE exactly as the server's converter does.
   */
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
  }

  private static HouseTablePrimaryKey key(String tableId) {
    return HouseTablePrimaryKey.builder().databaseId(DB).tableId(tableId).build();
  }
}
