package com.linkedin.openhouse.internal.catalog.view;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;
import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.DB;
import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.VIEW;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.view.model.LoadedView;
import com.linkedin.openhouse.internal.catalog.view.model.SqlViewRepresentationIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Create and replace: collision classification, caller-supplied identity and location,
 * Iceberg-owned version identity, no-op detection, dialect safety, stamping. Every metadata
 * assertion is a golden round trip.
 */
public class ViewCommitEngineCommitTest {

  private ViewCommitEngineHarness harness;
  private Path root;

  @BeforeEach
  void setUp(@TempDir Path tempDir) {
    root = tempDir;
    harness = new ViewCommitEngineHarness(tempDir);
  }

  /* ---- Create collision classification: every occupant shape, no side effects. ---- */

  @Test
  void createCollidingWithAnExistingViewReportsViewAlreadyExists() {
    harness
        .getHouseTableRepository()
        .seed(ViewTestFixtures.viewRow("/existing/00001-a.metadata.json"));

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root)));

    assertCreateCollisionLeftNoTrace();
  }

  @Test
  void createCollidingWithATableReportsNameOccupiedCarryingTheOccupantType() {
    harness
        .getHouseTableRepository()
        .seed(ViewTestFixtures.tableRow("/existing/00001-a.metadata.json"));

    ViewNameOccupiedException thrown =
        Assertions.assertThrows(
            ViewNameOccupiedException.class,
            () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root)));

    Assertions.assertEquals(ViewTestFixtures.ENTITY_TYPE_TABLE, thrown.getOccupantEntityType());
    Assertions.assertEquals(DB, thrown.getDatabaseId());
    Assertions.assertEquals(VIEW, thrown.getViewId());
    assertCreateCollisionLeftNoTrace();
  }

  /**
   * A row predating the discriminator means TABLE, not a free name. House Table resolves the legacy
   * null before the engine ever sees it, so this stays a clean collision rather than an integrity
   * failure.
   */
  @Test
  void createCollidingWithALegacyRowReportsNameOccupiedAsTable() {
    harness
        .getHouseTableRepository()
        .seed(ViewTestFixtures.legacyRow("/existing/00001-a.metadata.json"));

    ViewNameOccupiedException thrown =
        Assertions.assertThrows(
            ViewNameOccupiedException.class,
            () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root)));

    Assertions.assertEquals(ViewTestFixtures.ENTITY_TYPE_TABLE, thrown.getOccupantEntityType());
    assertCreateCollisionLeftNoTrace();
  }

  /** An unknown type must fail closed, preserving the raw value. */
  @Test
  void createCollidingWithAnUnknownEntityTypeFailsClosed() {
    harness
        .getHouseTableRepository()
        .seed(
            ViewTestFixtures.row(
                ViewTestFixtures.ENTITY_TYPE_UNKNOWN, "/existing/00001-a.metadata.json"));

    ViewNameOccupiedException thrown =
        Assertions.assertThrows(
            ViewNameOccupiedException.class,
            () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root)));

    Assertions.assertEquals(ViewTestFixtures.ENTITY_TYPE_UNKNOWN, thrown.getOccupantEntityType());
    assertCreateCollisionLeftNoTrace();
  }

  /** A non-canonical spelling is not a recognized type, so something occupies the name. */
  @Test
  void createCollidingWithANonCanonicalDiscriminatorFailsClosed() {
    harness
        .getHouseTableRepository()
        .seed(ViewTestFixtures.row("view", "/existing/00001-a.metadata.json"));

    ViewNameOccupiedException thrown =
        Assertions.assertThrows(
            ViewNameOccupiedException.class,
            () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root)));

    Assertions.assertEquals("view", thrown.getOccupantEntityType());
    assertCreateCollisionLeftNoTrace();
  }

  @Test
  void createOnAFreeNameProceedsAndPublishesExactlyOnce() {
    ViewCommitResult result =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));

    Assertions.assertTrue(result.isCreated());
    Assertions.assertTrue(result.isMetadataChanged());
    Assertions.assertEquals(1, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(1, harness.getHouseTableRepository().getFindEntityByIdCalls());
    Assertions.assertEquals(1, harness.metadataFiles().size());
    Optional<HouseTable> pointer = harness.getHouseTableRepository().peek(DB, VIEW);
    Assertions.assertTrue(pointer.isPresent());
    Assertions.assertEquals(
        result.getPointer().getMetadataLocation(), pointer.get().getTableLocation());
  }

  /** An occupied name costs one neutral read: no FileIO, no file, no publish. */
  private void assertCreateCollisionLeftNoTrace() {
    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    verify(harness.getCodec(), never()).write(any(ViewMetadata.class), any(OutputFile.class));
    verify(harness.getFileIOManager(), never()).getFileIO(any(StorageType.Type.class));
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  /* ---- Identity, location, and storage are the caller's, never the engine's. ---- */

  /** The engine records the identity it was handed; it does not mint one. */
  @Test
  void createUsesTheCallerSuppliedIdentityLocationAndStorage() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));
    ViewMetadata metadata = harness.readMetadata(created.getPointer().getMetadataLocation());

    Assertions.assertEquals(ViewTestFixtures.VIEW_UUID, created.getViewUuid());
    Assertions.assertEquals(ViewTestFixtures.VIEW_UUID, metadata.uuid());
    Assertions.assertEquals(
        ViewTestFixtures.VIEW_UUID,
        metadata.properties().get(CatalogConstants.OPENHOUSE_UUID_KEY),
        "the stamped OpenHouse UUID is the supplied one, not a fresh one");
    Assertions.assertEquals(
        ViewTestFixtures.viewLocation(root),
        metadata.location(),
        "the metadata root is exactly the location the caller allocated");
    Assertions.assertTrue(
        created.getPointer().getMetadataLocation().startsWith(ViewTestFixtures.viewLocation(root)),
        "the metadata file lives under the supplied root: "
            + created.getPointer().getMetadataLocation());
    Assertions.assertEquals(
        ViewTestFixtures.LOCAL_STORAGE_TYPE, created.getPointer().getStorageType());

    // Resolved from the supplied storage type; there is no selector seam left to consult.
    verify(harness.getFileIOManager(), times(1)).getFileIO(StorageType.LOCAL);
    verify(harness.getFileIOManager(), never()).getStorage(any(FileIO.class));
  }

  /**
   * LOCAL is the fixture default, so a create that only ever ran against it could not tell a
   * supplied storage type from a hardcoded one. This supplies a different type end to end.
   */
  @Test
  void createResolvesFileIoFromTheSuppliedStorageTypeRatherThanTheDefault() {
    ViewCommitResult created =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .storageType(ViewCommitEngineHarness.ALTERNATE_STORAGE_TYPE)
                    .build());

    verify(harness.getFileIOManager(), times(1)).getFileIO(StorageType.HDFS);
    verify(harness.getFileIOManager(), never()).getFileIO(StorageType.LOCAL);
    // Recovering the type from the FileIO would be lossy: two storages may share one FileIO.
    verify(harness.getFileIOManager(), never()).getStorage(any(FileIO.class));

    Assertions.assertEquals(
        ViewCommitEngineHarness.ALTERNATE_STORAGE_TYPE, created.getPointer().getStorageType());
    Assertions.assertEquals(
        ViewCommitEngineHarness.ALTERNATE_STORAGE_TYPE,
        harness.getHouseTableRepository().peek(DB, VIEW).get().getStorageType(),
        "the persisted row must carry the storage the caller selected");
  }

  /**
   * The whole point of ignoring the incoming physical fields on a replace: the published row's own
   * storage decides both which FileIO is used and what the new pointer reports, so an unusable
   * incoming value cannot redirect the write and the fixture default cannot mask the failure.
   */
  @Test
  void replaceResolvesFileIoFromThePublishedRowStorageNotTheIncomingValue() {
    ViewCommitResult created =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .storageType(ViewCommitEngineHarness.ALTERNATE_STORAGE_TYPE)
                    .build());
    Mockito.clearInvocations(harness.getFileIOManager());

    ViewCommitResult replaced =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .schema(ViewTestFixtures.schemaV2())
                    .representations(
                        Collections.singletonList(
                            ViewTestFixtures.sql(
                                ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
                    .storageType("no-such-storage")
                    .baseViewVersion(created.getPointer().getMetadataLocation())
                    .build());

    verify(harness.getFileIOManager(), times(1)).getFileIO(StorageType.HDFS);
    verify(harness.getFileIOManager(), never()).getFileIO(StorageType.LOCAL);
    verify(harness.getFileIOManager(), never()).getStorage(any(FileIO.class));

    Assertions.assertEquals(
        ViewCommitEngineHarness.ALTERNATE_STORAGE_TYPE, replaced.getPointer().getStorageType());
    Assertions.assertEquals(
        ViewCommitEngineHarness.ALTERNATE_STORAGE_TYPE,
        harness.getHouseTableRepository().peek(DB, VIEW).get().getStorageType(),
        "a replace must not rewrite the published storage fact");
  }

  /** One immutable file per commit, each with its own collision-avoidance UUID. */
  @Test
  void eachMetadataFileIsVersionPrefixedAndCarriesItsOwnRandomUuid() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));
    ViewCommitResult replaced = harness.getViewCommitEngine().commit(changedReplaceOf(created));

    String firstFile =
        Paths.get(created.getPointer().getMetadataLocation()).getFileName().toString();
    String secondFile =
        Paths.get(replaced.getPointer().getMetadataLocation()).getFileName().toString();

    Assertions.assertTrue(firstFile.startsWith("00001-"), firstFile);
    Assertions.assertTrue(secondFile.startsWith("00002-"), secondFile);
    Assertions.assertTrue(firstFile.endsWith(".metadata.json"), firstFile);
    Assertions.assertTrue(secondFile.endsWith(".metadata.json"), secondFile);

    // Parsed, not merely compared as text: two distinct arbitrary strings would satisfy a plain
    // inequality while telling us nothing about collision avoidance.
    UUID firstFileUuid = fileUuidOf(firstFile, "00001-");
    UUID secondFileUuid = fileUuidOf(secondFile, "00002-");
    Assertions.assertNotEquals(
        firstFileUuid, secondFileUuid, "each file needs its own collision-avoidance UUID");
    Assertions.assertNotEquals(
        UUID.fromString(ViewTestFixtures.VIEW_UUID),
        firstFileUuid,
        "the per-file UUID is not the entity identity, and must not be reused as one");
    Assertions.assertNotEquals(UUID.fromString(ViewTestFixtures.VIEW_UUID), secondFileUuid);
  }

  private static UUID fileUuidOf(String fileName, String versionPrefix) {
    String candidate =
        fileName.substring(versionPrefix.length(), fileName.indexOf(".metadata.json"));
    try {
      return UUID.fromString(candidate);
    } catch (IllegalArgumentException e) {
      throw new AssertionError(
          "the metadata file name must embed a real UUID, got: " + candidate, e);
    }
  }

  /**
   * A replace takes its physical identity from what is published, so a caller that supplies a
   * different UUID, a different root, and an unusable storage type still replaces in place. Without
   * this, a future cleanup could quietly turn a replace into a reallocation.
   */
  @Test
  void replaceIgnoresConflictingCreateOnlyPhysicalFieldsAndPreservesThePublishedOnes() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));
    ViewMetadata createdMetadata = harness.readMetadata(created.getPointer().getMetadataLocation());

    String hostileLocation =
        ViewTestFixtures.allocatedViewLocation(root, DB, VIEW, ViewTestFixtures.SECOND_VIEW_UUID);
    ViewCommitResult replaced =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .schema(ViewTestFixtures.schemaV2())
                    .representations(
                        Collections.singletonList(
                            ViewTestFixtures.sql(
                                ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
                    .viewUuid(ViewTestFixtures.SECOND_VIEW_UUID)
                    .viewLocation(hostileLocation)
                    .storageType("no-such-storage")
                    .baseViewVersion(created.getPointer().getMetadataLocation())
                    .build());
    ViewMetadata replacedMetadata =
        harness.readMetadata(replaced.getPointer().getMetadataLocation());

    Assertions.assertEquals(ViewTestFixtures.VIEW_UUID, replaced.getViewUuid());
    Assertions.assertEquals(createdMetadata.uuid(), replacedMetadata.uuid());
    Assertions.assertEquals(createdMetadata.location(), replacedMetadata.location());
    Assertions.assertTrue(
        replaced.getPointer().getMetadataLocation().startsWith(createdMetadata.location()),
        "the replacement file must stay under the published root: "
            + replaced.getPointer().getMetadataLocation());
    Assertions.assertFalse(
        replaced.getPointer().getMetadataLocation().startsWith(hostileLocation),
        "an incoming location must never be used on a replace");
    Assertions.assertEquals(
        ViewTestFixtures.LOCAL_STORAGE_TYPE,
        replaced.getPointer().getStorageType(),
        "storage comes from the published row, so an unusable incoming value is irrelevant");
    Assertions.assertEquals(
        ViewTestFixtures.VIEW_UUID,
        replacedMetadata.properties().get(CatalogConstants.OPENHOUSE_UUID_KEY));
  }

  /** Server-owned properties are authoritative. */
  @Test
  void callerSuppliedReservedPropertyIsRejectedBeforeAnythingIsWritten() {
    Map<String, String> hostile = new LinkedHashMap<>();
    hostile.put(CatalogConstants.OPENHOUSE_UUID_KEY, "00000000-0000-0000-0000-000000000000");

    Assertions.assertThrows(
        BadRequestException.class,
        () ->
            harness
                .getViewCommitEngine()
                .commit(ViewTestFixtures.baseIntent(root).viewProperties(hostile).build()));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    verify(harness.getCodec(), never()).write(any(ViewMetadata.class), any(OutputFile.class));
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  /* ---- Missing create-side inputs fail rather than fall back to allocation. ---- */

  @Test
  void createWithoutTheSuppliedIdentityFailsInsteadOfMintingOne() {
    assertCreateRejectsMissing(builder -> builder.viewUuid(null), "viewUuid");
  }

  @Test
  void createWithoutTheSuppliedLocationFailsInsteadOfAllocatingOne() {
    assertCreateRejectsMissing(builder -> builder.viewLocation(null), "viewLocation");
  }

  @Test
  void createWithoutTheSuppliedStorageTypeFailsInsteadOfSelectingOne() {
    assertCreateRejectsMissing(builder -> builder.storageType(null), "storageType");
  }

  private void assertCreateRejectsMissing(
      UnaryOperator<ViewCommitIntent.ViewCommitIntentBuilder> mutation, String field) {
    ViewCommitIntent intent = mutation.apply(ViewTestFixtures.baseIntent(root)).build();

    BadRequestException thrown =
        Assertions.assertThrows(
            BadRequestException.class, () -> harness.getViewCommitEngine().commit(intent));

    Assertions.assertTrue(
        thrown.getMessage() != null && thrown.getMessage().contains(field),
        "the failure must name the missing field: " + thrown.getMessage());
    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertTrue(
        harness.metadataFiles().isEmpty(), "a rejected create writes nothing for " + field);
  }

  /* ---- Version identity is Iceberg's, not OpenHouse's. ---- */

  /** Materially different steps, asserting only resulting metadata: no candidate id, no max+1. */
  @Test
  void versionIdsAndHistoryAreAssignedByIcebergAcrossMateriallyDifferentDefinitions() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));
    ViewMetadata afterCreate = harness.readMetadata(created.getPointer().getMetadataLocation());
    Assertions.assertEquals(1, afterCreate.versions().size());
    Assertions.assertEquals(1, afterCreate.history().size());
    Assertions.assertEquals(
        afterCreate.currentVersionId(), afterCreate.history().get(0).versionId());

    ViewCommitResult second = harness.getViewCommitEngine().commit(changedReplaceOf(created));
    ViewMetadata afterSecond = harness.readMetadata(second.getPointer().getMetadataLocation());
    Assertions.assertEquals(2, afterSecond.versions().size());
    Assertions.assertEquals(2, afterSecond.history().size());
    Assertions.assertNotEquals(afterCreate.currentVersionId(), afterSecond.currentVersionId());

    ViewCommitResult third =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .schema(ViewTestFixtures.schemaV2())
                    .representations(
                        Collections.singletonList(
                            ViewTestFixtures.sql(
                                ViewTestFixtures.SQL_V3, ViewTestFixtures.SPARK_DIALECT)))
                    .baseViewVersion(second.getPointer().getMetadataLocation())
                    .build());
    ViewMetadata afterThird = harness.readMetadata(third.getPointer().getMetadataLocation());
    Assertions.assertEquals(3, afterThird.versions().size());
    Assertions.assertEquals(3, afterThird.history().size());
    Assertions.assertEquals(
        afterThird.currentVersionId(),
        afterThird.history().get(afterThird.history().size() - 1).versionId());

    List<Integer> historyIds =
        afterThird.history().stream().map(entry -> entry.versionId()).collect(Collectors.toList());
    Assertions.assertEquals(historyIds.size(), historyIds.stream().distinct().count());
    Assertions.assertTrue(historyIds.contains(afterCreate.currentVersionId()));
    Assertions.assertTrue(historyIds.contains(afterSecond.currentVersionId()));
  }

  /* ---- No-op detection. ---- */

  /** Only the candidate timestamp and summary differ, so nothing observable changes. */
  @Test
  void identicalDefinitionReplaceIsANoOpThatWritesNothing() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));
    ViewMetadata afterCreate = harness.readMetadata(created.getPointer().getMetadataLocation());
    int filesAfterCreate = harness.metadataFiles().size();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int writesAfterCreate = harness.codecWrites();
    HouseTable pointerAfterCreate = harness.getHouseTableRepository().peek(DB, VIEW).get();

    ViewCommitResult replayed =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.replaceIntent(root, created.getPointer().getMetadataLocation()));

    Assertions.assertFalse(replayed.isCreated());
    Assertions.assertFalse(replayed.isMetadataChanged());
    Assertions.assertEquals(
        created.getPointer().getMetadataLocation(), replayed.getPointer().getMetadataLocation());
    Assertions.assertEquals(created.getViewUuid(), replayed.getViewUuid());
    Assertions.assertEquals(created.getLastModifiedTime(), replayed.getLastModifiedTime());
    Assertions.assertEquals(filesAfterCreate, harness.metadataFiles().size());
    Assertions.assertEquals(savesAfterCreate, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        writesAfterCreate,
        harness.codecWrites(),
        "a no-op must not ask the codec to write at all; an unchanged file count would also pass"
            + " if the candidate simply overwrote its predecessor");

    HouseTable pointerAfterReplay = harness.getHouseTableRepository().peek(DB, VIEW).get();
    Assertions.assertEquals(
        pointerAfterCreate.getTableLocation(), pointerAfterReplay.getTableLocation());
    Assertions.assertEquals(
        pointerAfterCreate.getTableVersion(), pointerAfterReplay.getTableVersion());

    ViewMetadata unchanged = harness.readMetadata(replayed.getPointer().getMetadataLocation());
    Assertions.assertEquals(afterCreate.currentVersionId(), unchanged.currentVersionId());
    Assertions.assertEquals(afterCreate.versions().size(), unchanged.versions().size());
    Assertions.assertEquals(afterCreate.history().size(), unchanged.history().size());
  }

  /**
   * Null and the empty namespace are the same value, so they must be the same value on both sides
   * of the comparison. Building with one and comparing against the other would make every replace
   * of a namespace-less view look like a change and publish a new file for nothing.
   */
  @Test
  void aNullDefaultNamespaceRoundTripsAsEmptyAndReplayingItIsANoOp() {
    ViewCommitResult created =
        harness
            .getViewCommitEngine()
            .commit(ViewTestFixtures.baseIntent(root).defaultNamespace(null).build());
    ViewMetadata createdMetadata = harness.readMetadata(created.getPointer().getMetadataLocation());
    Assertions.assertEquals(
        Namespace.empty(),
        createdMetadata.currentVersion().defaultNamespace(),
        "a null namespace is persisted as the empty namespace");

    int filesAfterCreate = harness.metadataFiles().size();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int writesAfterCreate = harness.codecWrites();

    ViewCommitResult replayedWithNull =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .defaultNamespace(null)
                    .baseViewVersion(created.getPointer().getMetadataLocation())
                    .build());
    Assertions.assertFalse(
        replayedWithNull.isMetadataChanged(), "replaying a null namespace is a no-op");

    ViewCommitResult replayedWithEmpty =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .defaultNamespace(Namespace.empty())
                    .baseViewVersion(created.getPointer().getMetadataLocation())
                    .build());
    Assertions.assertFalse(
        replayedWithEmpty.isMetadataChanged(),
        "the empty namespace means what null meant, so this is the same definition");

    Assertions.assertEquals(filesAfterCreate, harness.metadataFiles().size());
    Assertions.assertEquals(savesAfterCreate, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        writesAfterCreate, harness.codecWrites(), "neither replay may ask the codec to write");
  }

  /**
   * Null properties and empty properties are the same submission, and neither may be read as an
   * instruction to clear what is already stored.
   */
  @Test
  void nullAndEmptyViewPropertiesAreTheSameSubmissionAndPreserveStoredOnes() {
    Map<String, String> initial = new LinkedHashMap<>();
    initial.put("a", "1");
    initial.put("keep", "yes");
    ViewCommitResult created =
        harness
            .getViewCommitEngine()
            .commit(ViewTestFixtures.baseIntent(root).viewProperties(initial).build());
    int filesAfterCreate = harness.metadataFiles().size();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int writesAfterCreate = harness.codecWrites();

    ViewCommitResult replayedWithNull =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .viewProperties(null)
                    .baseViewVersion(created.getPointer().getMetadataLocation())
                    .build());
    Assertions.assertFalse(
        replayedWithNull.isMetadataChanged(),
        "omitting properties is not a change; it certainly is not a request to delete them");

    ViewCommitResult replayedWithEmpty =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .viewProperties(Collections.emptyMap())
                    .baseViewVersion(created.getPointer().getMetadataLocation())
                    .build());
    Assertions.assertFalse(
        replayedWithEmpty.isMetadataChanged(), "an empty map means exactly what null meant");

    Assertions.assertEquals(filesAfterCreate, harness.metadataFiles().size());
    Assertions.assertEquals(savesAfterCreate, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(writesAfterCreate, harness.codecWrites());

    Map<String, String> stillStored =
        harness.readMetadata(created.getPointer().getMetadataLocation()).properties();
    Assertions.assertEquals("1", stillStored.get("a"));
    Assertions.assertEquals(
        "yes", stillStored.get("keep"), "omitted properties must survive an omitting replace");
  }

  /** Equivalence is not blindness: moving off the empty namespace is still a change. */
  @Test
  void movingFromAnEmptyNamespaceToANonEmptyOneCommits() {
    ViewCommitResult created =
        harness
            .getViewCommitEngine()
            .commit(ViewTestFixtures.baseIntent(root).defaultNamespace(null).build());
    int filesAfterCreate = harness.metadataFiles().size();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();

    ViewCommitResult updated =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .defaultNamespace(Namespace.of(DB))
                    .baseViewVersion(created.getPointer().getMetadataLocation())
                    .build());

    Assertions.assertTrue(updated.isMetadataChanged());
    Assertions.assertEquals(filesAfterCreate + 1, harness.metadataFiles().size());
    Assertions.assertEquals(
        savesAfterCreate + 1, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        Namespace.of(DB),
        harness
            .readMetadata(updated.getPointer().getMetadataLocation())
            .currentVersion()
            .defaultNamespace());
  }

  /** Persisted state changes even when Iceberg reuses the version id. */
  @Test
  void propertyOnlyReplaceStillWritesAndPublishes() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();

    ViewCommitResult updated =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .viewProperties(ViewTestFixtures.userProperties("a", "2"))
                    .baseViewVersion(created.getPointer().getMetadataLocation())
                    .build());

    Assertions.assertFalse(updated.isCreated());
    Assertions.assertTrue(updated.isMetadataChanged());
    Assertions.assertNotEquals(
        created.getPointer().getMetadataLocation(), updated.getPointer().getMetadataLocation());
    Assertions.assertEquals(
        savesAfterCreate + 1, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(2, harness.metadataFiles().size());

    ViewMetadata metadata = harness.readMetadata(updated.getPointer().getMetadataLocation());
    Assertions.assertEquals("2", metadata.properties().get("a"));
  }

  /** Omitted properties survive; supplied ones win. */
  @Test
  void replacePreservesOmittedUserPropertiesAndMergesSuppliedOnes() {
    Map<String, String> initial = new LinkedHashMap<>();
    initial.put("a", "1");
    initial.put("keep", "yes");
    ViewCommitResult created =
        harness
            .getViewCommitEngine()
            .commit(ViewTestFixtures.baseIntent(root).viewProperties(initial).build());

    ViewCommitResult updated =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .viewProperties(ViewTestFixtures.userProperties("a", "2"))
                    .baseViewVersion(created.getPointer().getMetadataLocation())
                    .build());

    Map<String, String> properties =
        harness.readMetadata(updated.getPointer().getMetadataLocation()).properties();
    Assertions.assertEquals("2", properties.get("a"));
    Assertions.assertEquals("yes", properties.get("keep"));
  }

  /* ---- Bounding the engine-owned structural comparison: one field changes per test, so an
   * under-comparing implementation cannot hide behind another field. ---- */

  private static final List<SqlViewRepresentationIntent> BOTH_DIALECTS_V1 =
      ViewTestFixtures.sparkAndTrino(ViewTestFixtures.SQL_V1);

  private ViewCommitIntent changedReplaceOf(ViewCommitResult created) {
    return ViewTestFixtures.baseIntent(root)
        .schema(ViewTestFixtures.schemaV2())
        .representations(
            Collections.singletonList(
                ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
        .baseViewVersion(created.getPointer().getMetadataLocation())
        .build();
  }

  private ViewCommitResult createWithBothDialects() {
    return harness
        .getViewCommitEngine()
        .commit(ViewTestFixtures.baseIntent(root).representations(BOTH_DIALECTS_V1).build());
  }

  /**
   * A replace differing in exactly one structural field must be a real change, and must persist the
   * NEW value. Asserting only that a file appeared would pass an implementation that detects the
   * change and then writes the field it already had.
   */
  private void assertStructuralChangeIsNotANoOp(
      UnaryOperator<ViewCommitIntent.ViewCommitIntentBuilder> mutation, String changedField) {
    ViewCommitResult created = createWithBothDialects();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int filesAfterCreate = harness.metadataFiles().size();

    ViewCommitIntent intent =
        mutation
            .apply(
                ViewTestFixtures.baseIntent(root)
                    .representations(BOTH_DIALECTS_V1)
                    .baseViewVersion(created.getPointer().getMetadataLocation()))
            .build();

    ViewCommitResult result = harness.getViewCommitEngine().commit(intent);

    Assertions.assertTrue(
        result.isMetadataChanged(), "a changed " + changedField + " is not a no-op");
    Assertions.assertFalse(result.isCreated());
    Assertions.assertNotEquals(
        created.getPointer().getMetadataLocation(),
        result.getPointer().getMetadataLocation(),
        "a changed " + changedField + " must move the pointer");
    Assertions.assertEquals(
        filesAfterCreate + 1,
        harness.metadataFiles().size(),
        "a changed " + changedField + " must write a metadata file");
    Assertions.assertEquals(
        savesAfterCreate + 1,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "a changed " + changedField + " must publish");

    assertPersistedDefinitionMatches(intent, result, changedField);
  }

  /**
   * Reads the submitted definition back out of the file that was actually written, and out of a
   * fresh load, so persisting a stale value cannot pass as a detected change.
   */
  private void assertPersistedDefinitionMatches(
      ViewCommitIntent intent, ViewCommitResult result, String changedField) {
    ViewMetadata persisted = harness.readMetadata(result.getPointer().getMetadataLocation());
    Assertions.assertEquals(
        intent.getSchema().asStruct(),
        persisted.schema().asStruct(),
        "the persisted schema must be the submitted one after changing " + changedField);
    Assertions.assertEquals(
        intent.getSourceDialect(),
        persisted.currentVersion().summary().get(ViewTestFixtures.SOURCE_DIALECT_SUMMARY_KEY),
        "the persisted source dialect must be the submitted one after changing " + changedField);
    Assertions.assertEquals(
        intent.getDefaultCatalog(),
        persisted.currentVersion().defaultCatalog(),
        "the persisted default catalog must be the submitted one after changing " + changedField);
    Assertions.assertEquals(
        intent.getDefaultNamespace() == null ? Namespace.empty() : intent.getDefaultNamespace(),
        persisted.currentVersion().defaultNamespace(),
        "the persisted default namespace must be the submitted one after changing " + changedField);
    Assertions.assertEquals(
        submittedByDialect(intent),
        persistedByDialect(persisted),
        "every submitted representation must be persisted after changing " + changedField);
    Assertions.assertEquals(
        Collections.singleton(intent.getSchema().identifierFieldIds()),
        Collections.singleton(persisted.schema().identifierFieldIds()),
        "identifier fields are part of the definition after changing " + changedField);

    LoadedView reloaded = harness.newEngineInstance().loadView(DB, VIEW);
    Assertions.assertEquals(
        submittedByDialect(intent),
        loadedByDialect(reloaded),
        "a fresh load must report the submitted definition after changing " + changedField);
    Assertions.assertEquals(intent.getSourceDialect(), reloaded.getSourceDialect());
    Assertions.assertEquals(intent.getDefaultCatalog(), reloaded.getDefaultCatalog());
  }

  private static Map<String, String> submittedByDialect(ViewCommitIntent intent) {
    Map<String, String> byDialect = new LinkedHashMap<>();
    intent
        .getRepresentations()
        .forEach(
            representation -> byDialect.put(representation.getDialect(), representation.getSql()));
    return byDialect;
  }

  private static Map<String, String> persistedByDialect(ViewMetadata metadata) {
    Map<String, String> byDialect = new LinkedHashMap<>();
    metadata
        .currentVersion()
        .representations()
        .forEach(
            representation -> {
              SQLViewRepresentation sql = (SQLViewRepresentation) representation;
              Assertions.assertNull(
                  byDialect.put(sql.dialect(), sql.sql()),
                  "a dialect must not be persisted twice: " + sql.dialect());
            });
    return byDialect;
  }

  private static Map<String, String> loadedByDialect(LoadedView loaded) {
    Map<String, String> byDialect = new LinkedHashMap<>();
    loaded
        .getRepresentations()
        .forEach(
            representation ->
                Assertions.assertNull(
                    byDialect.put(representation.getDialect(), representation.getSql()),
                    "a dialect must not be reported twice: " + representation.getDialect()));
    return byDialect;
  }

  @Test
  void aChangedSchemaIsNotANoOp() {
    assertStructuralChangeIsNotANoOp(
        builder -> builder.schema(ViewTestFixtures.schemaV2()), "schema");
  }

  @Test
  void changedSqlTextIsNotANoOp() {
    assertStructuralChangeIsNotANoOp(
        builder -> builder.representations(ViewTestFixtures.sparkAndTrino(ViewTestFixtures.SQL_V2)),
        "SQL text");
  }

  /** Only one representation's SQL changes, so a whole-set comparison would miss it. */
  @Test
  void changedSqlInASingleRepresentationIsNotANoOp() {
    assertStructuralChangeIsNotANoOp(
        builder ->
            builder.representations(
                Arrays.asList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V1, ViewTestFixtures.SPARK_DIALECT),
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, ViewTestFixtures.TRINO_DIALECT))),
        "SQL of one representation");
  }

  /** Adding a dialect is allowed; only dropping one is rejected. */
  @Test
  void anAddedRepresentationDialectIsNotANoOp() {
    List<SqlViewRepresentationIntent> withPresto = new ArrayList<>(BOTH_DIALECTS_V1);
    withPresto.add(ViewTestFixtures.sql(ViewTestFixtures.SQL_V1, "presto"));
    assertStructuralChangeIsNotANoOp(
        builder -> builder.representations(withPresto), "representation set");
  }

  @Test
  void aChangedSourceDialectIsNotANoOp() {
    assertStructuralChangeIsNotANoOp(
        builder -> builder.sourceDialect(ViewTestFixtures.TRINO_DIALECT), "source dialect");
  }

  @Test
  void aChangedDefaultCatalogIsNotANoOp() {
    assertStructuralChangeIsNotANoOp(
        builder -> builder.defaultCatalog("other_catalog"), "default catalog");
  }

  @Test
  void aChangedDefaultNamespaceIsNotANoOp() {
    assertStructuralChangeIsNotANoOp(
        builder -> builder.defaultNamespace(Namespace.of("other_db")), "default namespace");
  }

  /** Identifier fields are in the schema but not the struct, so a column-only check misses them. */
  @Test
  void aChangedIdentifierFieldSetIsNotANoOp() {
    Schema withoutIdentifier =
        new Schema(
            Arrays.asList(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get())),
            Collections.emptySet());
    Schema withIdentifier =
        new Schema(
            Arrays.asList(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get())),
            Collections.singleton(1));
    Assertions.assertEquals(
        withoutIdentifier.asStruct(),
        withIdentifier.asStruct(),
        "the two schemas must differ only in identifier fields, or this test proves nothing");

    ViewCommitResult created =
        harness
            .getViewCommitEngine()
            .commit(ViewTestFixtures.baseIntent(root).schema(withoutIdentifier).build());
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int filesAfterCreate = harness.metadataFiles().size();

    ViewCommitResult updated =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .schema(withIdentifier)
                    .baseViewVersion(created.getPointer().getMetadataLocation())
                    .build());

    Assertions.assertTrue(updated.isMetadataChanged(), "an identifier-field change is a change");
    Assertions.assertEquals(filesAfterCreate + 1, harness.metadataFiles().size());
    Assertions.assertEquals(
        savesAfterCreate + 1, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        Collections.singleton(1),
        harness
            .readMetadata(updated.getPointer().getMetadataLocation())
            .schema()
            .identifierFieldIds());
  }

  /** A map keyed by dialect keeps only the last entry, so a repeat could compare equal. */
  @Test
  void aDuplicateDialectSubmissionIsRejectedRatherThanTreatedAsANoOp() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int filesAfterCreate = harness.metadataFiles().size();

    // The last entry alone equals the stored definition, so a lossy comparison would see no change.
    ViewCommitIntent duplicated =
        ViewTestFixtures.baseIntent(root)
            .representations(
                Arrays.asList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT),
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V1, ViewTestFixtures.SPARK_DIALECT)))
            .baseViewVersion(created.getPointer().getMetadataLocation())
            .build();

    Assertions.assertThrows(
        BadRequestException.class, () -> harness.getViewCommitEngine().commit(duplicated));

    Assertions.assertEquals(filesAfterCreate, harness.metadataFiles().size());
    Assertions.assertEquals(savesAfterCreate, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        created.getPointer().getMetadataLocation(),
        harness.getHouseTableRepository().peek(DB, VIEW).get().getTableLocation());
  }

  /** Iceberg compares dialects case-insensitively, so this check must too. */
  @Test
  void aDuplicateDialectDifferingOnlyInCaseIsAlsoRejected() {
    Assertions.assertThrows(
        BadRequestException.class,
        () ->
            harness
                .getViewCommitEngine()
                .commit(
                    ViewTestFixtures.baseIntent(root)
                        .representations(
                            Arrays.asList(
                                ViewTestFixtures.sql(
                                    ViewTestFixtures.SQL_V1, ViewTestFixtures.SPARK_DIALECT),
                                ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, "SPARK")))
                        .build()));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  /** With the clock pinned, only the engine's monotonic guard can make this true. */
  @Test
  void aChangedReplaceAdvancesLastModifiedEvenWhenTheClockDoesNot() {
    long fixedNow = 1_700_000_000_000L;
    ViewCommitEngine pinnedClock =
        new ViewCommitEngineImpl(
            harness.getHouseTableRepository(),
            harness.getFileIOManager(),
            harness.getRecordingCodec(),
            new StorageType()) {
          @Override
          protected long nowMillis() {
            return fixedNow;
          }
        };

    ViewCommitResult created = pinnedClock.commit(ViewTestFixtures.createIntent(root));
    Assertions.assertEquals(fixedNow, created.getLastModifiedTime());

    ViewCommitResult updated = pinnedClock.commit(changedReplaceOf(created));

    Assertions.assertTrue(updated.isMetadataChanged());
    Assertions.assertTrue(
        updated.getLastModifiedTime() > created.getLastModifiedTime(),
        "a changed replace must advance last-modified even inside one clock tick: "
            + updated.getLastModifiedTime()
            + " vs "
            + created.getLastModifiedTime());
    Assertions.assertEquals(
        updated.getLastModifiedTime(),
        Long.parseLong(
            harness
                .readMetadata(updated.getPointer().getMetadataLocation())
                .properties()
                .get(getCanonicalFieldName("lastModifiedTime"))),
        "the advanced value must be what was persisted");
    Assertions.assertEquals(
        fixedNow,
        Long.parseLong(
            harness
                .readMetadata(updated.getPointer().getMetadataLocation())
                .properties()
                .get(getCanonicalFieldName("creationTime"))),
        "creation time still belongs to the create");
  }

  /* ---- Dialect safety. ---- */

  /** The server stamps {@code replace.drop-dialect.allowed=false}; Iceberg enforces it. */
  @Test
  void replaceDroppingAPreviouslyStoredDialectIsRejected() {
    ViewCommitResult created = createWithBothDialects();

    ViewMetadata createdMetadata = harness.readMetadata(created.getPointer().getMetadataLocation());
    Assertions.assertEquals(
        "false", createdMetadata.properties().get(ViewProperties.REPLACE_DROP_DIALECT_ALLOWED));
    int filesAfterCreate = harness.metadataFiles().size();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();

    IllegalStateException thrown =
        Assertions.assertThrows(
            IllegalStateException.class,
            () ->
                harness
                    .getViewCommitEngine()
                    .commit(
                        ViewTestFixtures.baseIntent(root)
                            .representations(
                                Collections.singletonList(
                                    ViewTestFixtures.sql(
                                        ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
                            .baseViewVersion(created.getPointer().getMetadataLocation())
                            .build()));
    // Pin the dialect-specific failure, so an unrelated engine ISE cannot satisfy this test.
    Assertions.assertTrue(
        thrown.getMessage() != null && thrown.getMessage().contains("view dialects"),
        "expected the dropped-dialect failure, got: " + thrown.getMessage());
    Assertions.assertTrue(
        thrown.getMessage().contains(ViewProperties.REPLACE_DROP_DIALECT_ALLOWED),
        "the failure must name the guard that rejected it: " + thrown.getMessage());

    Assertions.assertEquals(filesAfterCreate, harness.metadataFiles().size());
    Assertions.assertEquals(savesAfterCreate, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        created.getPointer().getMetadataLocation(),
        harness.getHouseTableRepository().peek(DB, VIEW).get().getTableLocation());
  }

  /** A caller cannot re-enable dialect dropping. */
  @Test
  void callerCannotOverrideTheDropDialectGuard() {
    Map<String, String> hostile = new HashMap<>();
    hostile.put(ViewProperties.REPLACE_DROP_DIALECT_ALLOWED, "true");

    Assertions.assertThrows(
        BadRequestException.class,
        () ->
            harness
                .getViewCommitEngine()
                .commit(ViewTestFixtures.baseIntent(root).viewProperties(hostile).build()));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  /* ---- Stamping golden round-trip. ---- */

  /**
   * The {@code openhouse.table*} namespace is reused for views on purpose: House Table stores an
   * entity-neutral pointer. Entity type is never stamped into metadata; it belongs to the row.
   */
  @Test
  void createStampsInitialVersionAndReplaceStampsThePriorExactPath() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));
    ViewMetadata createdMetadata = harness.readMetadata(created.getPointer().getMetadataLocation());
    Map<String, String> createdProperties = createdMetadata.properties();

    Assertions.assertEquals(
        CatalogConstants.INITIAL_VERSION,
        createdProperties.get(getCanonicalFieldName("tableVersion")));
    Assertions.assertEquals(
        created.getPointer().getMetadataLocation(),
        createdProperties.get(getCanonicalFieldName("tableLocation")));
    Assertions.assertEquals(VIEW, createdProperties.get(CatalogConstants.OPENHOUSE_TABLEID_KEY));
    Assertions.assertEquals(DB, createdProperties.get(CatalogConstants.OPENHOUSE_DATABASEID_KEY));
    Assertions.assertEquals(
        ViewTestFixtures.CREATOR, createdProperties.get(getCanonicalFieldName("tableCreator")));
    Assertions.assertNotNull(createdProperties.get(getCanonicalFieldName("creationTime")));
    Assertions.assertNotNull(createdProperties.get(getCanonicalFieldName("lastModifiedTime")));
    Assertions.assertEquals(
        createdProperties.get(getCanonicalFieldName("creationTime")),
        createdProperties.get(getCanonicalFieldName("lastModifiedTime")));
    Assertions.assertFalse(
        createdProperties.containsKey(getCanonicalFieldName("entityType")),
        "entity type belongs to the House Table row and its route, never to metadata");
    Assertions.assertEquals("1", createdProperties.get("a"));

    Assertions.assertEquals(
        ViewTestFixtures.SPARK_DIALECT,
        createdMetadata
            .currentVersion()
            .summary()
            .get(ViewTestFixtures.SOURCE_DIALECT_SUMMARY_KEY));

    // The resolution context and schema the caller supplied must survive verbatim: they are part of
    // what makes two submissions structurally equal, so a lossy round trip would corrupt no-op
    // detection as well as the view itself.
    Assertions.assertEquals("openhouse", createdMetadata.currentVersion().defaultCatalog());
    Assertions.assertEquals(
        Collections.singletonList(DB),
        Arrays.asList(createdMetadata.currentVersion().defaultNamespace().levels()));
    Assertions.assertEquals(
        ViewTestFixtures.schemaV1().asStruct(),
        createdMetadata.schema().asStruct(),
        "the supplied schema must round trip through the metadata file");
    Assertions.assertEquals(
        createdMetadata.currentVersion().schemaId(), createdMetadata.currentSchemaId().intValue());

    List<SQLViewRepresentation> representations =
        createdMetadata.currentVersion().representations().stream()
            .map(SQLViewRepresentation.class::cast)
            .collect(Collectors.toList());
    Assertions.assertEquals(1, representations.size());
    Assertions.assertEquals(ViewTestFixtures.SQL_V1, representations.get(0).sql());
    Assertions.assertEquals(ViewTestFixtures.SPARK_DIALECT, representations.get(0).dialect());

    Assertions.assertEquals(
        Long.parseLong(createdProperties.get(getCanonicalFieldName("lastModifiedTime"))),
        created.getLastModifiedTime(),
        "the returned last-modified time must be the one persisted in metadata");

    ViewCommitResult replaced = harness.getViewCommitEngine().commit(changedReplaceOf(created));
    Map<String, String> replacedProperties =
        harness.readMetadata(replaced.getPointer().getMetadataLocation()).properties();

    Assertions.assertEquals(
        created.getPointer().getMetadataLocation(),
        replacedProperties.get(getCanonicalFieldName("tableVersion")));
    Assertions.assertEquals(
        replaced.getPointer().getMetadataLocation(),
        replacedProperties.get(getCanonicalFieldName("tableLocation")));
    Assertions.assertEquals(
        createdProperties.get(getCanonicalFieldName("creationTime")),
        replacedProperties.get(getCanonicalFieldName("creationTime")));
    Assertions.assertNotEquals(
        createdProperties.get(getCanonicalFieldName("lastModifiedTime")),
        replacedProperties.get(getCanonicalFieldName("lastModifiedTime")));
  }

  /** A re-read, a default, or a re-derived token turns a conditional write into a blind one. */
  @Test
  void publishedPointerRowCarriesTheNewPathAndTheCapturedBaseAsExpectedVersion() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));

    HouseTable afterCreate = harness.getHouseTableRepository().peek(DB, VIEW).get();
    Assertions.assertEquals(
        created.getPointer().getMetadataLocation(), afterCreate.getTableLocation());
    Assertions.assertEquals(
        CatalogConstants.INITIAL_VERSION,
        afterCreate.getTableVersion(),
        "a create must claim the name with INITIAL_VERSION");
    Assertions.assertEquals(ViewTestFixtures.LOCAL_STORAGE_TYPE, afterCreate.getStorageType());
    Assertions.assertEquals(DB, afterCreate.getDatabaseId());
    Assertions.assertEquals(VIEW, afterCreate.getTableId());

    String capturedBase = created.getPointer().getMetadataLocation();
    harness.clearEvents();

    ViewCommitResult replaced = harness.getViewCommitEngine().commit(changedReplaceOf(created));

    HouseTable afterReplace = harness.getHouseTableRepository().peek(DB, VIEW).get();
    Assertions.assertEquals(
        replaced.getPointer().getMetadataLocation(), afterReplace.getTableLocation());
    Assertions.assertEquals(
        capturedBase,
        afterReplace.getTableVersion(),
        "a replace must send back exactly the path the caller captured");

    List<String> events = harness.events();
    Assertions.assertEquals(
        1,
        countStartingWith(events, InMemoryViewHouseTableRepository.FIND_VIEW),
        "a changed replace reads the pointer exactly once: " + events);
    Assertions.assertEquals(
        1,
        countStartingWith(events, InMemoryViewHouseTableRepository.SAVE_VIEW),
        "a changed replace publishes exactly once: " + events);

    int writeAt = indexOfStartingWith(events, RecordingViewMetadataCodec.WRITE);
    int saveAt = indexOfStartingWith(events, InMemoryViewHouseTableRepository.SAVE_VIEW);
    int readAt = indexOfStartingWith(events, InMemoryViewHouseTableRepository.FIND_VIEW);
    Assertions.assertTrue(writeAt >= 0 && saveAt >= 0 && readAt >= 0, "events: " + events);
    Assertions.assertTrue(
        writeAt < saveAt, "the immutable file must be written before publishing: " + events);
    Assertions.assertTrue(
        readAt < writeAt, "the base is captured before the file is built: " + events);
    Assertions.assertEquals(
        readAt,
        lastIndexOfStartingWith(events, InMemoryViewHouseTableRepository.FIND_VIEW),
        "nothing may be re-read between capturing the base and swapping: " + events);
    Assertions.assertTrue(
        events.get(saveAt).contains("expected=" + capturedBase),
        "the swap must carry the captured base as its token: " + events.get(saveAt));
    Assertions.assertEquals(
        saveAt, events.size() - 1, "the swap is the last thing that happens: " + events);
  }

  private static int countStartingWith(List<String> events, String prefix) {
    return (int) events.stream().filter(event -> event.startsWith(prefix)).count();
  }

  private static int indexOfStartingWith(List<String> events, String prefix) {
    for (int i = 0; i < events.size(); i++) {
      if (events.get(i).startsWith(prefix)) {
        return i;
      }
    }
    return -1;
  }

  private static int lastIndexOfStartingWith(List<String> events, String prefix) {
    for (int i = events.size() - 1; i >= 0; i--) {
      if (events.get(i).startsWith(prefix)) {
        return i;
      }
    }
    return -1;
  }

  /** The create publish carries INITIAL_VERSION, and writes first. */
  @Test
  void createPublishesInitialVersionAfterWritingItsFile() {
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));

    List<String> events = harness.events();
    int probeAt = indexOfStartingWith(events, InMemoryViewHouseTableRepository.FIND_ENTITY);
    int writeAt = indexOfStartingWith(events, RecordingViewMetadataCodec.WRITE);
    int saveAt = indexOfStartingWith(events, InMemoryViewHouseTableRepository.SAVE_VIEW);
    Assertions.assertTrue(probeAt >= 0 && writeAt >= 0 && saveAt >= 0, "events: " + events);
    Assertions.assertTrue(probeAt < writeAt, "occupancy is checked first: " + events);
    Assertions.assertTrue(writeAt < saveAt, "write before publish: " + events);
    Assertions.assertTrue(
        events.get(saveAt).contains("expected=" + CatalogConstants.INITIAL_VERSION),
        "events: " + events);
    Assertions.assertEquals(
        1, countStartingWith(events, InMemoryViewHouseTableRepository.SAVE_VIEW));
    Assertions.assertEquals(
        1, countStartingWith(events, InMemoryViewHouseTableRepository.FIND_ENTITY));
  }

  /** A partial write would break engines reading the missing dialect. */
  @Test
  void everySuppliedRepresentationAndUserPropertyIsPersisted() {
    Map<String, String> userProperties = new LinkedHashMap<>();
    userProperties.put("owner", "team-a");
    userProperties.put("comment", "a view");

    ViewCommitResult created =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root)
                    .representations(ViewTestFixtures.sparkAndTrino(ViewTestFixtures.SQL_V1))
                    .viewProperties(userProperties)
                    .build());

    ViewMetadata metadata = harness.readMetadata(created.getPointer().getMetadataLocation());
    Map<String, String> byDialect = new HashMap<>();
    metadata
        .currentVersion()
        .representations()
        .forEach(
            representation -> {
              SQLViewRepresentation sql = (SQLViewRepresentation) representation;
              byDialect.put(sql.dialect(), sql.sql());
            });
    Assertions.assertEquals(2, byDialect.size(), "both dialects must be persisted: " + byDialect);
    Assertions.assertEquals(ViewTestFixtures.SQL_V1, byDialect.get(ViewTestFixtures.SPARK_DIALECT));
    Assertions.assertEquals(ViewTestFixtures.SQL_V1, byDialect.get(ViewTestFixtures.TRINO_DIALECT));

    Assertions.assertEquals("team-a", metadata.properties().get("owner"));
    Assertions.assertEquals("a view", metadata.properties().get("comment"));
  }

  /** What was committed is exactly what a load reports. */
  @Test
  void aCommittedViewLoadsBackWithTheSameDefinition() {
    ViewCommitResult created = createWithBothDialects();
    ViewMetadata metadata = harness.readMetadata(created.getPointer().getMetadataLocation());

    LoadedView loaded = harness.newEngineInstance().loadView(DB, VIEW);

    Assertions.assertEquals(created.getViewUuid(), loaded.getViewUuid());
    Assertions.assertEquals(
        created.getPointer().getMetadataLocation(), loaded.getPointer().getMetadataLocation());
    Assertions.assertEquals(metadata.currentVersionId(), loaded.getCurrentVersionId());
    Assertions.assertEquals(ViewTestFixtures.schemaV1().asStruct(), loaded.getSchema().asStruct());
    Assertions.assertEquals("openhouse", loaded.getDefaultCatalog());
    Assertions.assertEquals(Namespace.of(DB), loaded.getDefaultNamespace());
    Assertions.assertEquals(ViewTestFixtures.SPARK_DIALECT, loaded.getSourceDialect());

    // The complete dialect-to-SQL mapping, not just its size: returning one dialect twice and
    // dropping the other would otherwise pass, and would silently break every engine reading the
    // dropped dialect.
    Map<String, String> submitted = new LinkedHashMap<>();
    ViewTestFixtures.sparkAndTrino(ViewTestFixtures.SQL_V1)
        .forEach(
            representation -> submitted.put(representation.getDialect(), representation.getSql()));
    Map<String, String> loadedByDialect = new LinkedHashMap<>();
    loaded
        .getRepresentations()
        .forEach(
            representation ->
                Assertions.assertNull(
                    loadedByDialect.put(representation.getDialect(), representation.getSql()),
                    "a dialect must not be reported twice: " + representation.getDialect()));
    Assertions.assertEquals(submitted, loadedByDialect);

    Assertions.assertEquals(
        ViewTestFixtures.userProperties("a", "1").entrySet(),
        loaded.getProperties().entrySet().stream()
            .filter(entry -> !entry.getKey().startsWith("openhouse."))
            .filter(entry -> !entry.getKey().startsWith("replace."))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            .entrySet(),
        "the public model must carry exactly the user properties that were submitted");
    Assertions.assertEquals(created.getLastModifiedTime(), loaded.getLastModifiedTime());
  }

  /* ---- Base-token handling on replace. ---- */

  /** A stale base is rejected before any write. */
  @Test
  void replaceWithAStaleBaseTokenFailsBeforeWritingAnything() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root));
    int filesAfterCreate = harness.metadataFiles().size();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();

    ViewCommitIntent stale =
        ViewTestFixtures.baseIntent(root)
            .schema(ViewTestFixtures.schemaV2())
            .representations(
                Collections.singletonList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
            .baseViewVersion(created.getPointer().getMetadataLocation() + ".stale")
            .build();

    Assertions.assertThrows(
        CommitFailedException.class, () -> harness.getViewCommitEngine().commit(stale));

    Assertions.assertEquals(filesAfterCreate, harness.metadataFiles().size());
    Assertions.assertEquals(savesAfterCreate, harness.getHouseTableRepository().getSaveViewCalls());
    verify(harness.getCodec(), times(1)).write(any(ViewMetadata.class), any(OutputFile.class));
    Assertions.assertEquals(
        created.getPointer().getMetadataLocation(),
        harness.getHouseTableRepository().peek(DB, VIEW).get().getTableLocation());
  }

  /** A load failure, not an implicit create. */
  @Test
  void replaceOfAnAbsentViewNeverBecomesACreate() {
    Assertions.assertThrows(
        NoSuchViewException.class,
        () ->
            harness
                .getViewCommitEngine()
                .commit(ViewTestFixtures.replaceIntent(root, "/nowhere/00001-a.metadata.json")));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  /** A key occupied by a table reads as absent through the view route, so it is not a view. */
  @Test
  void replaceOfATablePointerIsRejectedAsNoSuchView() {
    harness
        .getHouseTableRepository()
        .seed(ViewTestFixtures.tableRow("/existing/00001-a.metadata.json"));

    Assertions.assertThrows(
        NoSuchViewException.class,
        () ->
            harness
                .getViewCommitEngine()
                .commit(ViewTestFixtures.replaceIntent(root, "/existing/00001-a.metadata.json")));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }
}
