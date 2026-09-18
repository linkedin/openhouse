package com.linkedin.openhouse.internal.catalog.view;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;
import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.DB;
import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.VIEW;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.view.model.LoadedView;
import com.linkedin.openhouse.internal.catalog.view.model.SqlViewRepresentationIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
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
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/** Create and replace: collision classification, identity, no-op detection, dialect safety. */
public class ViewCommitEngineCommitTest {

  private ViewCommitEngineHarness harness;
  private Path root;

  @BeforeEach
  void setUp(@TempDir Path tempDir) {
    root = tempDir;
    harness = new ViewCommitEngineHarness(tempDir);
  }

  @Test
  void createCollidingWithAnExistingViewReportsViewAlreadyExists() {
    harness
        .getHouseTableRepository()
        .seed(ViewTestFixtures.viewRow("/existing/00001-a.metadata.json"));
    HouseTable occupant = captureNeutral();
    int readsBeforeCommit = harness.readCalls();

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () -> harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, occupant)));

    assertCreateCollisionLeftNoTrace(readsBeforeCommit);
  }

  /** A non-view occupant is a name collision reported with its actual (hydrated) type. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("nonViewCreateCollisions")
  void createCollidingWithANonViewIsNameOccupiedCarryingTheActualType(
      String caseName, HouseTable seeded, String expectedOccupantType) {
    harness.getHouseTableRepository().seed(seeded);
    // The caller passes the hydrated neutral read, so a legacy null is already TABLE here.
    HouseTable occupant = captureNeutral();
    Assertions.assertEquals(
        expectedOccupantType,
        occupant.getEntityType(),
        "the caller captures the hydrated discriminator the engine will classify");
    int readsBeforeCommit = harness.readCalls();

    ViewNameOccupiedException thrown =
        Assertions.assertThrows(
            ViewNameOccupiedException.class,
            () ->
                harness
                    .getViewCommitEngine()
                    .commit(ViewTestFixtures.createIntent(root, occupant)));

    Assertions.assertEquals(expectedOccupantType, thrown.getOccupantEntityType());
    Assertions.assertEquals(DB, thrown.getDatabaseId());
    Assertions.assertEquals(VIEW, thrown.getViewId());
    assertCreateCollisionLeftNoTrace(readsBeforeCommit);
  }

  private static Stream<Arguments> nonViewCreateCollisions() {
    String path = "/existing/00001-a.metadata.json";
    return Stream.of(
        Arguments.of(
            "stored TABLE stays TABLE",
            ViewTestFixtures.tableRow(path),
            ViewTestFixtures.ENTITY_TYPE_TABLE),
        Arguments.of(
            "raw legacy null hydrates to TABLE",
            ViewTestFixtures.legacyRow(path),
            ViewTestFixtures.ENTITY_TYPE_TABLE),
        Arguments.of(
            "unknown discriminator fails closed",
            ViewTestFixtures.row(ViewTestFixtures.ENTITY_TYPE_UNKNOWN, path),
            ViewTestFixtures.ENTITY_TYPE_UNKNOWN),
        Arguments.of("lowercase view fails closed", ViewTestFixtures.row("view", path), "view"));
  }

  @Test
  void createOnAFreeNameProceedsAndPublishesExactlyOnce() {
    ViewCommitResult result =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));

    Assertions.assertTrue(result.isCreated());
    Assertions.assertTrue(result.isMetadataChanged());
    Assertions.assertEquals(1, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        0,
        harness.readCalls(),
        "a create classifies the supplied absence and never reads House Table");
    Assertions.assertEquals(1, harness.metadataFiles().size());
    Optional<HouseTable> pointer = harness.getHouseTableRepository().peek(DB, VIEW);
    Assertions.assertTrue(pointer.isPresent());
    Assertions.assertEquals(
        result.getPointer().getMetadataLocation(), pointer.get().getTableLocation());
  }

  /** The upstream lookup the future caller performs once, before the engine is invoked. */
  private HouseTable captureNeutral() {
    return harness
        .getHouseTableRepository()
        .findEntityById(ViewTestFixtures.key(DB, VIEW))
        .orElse(null);
  }

  /** A supplied occupant is classified in memory: no engine read, FileIO, file, or publish. */
  private void assertCreateCollisionLeftNoTrace(int readsBeforeCommit) {
    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        readsBeforeCommit,
        harness.readCalls(),
        "classifying a supplied occupant must not read House Table again");
    verify(harness.getCodec(), never()).read(any(InputFile.class));
    verify(harness.getCodec(), never()).write(any(ViewMetadata.class), any(OutputFile.class));
    verify(harness.getFileIOManager(), never()).getFileIO(any(StorageType.Type.class));
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  @Test
  void createUsesTheCallerSuppliedIdentityLocationAndStorage() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
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

    // Resolved from the supplied storage type; no selector seam is left to consult.
    verify(harness.getFileIOManager(), times(1)).getFileIO(StorageType.LOCAL);
    verify(harness.getFileIOManager(), never()).getStorage(any(FileIO.class));
  }

  /**
   * LOCAL is the fixture default, so only a different type distinguishes supplied from hardcoded.
   */
  @Test
  void createResolvesFileIoFromTheSuppliedStorageTypeRatherThanTheDefault() {
    ViewCommitResult created =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
                    .storageType(ViewCommitEngineHarness.ALTERNATE_STORAGE_TYPE)
                    .build());

    verify(harness.getFileIOManager(), times(1)).getFileIO(StorageType.HDFS);
    verify(harness.getFileIOManager(), never()).getFileIO(StorageType.LOCAL);
    // Recovering the type from the FileIO would be lossy: two storages may share one.
    verify(harness.getFileIOManager(), never()).getStorage(any(FileIO.class));

    Assertions.assertEquals(
        ViewCommitEngineHarness.ALTERNATE_STORAGE_TYPE, created.getPointer().getStorageType());
    Assertions.assertEquals(
        ViewCommitEngineHarness.ALTERNATE_STORAGE_TYPE,
        harness.getHouseTableRepository().peek(DB, VIEW).get().getStorageType(),
        "the persisted row must carry the storage the caller selected");
  }

  /**
   * An unusable incoming storage type must not redirect the write, nor be masked by the default.
   */
  @Test
  void replaceResolvesFileIoFromThePublishedRowStorageNotTheIncomingValue() {
    ViewCommitResult created =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
                    .storageType(ViewCommitEngineHarness.ALTERNATE_STORAGE_TYPE)
                    .build());
    HouseTable base = captureNeutral();
    Mockito.clearInvocations(harness.getFileIOManager());

    ViewCommitResult replaced =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.changedReplaceIntent(root, base)
                    .storageType("no-such-storage")
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

  @Test
  void eachMetadataFileIsVersionPrefixedAndCarriesItsOwnRandomUuid() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    ViewCommitResult replaced =
        harness.getViewCommitEngine().commit(changedReplaceOf(captureNeutral()));

    String firstFile =
        Paths.get(created.getPointer().getMetadataLocation()).getFileName().toString();
    String secondFile =
        Paths.get(replaced.getPointer().getMetadataLocation()).getFileName().toString();

    Assertions.assertTrue(firstFile.startsWith("00001-"), firstFile);
    Assertions.assertTrue(secondFile.startsWith("00002-"), secondFile);
    Assertions.assertTrue(firstFile.endsWith(".metadata.json"), firstFile);
    Assertions.assertTrue(secondFile.endsWith(".metadata.json"), secondFile);

    // Parsed, not compared as text: any two distinct strings would satisfy plain inequality.
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

  /** A replace takes physical identity from the published row, so it can never reallocate. */
  @Test
  void replaceIgnoresConflictingCreateOnlyPhysicalFieldsAndPreservesThePublishedOnes() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    ViewMetadata createdMetadata = harness.readMetadata(created.getPointer().getMetadataLocation());
    HouseTable base = captureNeutral();

    String hostileLocation =
        ViewTestFixtures.allocatedViewLocation(root, DB, VIEW, ViewTestFixtures.SECOND_VIEW_UUID);
    ViewCommitResult replaced =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.changedReplaceIntent(root, base)
                    .viewUuid(ViewTestFixtures.SECOND_VIEW_UUID)
                    .viewLocation(hostileLocation)
                    .storageType("no-such-storage")
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

  @Test
  void callerSuppliedReservedPropertyIsRejectedBeforeAnythingIsWritten() {
    Map<String, String> hostile = new LinkedHashMap<>();
    hostile.put(CatalogConstants.OPENHOUSE_UUID_KEY, "00000000-0000-0000-0000-000000000000");

    Assertions.assertThrows(
        BadRequestException.class,
        () ->
            harness
                .getViewCommitEngine()
                .commit(
                    ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
                        .viewProperties(hostile)
                        .build()));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    verify(harness.getCodec(), never()).write(any(ViewMetadata.class), any(OutputFile.class));
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

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
    ViewCommitIntent intent =
        mutation.apply(ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)).build();

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

  /** Asserts only resulting metadata: no candidate id, no max+1. */
  @Test
  void versionIdsAndHistoryAreAssignedByIcebergAcrossMateriallyDifferentDefinitions() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    ViewMetadata afterCreate = harness.readMetadata(created.getPointer().getMetadataLocation());
    Assertions.assertEquals(1, afterCreate.versions().size());
    Assertions.assertEquals(1, afterCreate.history().size());
    Assertions.assertEquals(
        afterCreate.currentVersionId(), afterCreate.history().get(0).versionId());

    ViewCommitResult second =
        harness.getViewCommitEngine().commit(changedReplaceOf(captureNeutral()));
    ViewMetadata afterSecond = harness.readMetadata(second.getPointer().getMetadataLocation());
    Assertions.assertEquals(2, afterSecond.versions().size());
    Assertions.assertEquals(2, afterSecond.history().size());
    Assertions.assertNotEquals(afterCreate.currentVersionId(), afterSecond.currentVersionId());

    HouseTable secondBase = captureNeutral();
    ViewCommitResult third =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.FALSE, secondBase)
                    .schema(ViewTestFixtures.schemaV2())
                    .representations(
                        Collections.singletonList(
                            ViewTestFixtures.sql(
                                ViewTestFixtures.SQL_V3, ViewTestFixtures.SPARK_DIALECT)))
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

  @Test
  void identicalDefinitionReplaceIsANoOpThatWritesNothing() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    ViewMetadata afterCreate = harness.readMetadata(created.getPointer().getMetadataLocation());
    int filesAfterCreate = harness.metadataFiles().size();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int writesAfterCreate = harness.codecWrites();
    HouseTable pointerAfterCreate = harness.getHouseTableRepository().peek(DB, VIEW).get();

    HouseTable base = captureNeutral();
    int readsAfterCapture = harness.readCalls();
    List<String> before = harness.events();
    int eventBaseline = before.size();

    ViewCommitResult replayed =
        harness.getViewCommitEngine().commit(ViewTestFixtures.replaceIntent(root, base));

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

    // The captured file is read once to compare; nothing else is touched.
    List<String> all = harness.events();
    List<String> noOpEvents = all.subList(eventBaseline, all.size());
    Assertions.assertEquals(
        readsAfterCapture,
        harness.readCalls(),
        "a no-op classifies the supplied snapshot and never reads House Table: " + noOpEvents);
    Assertions.assertEquals(
        1,
        countStartingWith(noOpEvents, RecordingViewMetadataCodec.READ),
        "a no-op reads the captured metadata file exactly once: " + noOpEvents);
    Assertions.assertEquals(
        0,
        countStartingWith(noOpEvents, RecordingViewMetadataCodec.WRITE),
        "a no-op writes no candidate: " + noOpEvents);
    Assertions.assertEquals(
        0,
        countStartingWith(noOpEvents, InMemoryViewHouseTableRepository.SAVE_VIEW),
        "a no-op publishes nothing: " + noOpEvents);

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

  /** Build and comparison must normalize alike, or every namespace-less replace looks changed. */
  @Test
  void aNullDefaultNamespaceRoundTripsAsEmptyAndReplayingItIsANoOp() {
    ViewCommitResult created =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
                    .defaultNamespace(null)
                    .build());
    ViewMetadata createdMetadata = harness.readMetadata(created.getPointer().getMetadataLocation());
    Assertions.assertEquals(
        Namespace.empty(),
        createdMetadata.currentVersion().defaultNamespace(),
        "a null namespace is persisted as the empty namespace");

    HouseTable base = captureNeutral();
    int filesAfterCreate = harness.metadataFiles().size();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int writesAfterCreate = harness.codecWrites();

    ViewCommitResult replayedWithNull =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.FALSE, base)
                    .defaultNamespace(null)
                    .build());
    Assertions.assertFalse(
        replayedWithNull.isMetadataChanged(), "replaying a null namespace is a no-op");

    ViewCommitResult replayedWithEmpty =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.FALSE, base)
                    .defaultNamespace(Namespace.empty())
                    .build());
    Assertions.assertFalse(
        replayedWithEmpty.isMetadataChanged(),
        "the empty namespace means what null meant, so this is the same definition");

    Assertions.assertEquals(filesAfterCreate, harness.metadataFiles().size());
    Assertions.assertEquals(savesAfterCreate, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        writesAfterCreate, harness.codecWrites(), "neither replay may ask the codec to write");
  }

  /** Neither null nor empty properties may be read as an instruction to clear stored ones. */
  @Test
  void nullAndEmptyViewPropertiesAreTheSameSubmissionAndPreserveStoredOnes() {
    Map<String, String> initial = new LinkedHashMap<>();
    initial.put("a", "1");
    initial.put("keep", "yes");
    ViewCommitResult created =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
                    .viewProperties(initial)
                    .build());
    HouseTable base = captureNeutral();
    int filesAfterCreate = harness.metadataFiles().size();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int writesAfterCreate = harness.codecWrites();

    ViewCommitResult replayedWithNull =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.FALSE, base)
                    .viewProperties(null)
                    .build());
    Assertions.assertFalse(
        replayedWithNull.isMetadataChanged(),
        "omitting properties is not a change; it certainly is not a request to delete them");

    ViewCommitResult replayedWithEmpty =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.FALSE, base)
                    .viewProperties(Collections.emptyMap())
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

  @Test
  void movingFromAnEmptyNamespaceToANonEmptyOneCommits() {
    harness
        .getViewCommitEngine()
        .commit(
            ViewTestFixtures.baseIntent(root, Boolean.TRUE, null).defaultNamespace(null).build());
    HouseTable base = captureNeutral();
    int filesAfterCreate = harness.metadataFiles().size();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();

    ViewCommitResult updated =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.FALSE, base)
                    .defaultNamespace(Namespace.of(DB))
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
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureNeutral();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();

    ViewCommitResult updated =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.FALSE, base)
                    .viewProperties(ViewTestFixtures.userProperties("a", "2"))
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

  @Test
  void replacePreservesOmittedUserPropertiesAndMergesSuppliedOnes() {
    Map<String, String> initial = new LinkedHashMap<>();
    initial.put("a", "1");
    initial.put("keep", "yes");
    harness
        .getViewCommitEngine()
        .commit(
            ViewTestFixtures.baseIntent(root, Boolean.TRUE, null).viewProperties(initial).build());
    HouseTable base = captureNeutral();

    ViewCommitResult updated =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.FALSE, base)
                    .viewProperties(ViewTestFixtures.userProperties("a", "2"))
                    .build());

    Map<String, String> properties =
        harness.readMetadata(updated.getPointer().getMetadataLocation()).properties();
    Assertions.assertEquals("2", properties.get("a"));
    Assertions.assertEquals("yes", properties.get("keep"));
  }

  /* One field changes per test, so an under-comparing implementation cannot hide behind another. */

  private static final List<SqlViewRepresentationIntent> BOTH_DIALECTS_V1 =
      ViewTestFixtures.sparkAndTrino(ViewTestFixtures.SQL_V1);

  /** A materially changed replacement built on an already-captured base snapshot. */
  private ViewCommitIntent changedReplaceOf(HouseTable base) {
    return ViewTestFixtures.changedReplaceIntent(root, base).build();
  }

  private ViewCommitResult createWithBothDialects() {
    return harness
        .getViewCommitEngine()
        .commit(
            ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
                .representations(BOTH_DIALECTS_V1)
                .build());
  }

  /** Asserts the NEW value persisted: a file appearing would also pass a stale write. */
  private void assertStructuralChangeIsNotANoOp(
      UnaryOperator<ViewCommitIntent.ViewCommitIntentBuilder> mutation, String changedField) {
    ViewCommitResult created = createWithBothDialects();
    HouseTable base = captureNeutral();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int filesAfterCreate = harness.metadataFiles().size();

    ViewCommitIntent intent =
        mutation
            .apply(
                ViewTestFixtures.baseIntent(root, Boolean.FALSE, base)
                    .representations(BOTH_DIALECTS_V1))
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

  /** Reads back from the written file and a fresh load, so a stale value cannot pass. */
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
        intent.getSchema().identifierFieldIds(),
        persisted.schema().identifierFieldIds(),
        "identifier fields are part of the definition after changing " + changedField);

    LoadedView reloaded = harness.newEngineInstance().loadView(DB, VIEW);
    Assertions.assertEquals(
        submittedByDialect(intent),
        loadedByDialect(reloaded),
        "a fresh load must report the submitted definition after changing " + changedField);
    Assertions.assertEquals(intent.getSourceDialect(), reloaded.getSourceDialect());
    Assertions.assertEquals(intent.getDefaultCatalog(), reloaded.getDefaultCatalog());
    Assertions.assertEquals(
        intent.getDefaultNamespace() == null ? Namespace.empty() : intent.getDefaultNamespace(),
        reloaded.getDefaultNamespace());
    Assertions.assertEquals(
        intent.getSchema().asStruct(),
        reloaded.getSchema().asStruct(),
        "a fresh load must report the submitted schema after changing " + changedField);
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

    harness
        .getViewCommitEngine()
        .commit(
            ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
                .schema(withoutIdentifier)
                .build());
    HouseTable base = captureNeutral();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int filesAfterCreate = harness.metadataFiles().size();

    ViewCommitResult updated =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.FALSE, base)
                    .schema(withIdentifier)
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
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureNeutral();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int filesAfterCreate = harness.metadataFiles().size();

    // The last entry alone equals the stored definition, so a lossy compare sees no change.
    ViewCommitIntent duplicated =
        ViewTestFixtures.baseIntent(root, Boolean.FALSE, base)
            .representations(
                Arrays.asList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT),
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V1, ViewTestFixtures.SPARK_DIALECT)))
            .build();

    Assertions.assertThrows(
        BadRequestException.class, () -> harness.getViewCommitEngine().commit(duplicated));

    Assertions.assertEquals(filesAfterCreate, harness.metadataFiles().size());
    Assertions.assertEquals(savesAfterCreate, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        created.getPointer().getMetadataLocation(),
        harness.getHouseTableRepository().peek(DB, VIEW).get().getTableLocation());
  }

  @Test
  void aDuplicateDialectDifferingOnlyInCaseIsAlsoRejected() {
    Assertions.assertThrows(
        BadRequestException.class,
        () ->
            harness
                .getViewCommitEngine()
                .commit(
                    ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
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

    ViewCommitResult created = pinnedClock.commit(ViewTestFixtures.createIntent(root, null));
    Assertions.assertEquals(fixedNow, created.getLastModifiedTime());

    ViewCommitResult updated = pinnedClock.commit(changedReplaceOf(captureNeutral()));

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

  /** The server stamps {@code replace.drop-dialect.allowed=false}; Iceberg enforces it. */
  @Test
  void replaceDroppingAPreviouslyStoredDialectIsRejected() {
    ViewCommitResult created = createWithBothDialects();
    HouseTable base = captureNeutral();

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
                        ViewTestFixtures.baseIntent(root, Boolean.FALSE, base)
                            .representations(
                                Collections.singletonList(
                                    ViewTestFixtures.sql(
                                        ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
                            .build()));
    // Pinned, so an unrelated engine ISE cannot satisfy this test.
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

  @Test
  void callerCannotOverrideTheDropDialectGuard() {
    Map<String, String> hostile = new HashMap<>();
    hostile.put(ViewProperties.REPLACE_DROP_DIALECT_ALLOWED, "true");

    Assertions.assertThrows(
        BadRequestException.class,
        () ->
            harness
                .getViewCommitEngine()
                .commit(
                    ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
                        .viewProperties(hostile)
                        .build()));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  /**
   * The {@code openhouse.table*} namespace is entity-neutral; type belongs to the row, not
   * metadata.
   */
  @Test
  void createStampsInitialVersionAndReplaceStampsThePriorExactPath() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
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

    // Must survive verbatim: they feed structural equality, so a lossy round trip breaks no-op
    // detection too.
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

    ViewCommitResult replaced =
        harness.getViewCommitEngine().commit(changedReplaceOf(captureNeutral()));
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

  /** A re-read or re-derived token would turn a conditional write into a blind one. */
  @Test
  void publishedPointerRowCarriesTheNewPathAndTheCapturedBaseAsExpectedVersion() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));

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

    HouseTable base = captureNeutral();
    String capturedBase = base.getTableLocation();
    int readsBeforeCommit = harness.readCalls();
    harness.clearEvents();

    ViewCommitResult replaced = harness.getViewCommitEngine().commit(changedReplaceOf(base));

    HouseTable afterReplace = harness.getHouseTableRepository().peek(DB, VIEW).get();
    Assertions.assertEquals(
        replaced.getPointer().getMetadataLocation(), afterReplace.getTableLocation());
    Assertions.assertEquals(
        capturedBase,
        afterReplace.getTableVersion(),
        "a replace must send back exactly the captured tableLocation, not the row's own"
            + " tableVersion ("
            + afterCreate.getTableVersion()
            + ")");

    Assertions.assertEquals(
        readsBeforeCommit,
        harness.readCalls(),
        "a replace works from the captured snapshot and never reads House Table");

    // Exactly read(A) -> write(B) -> publish(expected=A, location=B); no House Table read at all.
    String newLocation = replaced.getPointer().getMetadataLocation();
    Assertions.assertEquals(
        Arrays.asList(
            readEvent(capturedBase), writeEvent(newLocation), saveEvent(capturedBase, newLocation)),
        harness.events());
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

  /* Pure formatters mirroring the recording codec and fake, so a whole single-thread event trace
   * can be asserted for exact equality. */

  private static String readEvent(String location) {
    return RecordingViewMetadataCodec.READ + "(" + location + ")";
  }

  private static String writeEvent(String location) {
    return RecordingViewMetadataCodec.WRITE + "(" + location + ")";
  }

  private static String saveEvent(String expectedVersion, String location) {
    return InMemoryViewHouseTableRepository.SAVE_VIEW
        + "("
        + DB
        + "."
        + VIEW
        + ",expected="
        + expectedVersion
        + ",location="
        + location
        + ")";
  }

  @Test
  void createPublishesInitialVersionAfterWritingItsFile() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));

    // Exactly write(new) -> publish(expected=INITIAL, location=new); a create has no file to read.
    String newLocation = created.getPointer().getMetadataLocation();
    Assertions.assertEquals(
        Arrays.asList(
            writeEvent(newLocation), saveEvent(CatalogConstants.INITIAL_VERSION, newLocation)),
        harness.events());
    Assertions.assertEquals(
        0,
        harness.readCalls(),
        "a create classifies the supplied snapshot and never reads House Table");
  }

  @Test
  void everySuppliedRepresentationAndUserPropertyIsPersisted() {
    Map<String, String> userProperties = new LinkedHashMap<>();
    userProperties.put("owner", "team-a");
    userProperties.put("comment", "a view");

    ViewCommitResult created =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
                    .representations(ViewTestFixtures.sparkAndTrino(ViewTestFixtures.SQL_V1))
                    .viewProperties(userProperties)
                    .build());

    ViewMetadata metadata = harness.readMetadata(created.getPointer().getMetadataLocation());
    Map<String, String> expectedByDialect = new LinkedHashMap<>();
    expectedByDialect.put(ViewTestFixtures.SPARK_DIALECT, ViewTestFixtures.SQL_V1);
    expectedByDialect.put(ViewTestFixtures.TRINO_DIALECT, ViewTestFixtures.SQL_V1);
    Assertions.assertEquals(
        expectedByDialect,
        persistedByDialect(metadata),
        "both dialects must be persisted with their submitted SQL");

    Assertions.assertEquals("team-a", metadata.properties().get("owner"));
    Assertions.assertEquals("a view", metadata.properties().get("comment"));
  }

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

    // The whole mapping, not its size: one dialect returned twice would otherwise pass.
    Map<String, String> submitted = new LinkedHashMap<>();
    ViewTestFixtures.sparkAndTrino(ViewTestFixtures.SQL_V1)
        .forEach(
            representation -> submitted.put(representation.getDialect(), representation.getSql()));
    Assertions.assertEquals(submitted, loadedByDialect(loaded));

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

  /**
   * There is no pre-write compare any more: a changed replace built on a real but superseded base
   * reads that base's file, writes its candidate, then loses the swap. The candidate is retained.
   */
  @Test
  void changedReplaceOnAStaleButReadableBaseFailsAtTheSwapAfterWritingItsCandidate() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable staleBase = captureNeutral();

    // B lands first and moves the pointer off A, entirely outside the measured attempt.
    ViewCommitResult b = harness.getViewCommitEngine().commit(changedReplaceOf(staleBase));
    Assertions.assertTrue(b.isMetadataChanged());

    int filesBeforeStale = harness.metadataFiles().size();
    int savesBeforeStale = harness.getHouseTableRepository().getSaveViewCalls();
    int readsBeforeStale = harness.readCalls();
    harness.clearEvents();

    // A materially changed replace still built on the now-stale A.
    ViewCommitIntent staleButReadable =
        ViewTestFixtures.baseIntent(root, Boolean.FALSE, staleBase)
            .representations(
                Collections.singletonList(
                    ViewTestFixtures.sql(ViewTestFixtures.SQL_V3, ViewTestFixtures.SPARK_DIALECT)))
            .build();

    Assertions.assertThrows(
        CommitFailedException.class, () -> harness.getViewCommitEngine().commit(staleButReadable));

    List<String> events = harness.events();
    int readAt = indexOfStartingWith(events, RecordingViewMetadataCodec.READ);
    int saveAt = indexOfStartingWith(events, InMemoryViewHouseTableRepository.SAVE_VIEW);
    Assertions.assertTrue(readAt >= 0 && saveAt >= 0, "events: " + events);
    Assertions.assertEquals(
        1,
        countStartingWith(events, RecordingViewMetadataCodec.READ),
        "the stale base's own file is read exactly once: " + events);
    Assertions.assertTrue(
        events.get(readAt).contains(staleBase.getTableLocation()),
        "the read is of the captured stale path, not a re-derived current one: " + events);
    Assertions.assertEquals(
        1,
        countStartingWith(events, RecordingViewMetadataCodec.WRITE),
        "a candidate is built before the losing swap: " + events);
    Assertions.assertEquals(
        readsBeforeStale, harness.readCalls(), "a losing replace still reads no House Table row");
    Assertions.assertEquals(
        filesBeforeStale + 1,
        harness.metadataFiles().size(),
        "the losing candidate is written and left in place, never cleaned up");
    Assertions.assertEquals(
        savesBeforeStale + 1,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "exactly one swap is attempted");
    Assertions.assertTrue(
        events.get(saveAt).contains("expected=" + staleBase.getTableLocation()),
        "the swap carries A's exact captured path, never B's: " + events.get(saveAt));
    Assertions.assertEquals(
        b.getPointer().getMetadataLocation(),
        harness.getHouseTableRepository().peek(DB, VIEW).get().getTableLocation(),
        "the stale attempt must never promote A over the current B pointer");
  }

  @Test
  void replaceOfAnAbsentViewNeverBecomesACreate() {
    Assertions.assertThrows(
        NoSuchViewException.class,
        () -> harness.getViewCommitEngine().commit(ViewTestFixtures.replaceIntent(root, null)));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        0, harness.readCalls(), "a completed lookup that found absence needs no engine read");
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  /** A key occupied by a table is a non-view snapshot, so a replace of it is NoSuchView. */
  @Test
  void replaceOfATablePointerIsRejectedAsNoSuchView() {
    harness
        .getHouseTableRepository()
        .seed(ViewTestFixtures.tableRow("/existing/00001-a.metadata.json"));
    HouseTable occupant = captureNeutral();
    int readsBeforeCommit = harness.readCalls();

    Assertions.assertThrows(
        NoSuchViewException.class,
        () -> harness.getViewCommitEngine().commit(ViewTestFixtures.replaceIntent(root, occupant)));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        readsBeforeCommit,
        harness.readCalls(),
        "a supplied non-view is rejected in memory, before any FileIO or read");
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  // ---- Missing create flag ----

  /**
   * A missing create flag — explicitly null or genuinely omitted — is rejected before any effect.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("missingCreateFlagCases")
  void aMissingCreateFlagIsRejectedBeforeAnyEffect(
      String caseName, boolean explicitNull, boolean withSnapshot) {
    HouseTable snapshot = null;
    if (withSnapshot) {
      harness
          .getHouseTableRepository()
          .seed(ViewTestFixtures.viewRow("/existing/00001-a.metadata.json"));
      snapshot = captureNeutral();
    }
    int readsBeforeCommit = harness.readCalls();

    // Explicit-null invokes the isCreate setter with null; omitted never invokes it at all.
    ViewCommitIntent.ViewCommitIntentBuilder builder =
        explicitNull
            ? ViewTestFixtures.baseIntent(root, null, snapshot)
            : ViewTestFixtures.intentBuilderWithoutCreateFlag(root).baseRow(snapshot);
    ViewCommitIntent intent = builder.build();
    Assertions.assertNull(
        intent.getIsCreate(), "the builder applies no default create flag: " + caseName);
    Assertions.assertSame(snapshot, intent.getBaseRow());

    BadRequestException thrown =
        Assertions.assertThrows(
            BadRequestException.class, () -> harness.getViewCommitEngine().commit(intent));

    Assertions.assertTrue(
        thrown.getMessage() != null && thrown.getMessage().contains("isCreate"),
        "the failure must name the missing create flag: " + thrown.getMessage());
    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        readsBeforeCommit, harness.readCalls(), "a missing create flag reads no House Table row");
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
    verify(harness.getCodec(), never()).write(any(ViewMetadata.class), any(OutputFile.class));
    verify(harness.getFileIOManager(), never()).getFileIO(any(StorageType.Type.class));
  }

  private static Stream<Arguments> missingCreateFlagCases() {
    return Stream.of(
        Arguments.of("explicit null, absent snapshot", true, false),
        Arguments.of("explicit null, present snapshot", true, true),
        Arguments.of("omitted, absent snapshot", false, false),
        Arguments.of("omitted, present snapshot", false, true));
  }

  /** the missing-create-flag guard runs before the reserved-property guard, which also throws. */
  @Test
  void missingCreateFlagIsReportedAheadOfOtherMalformedFields() {
    Map<String, String> reserved = new LinkedHashMap<>();
    reserved.put(CatalogConstants.OPENHOUSE_UUID_KEY, "00000000-0000-0000-0000-000000000000");
    ViewCommitIntent noModeAndReserved =
        ViewTestFixtures.baseIntent(root, null, null).viewProperties(reserved).build();

    BadRequestException thrown =
        Assertions.assertThrows(
            BadRequestException.class,
            () -> harness.getViewCommitEngine().commit(noModeAndReserved));

    Assertions.assertTrue(
        thrown.getMessage() != null && thrown.getMessage().contains("isCreate"),
        "a missing create flag must be reported before the reserved-property failure: "
            + thrown.getMessage());
    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
  }

  /** A missing prepared CREATE input outranks an occupied name. */
  @Test
  void aMissingPreparedCreateInputPrecedesOccupancyClassification() {
    harness
        .getHouseTableRepository()
        .seed(ViewTestFixtures.viewRow("/existing/00001-a.metadata.json"));
    HouseTable occupant = captureNeutral();
    int readsBeforeCommit = harness.readCalls();

    BadRequestException thrown =
        Assertions.assertThrows(
            BadRequestException.class,
            () ->
                harness
                    .getViewCommitEngine()
                    .commit(
                        ViewTestFixtures.baseIntent(root, Boolean.TRUE, occupant)
                            .viewUuid(null)
                            .build()));

    Assertions.assertTrue(
        thrown.getMessage() != null && thrown.getMessage().contains("viewUuid"),
        "a missing prepared input outranks an occupied name: " + thrown.getMessage());
    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(readsBeforeCommit, harness.readCalls());
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  /** Prepared-input validation order is viewUuid, then viewLocation, then storageType. */
  @Test
  void aCreatePreparedInputOrderingIsUuidThenLocationThenStorage() {
    BadRequestException allBad =
        Assertions.assertThrows(
            BadRequestException.class,
            () ->
                harness
                    .getViewCommitEngine()
                    .commit(
                        ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
                            .viewUuid(null)
                            .viewLocation(null)
                            .storageType(null)
                            .build()));
    Assertions.assertTrue(
        allBad.getMessage() != null && allBad.getMessage().contains("viewUuid"),
        "viewUuid is validated first: " + allBad.getMessage());
    Assertions.assertFalse(
        allBad.getMessage().contains("viewLocation"),
        "only the first bad input is reported: " + allBad.getMessage());

    BadRequestException locationBeforeStorage =
        Assertions.assertThrows(
            BadRequestException.class,
            () ->
                harness
                    .getViewCommitEngine()
                    .commit(
                        ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
                            .viewLocation(null)
                            .storageType(null)
                            .build()));
    Assertions.assertTrue(
        locationBeforeStorage.getMessage() != null
            && locationBeforeStorage.getMessage().contains("viewLocation"),
        "viewLocation is validated before storageType: " + locationBeforeStorage.getMessage());
    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  // ---- REPLACE of a non-view snapshot: NoSuchView before FileIO ----

  @Test
  void replaceOfALegacyCapturedRowIsNoSuchViewBeforeFileIo() {
    harness
        .getHouseTableRepository()
        .seed(ViewTestFixtures.legacyRow("/existing/00001-a.metadata.json"));
    assertReplaceOfNonViewIsNoSuchView(captureNeutral());
  }

  @Test
  void replaceOfAnUnknownEntityTypeCapturedRowIsNoSuchView() {
    harness
        .getHouseTableRepository()
        .seed(
            ViewTestFixtures.row(
                ViewTestFixtures.ENTITY_TYPE_UNKNOWN, "/existing/00001-a.metadata.json"));
    assertReplaceOfNonViewIsNoSuchView(captureNeutral());
  }

  @Test
  void replaceOfANonCanonicalDiscriminatorCapturedRowIsNoSuchView() {
    harness
        .getHouseTableRepository()
        .seed(ViewTestFixtures.row("view", "/existing/00001-a.metadata.json"));
    assertReplaceOfNonViewIsNoSuchView(captureNeutral());
  }

  /** A non-view is rejected before its pointer or storage is even looked at. */
  @Test
  void replaceOfANonViewIgnoresItsMissingPointerAndStorageAndIsStillNoSuchView() {
    HouseTable tableWithNoPointer =
        ViewTestFixtures.tableRow("/existing/00001-a.metadata.json")
            .toBuilder()
            .tableLocation(null)
            .storageType(null)
            .build();
    assertReplaceOfNonViewIsNoSuchView(tableWithNoPointer);
  }

  private void assertReplaceOfNonViewIsNoSuchView(HouseTable occupant) {
    int readsBeforeCommit = harness.readCalls();

    Assertions.assertThrows(
        NoSuchViewException.class,
        () -> harness.getViewCommitEngine().commit(ViewTestFixtures.replaceIntent(root, occupant)));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        readsBeforeCommit,
        harness.readCalls(),
        "a non-view snapshot is rejected in memory, before any read");
    verify(harness.getFileIOManager(), never()).getFileIO(any(StorageType.Type.class));
    verify(harness.getCodec(), never()).read(any(InputFile.class));
    verify(harness.getCodec(), never()).write(any(ViewMetadata.class), any(OutputFile.class));
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  // ---- Row-target contract ----

  /** A captured row that does not name the requested view (wrong or blank key) is a bad call. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("malformedRowKeys")
  void aCapturedRowWithAMismatchedKeyIsAMalformedInvocation(
      String caseName, String dbId, String viewId) {
    HouseTable mismatched =
        ViewTestFixtures.viewRow("/existing/00001-a.metadata.json")
            .toBuilder()
            .databaseId(dbId)
            .tableId(viewId)
            .build();
    assertRowKeyMismatchIsRejected(mismatched);
  }

  private static Stream<Arguments> malformedRowKeys() {
    return Stream.of(
        Arguments.of("another database", "other_db", VIEW),
        Arguments.of("another view", DB, "other_view"),
        Arguments.of("blank database", "", VIEW),
        Arguments.of("null database", null, VIEW),
        Arguments.of("null view", DB, null),
        Arguments.of("blank view", DB, ""),
        Arguments.of("whitespace view", DB, "   "));
  }

  private void assertRowKeyMismatchIsRejected(HouseTable mismatchedRow) {
    int readsBeforeCommit = harness.readCalls();

    Assertions.assertThrows(
        BadRequestException.class,
        () ->
            harness
                .getViewCommitEngine()
                .commit(ViewTestFixtures.replaceIntent(root, mismatchedRow)));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(readsBeforeCommit, harness.readCalls());
    verify(harness.getFileIOManager(), never()).getFileIO(any(StorageType.Type.class));
    verify(harness.getCodec(), never()).read(any(InputFile.class));
    verify(harness.getCodec(), never()).write(any(ViewMetadata.class), any(OutputFile.class));
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  @Test
  void aCreateOccupantNamingAnotherKeyIsAMalformedInvocation() {
    HouseTable wrongKeyOccupant =
        ViewTestFixtures.viewRow("/existing/00001-a.metadata.json")
            .toBuilder()
            .tableId("other_view")
            .build();

    Assertions.assertThrows(
        BadRequestException.class,
        () ->
            harness
                .getViewCommitEngine()
                .commit(ViewTestFixtures.createIntent(root, wrongKeyOccupant)));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  /**
   * Case-only differences identify the same view, without the fake pretending case-insensitivity.
   */
  @Test
  void aCaseOnlyDifferentOccupantKeyIsAcceptedOnACreateCollision() {
    HouseTable caseShifted =
        ViewTestFixtures.viewRow("/existing/00001-a.metadata.json")
            .toBuilder()
            .databaseId("VIEWDB")
            .tableId("V1")
            .build();
    int readsBeforeCommit = harness.readCalls();

    Assertions.assertThrows(
        AlreadyExistsException.class,
        () ->
            harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, caseShifted)));

    Assertions.assertEquals(0, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(readsBeforeCommit, harness.readCalls());
    Assertions.assertTrue(harness.metadataFiles().isEmpty());
  }

  @Test
  void aCaseOnlyDifferentSnapshotKeyIsAcceptedOnAReplaceNoOp() {
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureNeutral();
    HouseTable caseShifted = base.toBuilder().databaseId("VIEWDB").tableId("V1").build();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int readsAfterCapture = harness.readCalls();

    ViewCommitResult replayed =
        harness.getViewCommitEngine().commit(ViewTestFixtures.replaceIntent(root, caseShifted));

    Assertions.assertFalse(
        replayed.isMetadataChanged(), "a case-only key still identifies the same view");
    Assertions.assertFalse(replayed.isCreated());
    Assertions.assertEquals(
        savesAfterCreate,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "a no-op publishes nothing, and never under a differently spelled key");
    Assertions.assertEquals(readsAfterCapture, harness.readCalls());
    Assertions.assertTrue(
        harness.getHouseTableRepository().peek(DB, VIEW).isPresent(),
        "the real lower-cased row is untouched");
    Assertions.assertFalse(
        harness.getHouseTableRepository().peek("VIEWDB", "V1").isPresent(),
        "no row was published under the upper-cased key");
  }

  // ---- Malformed canonical-view snapshot ----

  @Test
  void aCanonicalViewSnapshotWithANullPointerIsMalformedState() {
    assertMalformedFieldIsRejected(builder -> builder.tableLocation(null), "tableLocation");
  }

  @Test
  void aCanonicalViewSnapshotWithAnEmptyPointerIsMalformedState() {
    assertMalformedFieldIsRejected(builder -> builder.tableLocation(""), "tableLocation");
  }

  @Test
  void aCanonicalViewSnapshotWithAWhitespacePointerIsMalformedState() {
    assertMalformedFieldIsRejected(builder -> builder.tableLocation("   "), "tableLocation");
  }

  @Test
  void aCanonicalViewSnapshotWithANullStorageIsMalformedState() {
    assertMalformedFieldIsRejected(builder -> builder.storageType(null), "storageType");
  }

  @Test
  void aCanonicalViewSnapshotWithAnEmptyStorageIsMalformedState() {
    assertMalformedFieldIsRejected(builder -> builder.storageType(""), "storageType");
  }

  @Test
  void aCanonicalViewSnapshotWithAWhitespaceStorageIsMalformedState() {
    assertMalformedFieldIsRejected(builder -> builder.storageType("   "), "storageType");
  }

  /**
   * A canonical VIEW whose required pointer or storage field is null/blank is corrupt server state:
   * an IllegalStateException naming the field, before any FileIO, codec, publish, or House Table
   * read. The setup CREATE's own FileIO call is cleared first so only the attempt is measured.
   */
  private void assertMalformedFieldIsRejected(
      UnaryOperator<HouseTable.HouseTableBuilder> mutation, String field) {
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureNeutral();
    HouseTable malformed = mutation.apply(base.toBuilder()).build();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int readsAfterCapture = harness.readCalls();
    Mockito.clearInvocations(harness.getFileIOManager());
    harness.clearEvents();

    IllegalStateException thrown =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> harness.getViewCommitEngine().commit(changedReplaceOf(malformed)));

    Assertions.assertTrue(
        thrown.getMessage() != null && thrown.getMessage().contains(field),
        "the failure must name the malformed field " + field + ": " + thrown.getMessage());
    Assertions.assertEquals(
        savesAfterCreate,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "a malformed snapshot publishes nothing");
    Assertions.assertEquals(
        readsAfterCapture, harness.readCalls(), "a malformed snapshot reads no House Table row");
    verify(harness.getFileIOManager(), never()).getFileIO(any(StorageType.Type.class));
    Assertions.assertEquals(
        Collections.emptyList(),
        harness.events(),
        "a malformed snapshot is rejected before any codec read, candidate write, or PUT");
  }

  @Test
  void aCanonicalViewSnapshotWithUnknownStorageKeepsItsRealError() {
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureNeutral();
    HouseTable badStorage = base.toBuilder().storageType("no-such-storage").build();
    int savesAfterCreate = harness.getHouseTableRepository().getSaveViewCalls();
    int readsAfterCapture = harness.readCalls();
    Mockito.clearInvocations(harness.getFileIOManager());
    harness.clearEvents();

    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> harness.getViewCommitEngine().commit(changedReplaceOf(badStorage)));

    Assertions.assertTrue(
        thrown.getMessage() != null && thrown.getMessage().contains("no-such-storage"),
        "an unknown storage type keeps its own error rather than a fallback: "
            + thrown.getMessage());
    // A nonblank-but-unknown storage is rejected by fromString before FileIO/codec/publish.
    Assertions.assertEquals(savesAfterCreate, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(readsAfterCapture, harness.readCalls());
    verify(harness.getFileIOManager(), never()).getFileIO(any(StorageType.Type.class));
    Assertions.assertEquals(
        Collections.emptyList(),
        harness.events(),
        "an unknown storage type is rejected before any codec read, candidate write, or PUT");
  }

  // ---- Exact compare-and-swap token ----

  @Test
  void aChangedReplaceUsesTheCapturedTableLocationNotTheRowTableVersionAsTheSwapToken() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureNeutral();
    Assertions.assertNotEquals(
        base.getTableVersion(),
        base.getTableLocation(),
        "the captured tableVersion must differ from its tableLocation, or this proves nothing");
    HouseTable tokenTrap = base.toBuilder().tableVersion("NOT_A_PATH_TOKEN").build();
    harness.clearEvents();

    ViewCommitResult replaced = harness.getViewCommitEngine().commit(changedReplaceOf(tokenTrap));

    HouseTable afterReplace = harness.getHouseTableRepository().peek(DB, VIEW).get();
    Assertions.assertEquals(
        base.getTableLocation(),
        afterReplace.getTableVersion(),
        "the published expected-version token is the captured tableLocation");
    Assertions.assertNotEquals("NOT_A_PATH_TOKEN", afterReplace.getTableVersion());

    ViewMetadata replacedMetadata =
        harness.readMetadata(replaced.getPointer().getMetadataLocation());
    Assertions.assertEquals(
        base.getTableLocation(),
        replacedMetadata.properties().get(getCanonicalFieldName("tableVersion")),
        "the written metadata stamps the captured tableLocation, not the row's tableVersion");

    List<String> events = harness.events();
    int readAt = indexOfStartingWith(events, RecordingViewMetadataCodec.READ);
    int saveAt = indexOfStartingWith(events, InMemoryViewHouseTableRepository.SAVE_VIEW);
    Assertions.assertTrue(readAt >= 0 && saveAt >= 0, "events: " + events);
    Assertions.assertTrue(
        events.get(readAt).contains(base.getTableLocation()),
        "the captured file is read at its tableLocation, not the tableVersion token: " + events);
    Assertions.assertTrue(
        events.get(saveAt).contains("expected=" + base.getTableLocation()),
        "the swap token is the captured tableLocation: " + events.get(saveAt));
  }

  // ---- Captured file: no fallback ----

  @Test
  void aReplaceWhoseCapturedFileIsMissingPropagatesTheReadFailureWithNoFallback() {
    // A healthy view exists, so a fallback to the live pointer would be observable.
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable healthy = harness.getHouseTableRepository().peek(DB, VIEW).get();
    HouseTable ghost =
        ViewTestFixtures.viewRow(
            root.resolve("ghost").resolve("00001-missing.metadata.json").toString());
    int readsBeforeCommit = harness.readCalls();
    int savesBeforeCommit = harness.getHouseTableRepository().getSaveViewCalls();
    int filesBeforeCommit = harness.metadataFiles().size();
    harness.clearEvents();

    Assertions.assertThrows(
        NotFoundException.class,
        () -> harness.getViewCommitEngine().commit(changedReplaceOf(ghost)));

    Assertions.assertEquals(
        savesBeforeCommit, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        readsBeforeCommit,
        harness.readCalls(),
        "the engine must not fall back to a House Table read when the captured file is unreadable");
    Assertions.assertEquals(
        filesBeforeCommit, harness.metadataFiles().size(), "no candidate is written");
    assertCapturedReadFailedWithoutFallback(ghost.getTableLocation());
    Assertions.assertEquals(
        healthy,
        harness.getHouseTableRepository().peek(DB, VIEW).get(),
        "the healthy live pointer must be left untouched");
  }

  @Test
  void aReplaceWhoseCapturedFileIsCorruptPropagatesTheParseFailureWithNoFallback()
      throws IOException {
    harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable healthy = harness.getHouseTableRepository().peek(DB, VIEW).get();
    Path corruptPath = root.resolve("corrupt").resolve("00001-corrupt.metadata.json");
    Files.createDirectories(corruptPath.getParent());
    Files.write(corruptPath, "{ this is not valid view metadata json".getBytes());
    HouseTable corruptRow = ViewTestFixtures.viewRow(corruptPath.toString());
    int readsBeforeCommit = harness.readCalls();
    int savesBeforeCommit = harness.getHouseTableRepository().getSaveViewCalls();
    harness.clearEvents();

    // Iceberg's ViewMetadataParser wraps a read/parse IOException as UncheckedIOException; invalid
    // JSON surfaces a Jackson JsonProcessingException cause. Pinning both excludes a masked
    // CommitStateUnknownException, NPE, or any other reclassification.
    UncheckedIOException thrown =
        Assertions.assertThrows(
            UncheckedIOException.class,
            () -> harness.getViewCommitEngine().commit(changedReplaceOf(corruptRow)));
    Assertions.assertTrue(
        thrown.getMessage() != null && thrown.getMessage().contains("Failed to read json file"),
        "the failure is the real json read error: " + thrown.getMessage());
    Assertions.assertTrue(
        thrown.getCause() instanceof JsonProcessingException,
        "the direct cause is the Jackson parse failure, not a reclassification: "
            + thrown.getCause());

    Assertions.assertEquals(
        savesBeforeCommit, harness.getHouseTableRepository().getSaveViewCalls());
    Assertions.assertEquals(
        readsBeforeCommit,
        harness.readCalls(),
        "a corrupt captured file must not trigger a House Table fallback read");
    assertCapturedReadFailedWithoutFallback(corruptPath.toString());
    Assertions.assertEquals(
        healthy,
        harness.getHouseTableRepository().peek(DB, VIEW).get(),
        "the healthy live pointer must be left untouched");
  }

  /**
   * The recording codec logs the read before it delegates, so a failed read still emits its event.
   */
  private void assertCapturedReadFailedWithoutFallback(String capturedPath) {
    // Exactly read(capturedPath): the read is attempted once and there is no second read,
    // no candidate write, and no PUT after it fails.
    Assertions.assertEquals(
        Collections.singletonList(readEvent(capturedPath)),
        harness.events(),
        "the captured file is read once and nothing else happens after the read fails");
  }

  /** A valid REPLACE with every CREATE-only input null keeps the captured identity and creator. */
  @Test
  void aReplaceIgnoresNullCreateOnlyInputsAndPreservesTheStoredCreator() {
    ViewCommitResult created =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    ViewMetadata createdMetadata = harness.readMetadata(created.getPointer().getMetadataLocation());
    Assertions.assertEquals(
        ViewTestFixtures.CREATOR,
        createdMetadata.properties().get(getCanonicalFieldName("tableCreator")),
        "the fixture creator seeds the stored creator, or preservation is not observable");
    HouseTable base = captureNeutral();

    ViewCommitResult replaced =
        harness
            .getViewCommitEngine()
            .commit(
                ViewTestFixtures.changedReplaceIntent(root, base)
                    .viewUuid(null)
                    .viewLocation(null)
                    .storageType(null)
                    .creator("someone_else")
                    .build());
    ViewMetadata replacedMetadata =
        harness.readMetadata(replaced.getPointer().getMetadataLocation());

    Assertions.assertTrue(replaced.isMetadataChanged());
    Assertions.assertEquals(
        created.getViewUuid(),
        replaced.getViewUuid(),
        "identity comes from the captured metadata, never the null incoming uuid");
    Assertions.assertEquals(createdMetadata.location(), replacedMetadata.location());
    Assertions.assertTrue(
        replaced.getPointer().getMetadataLocation().startsWith(createdMetadata.location()),
        "the file stays under the captured root, not a null incoming location");
    Assertions.assertEquals(
        ViewTestFixtures.LOCAL_STORAGE_TYPE,
        replaced.getPointer().getStorageType(),
        "storage comes from the captured row, not the null incoming value");
    Assertions.assertEquals(
        ViewTestFixtures.CREATOR,
        replacedMetadata.properties().get(getCanonicalFieldName("tableCreator")),
        "the original creator is preserved, not the differing incoming creator");
  }

  // ---- Snapshot no-op against moved / deleted / recreated state ----

  @Test
  void aNoOpAgainstAChangedSnapshotReturnsTheCapturedPointerAndLeavesTheStoreUntouched() {
    ViewCommitResult a =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureNeutral();

    harness.getViewCommitEngine().commit(changedReplaceOf(base));
    HouseTable pointerAtB = harness.getHouseTableRepository().peek(DB, VIEW).get();
    int savesAtB = harness.getHouseTableRepository().getSaveViewCalls();
    int readsAtB = harness.readCalls();
    harness.clearEvents();

    ViewCommitResult replayed =
        harness.getViewCommitEngine().commit(ViewTestFixtures.replaceIntent(root, base));

    Assertions.assertFalse(replayed.isMetadataChanged());
    Assertions.assertFalse(replayed.isCreated());
    Assertions.assertEquals(
        a.getPointer().getMetadataLocation(),
        replayed.getPointer().getMetadataLocation(),
        "a snapshot no-op returns the captured pointer, even though the store moved on");
    Assertions.assertEquals(a.getViewUuid(), replayed.getViewUuid());
    Assertions.assertEquals(a.getLastModifiedTime(), replayed.getLastModifiedTime());
    Assertions.assertEquals(
        savesAtB,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "a no-op publishes nothing");
    Assertions.assertEquals(readsAtB, harness.readCalls(), "a no-op reads no House Table row");
    assertNoOpReadCapturedFileOnce(base);
    Assertions.assertEquals(
        pointerAtB,
        harness.getHouseTableRepository().peek(DB, VIEW).get(),
        "the current pointer must be left exactly as it was");
  }

  @Test
  void aNoOpAgainstADeletedSnapshotDoesNotResurrectIt() {
    ViewCommitResult a =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureNeutral();
    Assertions.assertTrue(harness.getViewCommitEngine().dropView(DB, VIEW));
    int savesAfterDrop = harness.getHouseTableRepository().getSaveViewCalls();
    int readsAfterDrop = harness.readCalls();
    harness.clearEvents();

    ViewCommitResult replayed =
        harness.getViewCommitEngine().commit(ViewTestFixtures.replaceIntent(root, base));

    Assertions.assertFalse(replayed.isMetadataChanged());
    Assertions.assertFalse(replayed.isCreated());
    Assertions.assertEquals(
        a.getPointer().getMetadataLocation(), replayed.getPointer().getMetadataLocation());
    Assertions.assertEquals(a.getViewUuid(), replayed.getViewUuid());
    Assertions.assertEquals(a.getLastModifiedTime(), replayed.getLastModifiedTime());
    Assertions.assertEquals(
        savesAfterDrop,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "a no-op must not re-publish a deleted view");
    Assertions.assertEquals(readsAfterDrop, harness.readCalls());
    assertNoOpReadCapturedFileOnce(base);
    Assertions.assertFalse(
        harness.getHouseTableRepository().peek(DB, VIEW).isPresent(),
        "the deleted row stays deleted; a snapshot no-op cannot resurrect it");
  }

  @Test
  void aNoOpAgainstARecreatedSnapshotDoesNotRepublishTheCapturedOne() {
    ViewCommitResult a =
        harness.getViewCommitEngine().commit(ViewTestFixtures.createIntent(root, null));
    HouseTable base = captureNeutral();
    Assertions.assertTrue(harness.getViewCommitEngine().dropView(DB, VIEW));

    // A distinguishable C: its own identity, root, and definition, so "returned A" is not vacuous.
    harness
        .getViewCommitEngine()
        .commit(
            ViewTestFixtures.baseIntent(root, Boolean.TRUE, null)
                .viewUuid(ViewTestFixtures.SECOND_VIEW_UUID)
                .viewLocation(
                    ViewTestFixtures.allocatedViewLocation(
                        root, DB, VIEW, ViewTestFixtures.SECOND_VIEW_UUID))
                .schema(ViewTestFixtures.schemaV2())
                .representations(
                    Collections.singletonList(
                        ViewTestFixtures.sql(
                            ViewTestFixtures.SQL_V2, ViewTestFixtures.SPARK_DIALECT)))
                .build());
    HouseTable pointerAtC = harness.getHouseTableRepository().peek(DB, VIEW).get();
    int savesAtC = harness.getHouseTableRepository().getSaveViewCalls();
    int readsAtC = harness.readCalls();
    harness.clearEvents();

    ViewCommitResult replayed =
        harness.getViewCommitEngine().commit(ViewTestFixtures.replaceIntent(root, base));

    Assertions.assertFalse(replayed.isMetadataChanged());
    Assertions.assertFalse(replayed.isCreated());
    Assertions.assertEquals(
        a.getPointer().getMetadataLocation(),
        replayed.getPointer().getMetadataLocation(),
        "the no-op still reports the captured A pointer, not the recreated C one");
    Assertions.assertEquals(a.getViewUuid(), replayed.getViewUuid());
    Assertions.assertNotEquals(
        ViewTestFixtures.SECOND_VIEW_UUID,
        replayed.getViewUuid(),
        "the no-op must not report the recreated C identity");
    Assertions.assertTrue(
        pointerAtC
            .getTableLocation()
            .startsWith(
                ViewTestFixtures.viewLocation(root, DB, VIEW, ViewTestFixtures.SECOND_VIEW_UUID)),
        "C lives under its own distinct root: " + pointerAtC.getTableLocation());
    Assertions.assertEquals(a.getLastModifiedTime(), replayed.getLastModifiedTime());
    Assertions.assertEquals(
        savesAtC,
        harness.getHouseTableRepository().getSaveViewCalls(),
        "a no-op cannot republish the captured snapshot over the live recreated row");
    Assertions.assertEquals(readsAtC, harness.readCalls());
    assertNoOpReadCapturedFileOnce(base);
    Assertions.assertEquals(
        pointerAtC,
        harness.getHouseTableRepository().peek(DB, VIEW).get(),
        "the recreated row is left exactly as it was");
  }

  /** A snapshot no-op reads exactly the captured file once and writes/publishes nothing. */
  private void assertNoOpReadCapturedFileOnce(HouseTable capturedBase) {
    // Exactly read(capturedBase): one read of the captured file, then no candidate write and no
    // PUT.
    Assertions.assertEquals(
        Collections.singletonList(readEvent(capturedBase.getTableLocation())),
        harness.events(),
        "a no-op reads the captured metadata file exactly once and neither writes nor publishes");
  }
}
