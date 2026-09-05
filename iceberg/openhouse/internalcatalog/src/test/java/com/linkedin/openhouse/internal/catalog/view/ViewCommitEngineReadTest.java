package com.linkedin.openhouse.internal.catalog.view;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;
import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.DB;
import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.LOCAL_STORAGE_TYPE;
import static com.linkedin.openhouse.internal.catalog.view.ViewTestFixtures.VIEW;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.view.model.LoadedView;
import com.linkedin.openhouse.internal.catalog.view.model.ViewPointer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.view.ViewMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

/**
 * Read-side behaviour: load probes, storage resolution, listing, dropping, unsupported rename.
 * Every collaborator that could touch storage is a mock, so "this cost nothing" is asserted, not
 * inferred.
 *
 * <p>The House Table mock keeps the adapter's contract: {@code findViewById} answers with a view or
 * with nothing. A non-view occupant is filtered out by the server's typed predicate long before the
 * engine, which is why the engine has no discriminator check of its own to test here.
 */
public class ViewCommitEngineReadTest {

  private static final String VIEW_BASE = "/tmp/openhouse/viewdb/v1-1111";
  private static final String METADATA_PATH = VIEW_BASE + "/00001-abcd.metadata.json";

  private HouseTableRepository houseTableRepository;
  private FileIOManager fileIOManager;
  private ViewMetadataCodec viewMetadataCodec;
  private StorageType storageType;
  private ViewCommitEngine viewCommitEngine;

  @BeforeEach
  void setUp() {
    houseTableRepository = mock(HouseTableRepository.class);
    fileIOManager = mock(FileIOManager.class);
    viewMetadataCodec = mock(ViewMetadataCodec.class);
    storageType = mock(StorageType.class);
    viewCommitEngine =
        new ViewCommitEngineImpl(
            houseTableRepository, fileIOManager, viewMetadataCodec, storageType);
  }

  /** The common case, so it must not cost a FileIO or a parse. */
  @Test
  void loadViewOnAbsentPointerCostsOneTypedLookupAndNothingElse() {
    when(houseTableRepository.findViewById(any(HouseTablePrimaryKey.class)))
        .thenReturn(Optional.empty());

    Assertions.assertThrows(NoSuchViewException.class, () -> viewCommitEngine.loadView(DB, VIEW));

    verify(houseTableRepository, times(1))
        .findViewById(HouseTablePrimaryKey.builder().databaseId(DB).tableId(VIEW).build());
    verify(houseTableRepository, never()).findById(any(HouseTablePrimaryKey.class));
    verifyNoInteractions(fileIOManager);
    verifyNoInteractions(viewMetadataCodec);
  }

  /**
   * A table at a view key is absent through the typed route, so it is the same answer as a missing
   * key. Reporting anything else would tell a later create the name was free.
   */
  @Test
  void loadViewOfAKeyHeldByANonViewIsTheSameAnswerAsAbsent() {
    when(houseTableRepository.findViewById(any(HouseTablePrimaryKey.class)))
        .thenReturn(Optional.empty());

    Assertions.assertThrows(NoSuchViewException.class, () -> viewCommitEngine.loadView(DB, VIEW));

    verify(houseTableRepository, times(1)).findViewById(any(HouseTablePrimaryKey.class));
    verifyNoInteractions(fileIOManager);
    verifyNoInteractions(viewMetadataCodec);
  }

  /** FileIO comes from the row's own storage, never from a cluster-wide selection. */
  @Test
  void loadViewSelectsFileIoFromPointerRowStorageAndParsesExactlyThatPath() {
    HouseTable row = ViewTestFixtures.viewRow(METADATA_PATH);
    when(houseTableRepository.findViewById(any(HouseTablePrimaryKey.class)))
        .thenReturn(Optional.of(row));
    when(storageType.fromString(LOCAL_STORAGE_TYPE)).thenReturn(StorageType.LOCAL);
    FileIO fileIO = mock(FileIO.class);
    when(fileIOManager.getFileIO(StorageType.LOCAL)).thenReturn(fileIO);
    InputFile inputFile = mock(InputFile.class);
    when(fileIO.newInputFile(METADATA_PATH)).thenReturn(inputFile);
    Map<String, String> persistedProperties = new LinkedHashMap<>();
    persistedProperties.put("user-key", "user-value");
    persistedProperties.put(getCanonicalFieldName("lastModifiedTime"), "1700000000000");
    ViewMetadata metadata =
        ViewMetadataTestUtil.metadata(VIEW_BASE, ViewTestFixtures.schemaV1(), persistedProperties);
    when(viewMetadataCodec.read(inputFile)).thenReturn(metadata);

    LoadedView loaded = viewCommitEngine.loadView(DB, VIEW);

    verify(storageType, times(1)).fromString(LOCAL_STORAGE_TYPE);
    verify(fileIOManager, times(1)).getFileIO(StorageType.LOCAL);
    verify(fileIO, times(1)).newInputFile(METADATA_PATH);
    verify(viewMetadataCodec, times(1)).read(inputFile);

    Assertions.assertEquals(METADATA_PATH, loaded.getPointer().getMetadataLocation());
    Assertions.assertEquals(LOCAL_STORAGE_TYPE, loaded.getPointer().getStorageType());
    Assertions.assertEquals(DB, loaded.getPointer().getDatabaseId());
    Assertions.assertEquals(VIEW, loaded.getPointer().getViewId());

    // The whole conversion, not a sample of it: these fields are what the later layer answers reads
    // with, and they are also what structural equality is computed over on a replace.
    Assertions.assertEquals(metadata.uuid(), loaded.getViewUuid());
    Assertions.assertEquals(metadata.currentVersionId(), loaded.getCurrentVersionId());
    Assertions.assertEquals(metadata.schema().asStruct(), loaded.getSchema().asStruct());
    Assertions.assertEquals(metadata.currentVersion().defaultCatalog(), loaded.getDefaultCatalog());
    Assertions.assertEquals(
        metadata.currentVersion().defaultNamespace(), loaded.getDefaultNamespace());
    Assertions.assertEquals(1, loaded.getRepresentations().size());
    Assertions.assertEquals(
        ViewTestFixtures.SPARK_DIALECT, loaded.getRepresentations().get(0).getDialect());
    Assertions.assertEquals(ViewTestFixtures.SQL_V1, loaded.getRepresentations().get(0).getSql());
    Assertions.assertEquals(ViewTestFixtures.SPARK_DIALECT, loaded.getSourceDialect());
    Assertions.assertEquals("user-value", loaded.getProperties().get("user-key"));
    Assertions.assertEquals(
        Long.parseLong(metadata.properties().get(getCanonicalFieldName("lastModifiedTime"))),
        loaded.getLastModifiedTime(),
        "last-modified must come from the parsed metadata, not from the clock");
  }

  /** Broken, not absent: collapsing this would let a later create overwrite a live pointer. */
  @Test
  void loadViewPropagatesCorruptMetadataInsteadOfReportingAbsence() {
    HouseTable row = ViewTestFixtures.viewRow(METADATA_PATH);
    when(houseTableRepository.findViewById(any(HouseTablePrimaryKey.class)))
        .thenReturn(Optional.of(row));
    when(storageType.fromString(LOCAL_STORAGE_TYPE)).thenReturn(StorageType.LOCAL);
    FileIO fileIO = mock(FileIO.class);
    when(fileIOManager.getFileIO(StorageType.LOCAL)).thenReturn(fileIO);
    InputFile inputFile = mock(InputFile.class);
    when(fileIO.newInputFile(METADATA_PATH)).thenReturn(inputFile);
    when(viewMetadataCodec.read(inputFile))
        .thenThrow(new NotFoundException("metadata file is gone: %s", METADATA_PATH));

    Assertions.assertThrows(NotFoundException.class, () -> viewCommitEngine.loadView(DB, VIEW));
  }

  /**
   * A contract violation reported by the adapter is neither absence nor a commit outcome, so it
   * propagates untouched rather than being swallowed into a miss.
   */
  @Test
  void loadViewPropagatesAnAdapterContractViolationUntouched() {
    IllegalStateException reportedByTheAdapter =
        new IllegalStateException(
            "House Table answered the view route for viewdb.v1 with a row whose entity type is"
                + " 'TABLE'");
    when(houseTableRepository.findViewById(any(HouseTablePrimaryKey.class)))
        .thenThrow(reportedByTheAdapter);

    IllegalStateException thrown =
        Assertions.assertThrows(
            IllegalStateException.class, () -> viewCommitEngine.loadView(DB, VIEW));

    // The same instance, not merely one of the same type: wrapping or rebuilding it here would lose
    // the adapter's report of which key and which value were corrupt.
    Assertions.assertSame(
        reportedByTheAdapter,
        thrown,
        "the engine must propagate the adapter's own exception untouched");
    Assertions.assertTrue(
        thrown.getMessage().contains(DB) && thrown.getMessage().contains(VIEW),
        "corruption must surface as itself, naming the key: " + thrown.getMessage());
    verifyNoInteractions(fileIOManager);
    verifyNoInteractions(viewMetadataCodec);
  }

  /** Listing is a pointer-row operation: no metadata file is opened for any row on the page. */
  @Test
  void listViewsReturnsPointersWithoutParsingAnyMetadata() {
    Pageable pageable = PageRequest.of(0, 2);
    HouseTable first = ViewTestFixtures.viewRow(METADATA_PATH);
    HouseTable second = ViewTestFixtures.viewRow(METADATA_PATH).toBuilder().tableId("v2").build();
    when(houseTableRepository.findAllViewsByDatabaseId(DB, pageable))
        .thenReturn(new PageImpl<>(Arrays.asList(first, second), pageable, 5L));

    Page<ViewPointer> page = viewCommitEngine.listViews(DB, pageable);

    Assertions.assertEquals(2, page.getContent().size());
    Assertions.assertEquals(5L, page.getTotalElements());
    Assertions.assertEquals(3, page.getTotalPages());
    Assertions.assertEquals(VIEW, page.getContent().get(0).getViewId());
    Assertions.assertEquals("v2", page.getContent().get(1).getViewId());
    Assertions.assertEquals(METADATA_PATH, page.getContent().get(0).getMetadataLocation());
    Assertions.assertEquals(LOCAL_STORAGE_TYPE, page.getContent().get(0).getStorageType());
    verify(houseTableRepository, times(1)).findAllViewsByDatabaseId(DB, pageable);
    verifyNoInteractions(viewMetadataCodec);
    verifyNoInteractions(fileIOManager);
  }

  /** Never falls back to the table delete path, which would purge storage. */
  @Test
  void dropViewIsATypedHardPointerDeleteWithNoStorageOrParserWork() {
    when(houseTableRepository.deleteViewById(any(HouseTablePrimaryKey.class))).thenReturn(true);

    Assertions.assertTrue(viewCommitEngine.dropView(DB, VIEW));

    verify(houseTableRepository, times(1))
        .deleteViewById(HouseTablePrimaryKey.builder().databaseId(DB).tableId(VIEW).build());
    verify(houseTableRepository, never()).deleteById(any(HouseTablePrimaryKey.class));
    verify(houseTableRepository, never())
        .deleteById(any(HouseTablePrimaryKey.class), any(Boolean.class));
    verifyNoInteractions(fileIOManager);
    verifyNoInteractions(viewMetadataCodec);
  }

  /** A key that is absent, or occupied by a table, is not a view this engine can drop. */
  @Test
  void dropViewReturnsFalseWhenTheKeyIsNotAView() {
    when(houseTableRepository.deleteViewById(any(HouseTablePrimaryKey.class))).thenReturn(false);

    Assertions.assertFalse(viewCommitEngine.dropView(DB, VIEW));

    verifyNoInteractions(fileIOManager);
    verifyNoInteractions(viewMetadataCodec);
  }

  /** An ambiguous delete may or may not have landed, so it is unknown state, not failure. */
  @Test
  void dropViewReportsAnAmbiguousDeleteAsCommitStateUnknown() {
    when(houseTableRepository.deleteViewById(any(HouseTablePrimaryKey.class)))
        .thenThrow(new HouseTableRepositoryStateUnknownException("", new RuntimeException("504")));

    Assertions.assertThrows(
        CommitStateUnknownException.class, () -> viewCommitEngine.dropView(DB, VIEW));

    verify(houseTableRepository, times(1)).deleteViewById(any(HouseTablePrimaryKey.class));
  }

  /** Rename is rejected before it can touch House Table or storage at all. */
  @Test
  void renameViewIsUnsupportedAndTouchesNothing() {
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> viewCommitEngine.renameView(DB, VIEW, "v2"));

    verifyNoInteractions(houseTableRepository);
    verifyNoInteractions(fileIOManager);
    verifyNoInteractions(viewMetadataCodec);
  }
}
