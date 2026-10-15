package com.linkedin.openhouse.tables.repository.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.view.ViewCommitEngine;
import com.linkedin.openhouse.internal.catalog.view.model.SqlViewRepresentationIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import com.linkedin.openhouse.internal.catalog.view.model.ViewPointer;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.repository.ViewCommitOutcome;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

public class OpenHouseInternalViewRepositoryImplTest {

  private static final String CAPTURED_POINTER =
      "file:/warehouse/my_database/my_view/metadata/00012-captured.metadata.json";
  private static final String CREATED_RESULT_UUID = "created-view-uuid";
  private static final String COMMITTED_POINTER =
      "file:/warehouse/my_database/my_view/metadata/00013-committed.metadata.json";

  @Test
  public void findByIdUsesTypedViewLookupAndMapsPointerWithoutMetadataRead() {
    HouseTable viewRow = capturedViewRow();
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    when(houseTableRepository.findViewById(any(HouseTablePrimaryKey.class)))
        .thenReturn(Optional.of(viewRow));
    ViewCommitEngine engine = Mockito.mock(ViewCommitEngine.class);
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(houseTableRepository, engine, Mockito.mock(StorageSelector.class));

    ViewDto result =
        repository.findById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);

    verify(houseTableRepository, times(1)).findViewById(any(HouseTablePrimaryKey.class));
    verify(houseTableRepository, never()).findEntityById(any(HouseTablePrimaryKey.class));
    verifyNoInteractions(engine);
    org.junit.jupiter.api.Assertions.assertEquals(ViewModelConstants.VIEW_ID, result.getViewId());
    org.junit.jupiter.api.Assertions.assertEquals(CAPTURED_POINTER, result.getMetadataLocation());
  }

  @Test
  public void findByIdAndPrepareDeleteHideAbsentOrSameNameTableBehindTypedViewLookup() {
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    when(houseTableRepository.findViewById(any(HouseTablePrimaryKey.class)))
        .thenReturn(Optional.empty());
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(
            houseTableRepository,
            Mockito.mock(ViewCommitEngine.class),
            Mockito.mock(StorageSelector.class));

    org.junit.jupiter.api.Assertions.assertThrows(
        com.linkedin.openhouse.tables.exception.ViewApiException.class,
        () -> repository.findById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID));
    PreparedViewOperation prepared =
        repository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);

    verify(houseTableRepository, times(2)).findViewById(any(HouseTablePrimaryKey.class));
    verify(houseTableRepository, never()).findEntityById(any(HouseTablePrimaryKey.class));
    org.junit.jupiter.api.Assertions.assertFalse(prepared.getViewBaseRow().isPresent());
  }

  @Test
  public void prepareDeleteUsesTypedViewLookupAndCarriesCapturedPointer() {
    HouseTable viewRow = capturedViewRow();
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    when(houseTableRepository.findViewById(any(HouseTablePrimaryKey.class)))
        .thenReturn(Optional.of(viewRow));
    ViewCommitEngine engine = Mockito.mock(ViewCommitEngine.class);
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(houseTableRepository, engine, Mockito.mock(StorageSelector.class));

    PreparedViewOperation prepared =
        repository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);

    verify(houseTableRepository, times(1)).findViewById(any(HouseTablePrimaryKey.class));
    verify(houseTableRepository, never()).findEntityById(any(HouseTablePrimaryKey.class));
    verifyNoInteractions(engine);
    assertSame(viewRow, prepared.getViewBaseRow().get());
    org.junit.jupiter.api.Assertions.assertEquals(
        CAPTURED_POINTER, prepared.getViewBaseRow().get().getTableLocation());
  }

  /**
   * Listing reads the typed HTS view page directly: identifiers only, in source order, framed by
   * the caller's requested pageable even when the source page reports a different one.
   */
  @Test
  public void searchViewsReadsTypedHtsViewPageOnceAndMapsIdentifiersOnlyInSourceOrder() {
    Pageable requested = PageRequest.of(1, 2, Sort.by("tableId"));
    Pageable differentSourcePageable = PageRequest.of(0, 2, Sort.by("databaseId"));
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    when(houseTableRepository.findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, requested))
        .thenReturn(
            new PageImpl<>(
                Arrays.asList(listedViewRow("view_b"), listedViewRow("view_a")),
                differentSourcePageable,
                5));
    ViewCommitEngine engine = Mockito.mock(ViewCommitEngine.class);
    StorageSelector storageSelector = Mockito.mock(StorageSelector.class);
    Storage defaultStorage = Mockito.mock(Storage.class);
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(houseTableRepository, engine, storageSelector, defaultStorage);

    Page<ViewDto> results = repository.searchViews(ViewModelConstants.DATABASE_ID, requested);

    verify(houseTableRepository, times(1))
        .findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, requested);
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(engine);
    verifyNoInteractions(storageSelector, defaultStorage);
    assertEquals(
        Arrays.asList(
            ViewDto.builder().viewId("view_b").databaseId(ViewModelConstants.DATABASE_ID).build(),
            ViewDto.builder().viewId("view_a").databaseId(ViewModelConstants.DATABASE_ID).build()),
        results.getContent(),
        "List entries stay identifier-only in source order; pointer and creator fields are unset.");
    assertEquals(requested, results.getPageable());
    assertEquals(1, results.getNumber());
    assertEquals(2, results.getSize());
    assertEquals(Sort.by("tableId"), results.getSort());
    assertEquals(5, results.getTotalElements());
    assertEquals(3, results.getTotalPages());
    assertTrue(results.hasNext());
  }

  /**
   * The requested size frames the result, not the source page's, and a nonempty page keeps the
   * existing request-framed PageImpl total adjustment: requested offset 4 plus one row reports 5
   * total even though the source reported 4.
   */
  @Test
  public void searchViewsFramesNonemptyPageBySizeOfRequestIncludingPageImplTotalAdjustment() {
    Pageable requested = PageRequest.of(2, 2, Sort.by("tableId"));
    Pageable sourcePageable = PageRequest.of(0, 4, Sort.by("databaseId"));
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    when(houseTableRepository.findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, requested))
        .thenReturn(
            new PageImpl<>(Collections.singletonList(listedViewRow("view_a")), sourcePageable, 4));
    ViewCommitEngine engine = Mockito.mock(ViewCommitEngine.class);
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(houseTableRepository, engine, Mockito.mock(StorageSelector.class));

    Page<ViewDto> results = repository.searchViews(ViewModelConstants.DATABASE_ID, requested);

    verify(houseTableRepository, times(1))
        .findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, requested);
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(engine);
    assertEquals(
        Collections.singletonList(
            ViewDto.builder().viewId("view_a").databaseId(ViewModelConstants.DATABASE_ID).build()),
        results.getContent());
    assertEquals(requested, results.getPageable());
    assertEquals(2, results.getNumber());
    assertEquals(2, results.getSize());
    assertEquals(Sort.by("tableId"), results.getSort());
    assertEquals(5, results.getTotalElements());
    assertEquals(3, results.getTotalPages());
    assertFalse(results.hasNext());
  }

  @Test
  public void searchViewsKeepsShortAndEmptyNonterminalAndTerminalPagesDistinct() {
    Pageable shortNonterminal = PageRequest.of(0, 2, Sort.by("tableId"));
    Pageable emptyNonterminal = PageRequest.of(1, 2, Sort.by("tableId"));
    Pageable terminal = PageRequest.of(2, 2, Sort.by("tableId"));
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    when(houseTableRepository.findAllViewsByDatabaseId(
            ViewModelConstants.DATABASE_ID, shortNonterminal))
        .thenReturn(
            new PageImpl<>(
                Collections.singletonList(listedViewRow(ViewModelConstants.VIEW_ID)),
                shortNonterminal,
                5));
    when(houseTableRepository.findAllViewsByDatabaseId(
            ViewModelConstants.DATABASE_ID, emptyNonterminal))
        .thenReturn(new PageImpl<>(Collections.emptyList(), emptyNonterminal, 6));
    when(houseTableRepository.findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, terminal))
        .thenReturn(new PageImpl<>(Collections.emptyList(), terminal, 4));
    ViewCommitEngine engine = Mockito.mock(ViewCommitEngine.class);
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(houseTableRepository, engine, Mockito.mock(StorageSelector.class));

    Page<ViewDto> shortPage =
        repository.searchViews(ViewModelConstants.DATABASE_ID, shortNonterminal);
    Page<ViewDto> empty = repository.searchViews(ViewModelConstants.DATABASE_ID, emptyNonterminal);
    Page<ViewDto> last = repository.searchViews(ViewModelConstants.DATABASE_ID, terminal);

    assertEquals(1, shortPage.getContent().size());
    assertEquals(0, shortPage.getNumber());
    assertEquals(5, shortPage.getTotalElements());
    assertTrue(
        shortPage.hasNext(),
        "A short source page with remaining rows must stay nonterminal for the cursor adapter.");
    assertTrue(empty.getContent().isEmpty());
    assertEquals(1, empty.getNumber());
    assertEquals(6, empty.getTotalElements());
    assertTrue(empty.hasNext(), "An empty page is not proof of the end of the listing.");
    assertTrue(last.getContent().isEmpty());
    assertEquals(2, last.getNumber());
    assertEquals(4, last.getTotalElements());
    assertFalse(last.hasNext());
    verifyNoInteractions(engine);
  }

  /**
   * A typed-list contract violation or outage is neither an empty page nor a commit outcome: the
   * same exception surfaces after one read.
   */
  @Test
  public void searchViewsPropagatesTypedHtsListFailuresUntouchedAfterOneRead() {
    Pageable requested = PageRequest.of(0, 2, Sort.by("tableId"));
    RuntimeException[] failures = {
      new IllegalStateException("House Table listed a non-view row for the view route"),
      new HouseTableRepositoryStateUnknownException("", new RuntimeException("504"))
    };
    for (RuntimeException failure : failures) {
      HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
      when(houseTableRepository.findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, requested))
          .thenThrow(failure);
      ViewCommitEngine engine = Mockito.mock(ViewCommitEngine.class);
      OpenHouseInternalViewRepositoryImpl repository =
          newRepository(houseTableRepository, engine, Mockito.mock(StorageSelector.class));

      RuntimeException thrown =
          assertThrows(
              RuntimeException.class,
              () -> repository.searchViews(ViewModelConstants.DATABASE_ID, requested));

      assertSame(failure, thrown);
      verify(houseTableRepository, times(1))
          .findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, requested);
      verifyNoMoreInteractions(houseTableRepository);
      verifyNoInteractions(engine);
    }
  }

  @Test
  public void prepareWriteCapturesTableOccupantNeutrallyWithoutAllocationOrClassification() {
    HouseTable tableOccupant =
        HouseTable.builder()
            .databaseId(ViewModelConstants.DATABASE_ID)
            .tableId(ViewModelConstants.VIEW_ID)
            .tableLocation(CAPTURED_POINTER)
            .storageType("local")
            .entityType("TABLE")
            .build();
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    when(houseTableRepository.findEntityById(any(HouseTablePrimaryKey.class)))
        .thenReturn(Optional.of(tableOccupant));
    StorageSelector storageSelector = Mockito.mock(StorageSelector.class);
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(houseTableRepository, Mockito.mock(ViewCommitEngine.class), storageSelector);

    PreparedViewOperation prepared =
        repository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);

    assertSame(tableOccupant, prepared.getOccupantRow().get());
    assertFalse(
        prepared.getViewBaseRow().isPresent(),
        "A TABLE occupant is captured but not revealed as a view before service authorization.");
    verify(storageSelector, never()).selectStorage(any(), any());
  }

  @Test
  public void commitReplaceUsesTheSingleCapturedCasSnapshotWithoutRefreshingHts() {
    HouseTable captured =
        HouseTable.builder()
            .databaseId(ViewModelConstants.DATABASE_ID)
            .tableId(ViewModelConstants.VIEW_ID)
            .tableLocation(CAPTURED_POINTER)
            .tableUUID("view-uuid")
            .storageType("local")
            .entityType("VIEW")
            .build();
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    when(houseTableRepository.findEntityById(any(HouseTablePrimaryKey.class)))
        .thenReturn(Optional.of(captured))
        .thenThrow(new AssertionError("commit must not refresh the prepared row"));
    ViewCommitEngine engine = Mockito.mock(ViewCommitEngine.class);
    when(engine.commit(any(ViewCommitIntent.class))).thenReturn(committedResult());
    StorageSelector storageSelector = Mockito.mock(StorageSelector.class);
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(houseTableRepository, engine, storageSelector);

    PreparedViewOperation prepared =
        repository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    ViewCommitOutcome result =
        repository.commitReplace(ViewModelConstants.fullyPopulatedRequest(), prepared, "alice");

    ArgumentCaptor<ViewCommitIntent> intent = ArgumentCaptor.forClass(ViewCommitIntent.class);
    verify(engine).commit(intent.capture());
    verify(houseTableRepository, times(1)).findEntityById(any(HouseTablePrimaryKey.class));
    assertSame(captured, prepared.getViewBaseRow().get());
    assertSame(captured, intent.getValue().getBaseRow());
    org.junit.jupiter.api.Assertions.assertEquals(Boolean.FALSE, intent.getValue().getIsCreate());
    assertIntentCarriesRequestedDefinition(intent.getValue());
    assertEquals(COMMITTED_POINTER, result.getDto().getMetadataLocation());
    assertEquals("view-uuid", result.getCommittedViewUuid());
    assertFalse(result.isCreated());
    verify(storageSelector, never()).selectStorage(any(), any());
  }

  @Test
  public void commitCreateAllocatesSelectedUuidRootAndPassesStorageTypeToEngine() {
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    StorageSelector storageSelector = Mockito.mock(StorageSelector.class);
    Storage storage = Mockito.mock(Storage.class);
    when(storage.getType()).thenReturn(StorageType.HDFS);
    when(storage.allocateTableLocation(
            eq(ViewModelConstants.DATABASE_ID),
            eq(ViewModelConstants.VIEW_ID),
            any(),
            eq("alice"),
            anyMap()))
        .thenAnswer(
            invocation ->
                "hdfs:/warehouse/"
                    + ViewModelConstants.DATABASE_ID
                    + "/"
                    + ViewModelConstants.VIEW_ID
                    + "-"
                    + invocation.getArgument(2));
    when(storageSelector.selectStorage(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(storage);
    ViewCommitEngine engine = Mockito.mock(ViewCommitEngine.class);
    when(engine.commit(any(ViewCommitIntent.class))).thenReturn(createdResult());
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(houseTableRepository, engine, storageSelector);

    ViewCommitOutcome outcome =
        repository.commitCreate(
            ViewModelConstants.createRequestWithoutBaseVersion(),
            PreparedViewOperation.observedAbsence(),
            "alice");

    ArgumentCaptor<ViewCommitIntent> intent = ArgumentCaptor.forClass(ViewCommitIntent.class);
    ArgumentCaptor<String> allocatedUuid = ArgumentCaptor.forClass(String.class);
    verify(storageSelector)
        .selectStorage(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    verify(storage)
        .allocateTableLocation(
            eq(ViewModelConstants.DATABASE_ID),
            eq(ViewModelConstants.VIEW_ID),
            allocatedUuid.capture(),
            eq("alice"),
            anyMap());
    verify(engine).commit(intent.capture());
    org.junit.jupiter.api.Assertions.assertEquals(Boolean.TRUE, intent.getValue().getIsCreate());
    org.junit.jupiter.api.Assertions.assertEquals("hdfs", intent.getValue().getStorageType());
    assertIntentCarriesRequestedDefinition(intent.getValue());
    UUID.fromString(intent.getValue().getViewUuid());
    org.junit.jupiter.api.Assertions.assertEquals(
        allocatedUuid.getValue(), intent.getValue().getViewUuid());
    org.junit.jupiter.api.Assertions.assertEquals(
        "hdfs:/warehouse/"
            + ViewModelConstants.DATABASE_ID
            + "/"
            + ViewModelConstants.VIEW_ID
            + "-"
            + intent.getValue().getViewUuid(),
        intent.getValue().getViewLocation());
    // The audit carrier is sourced from the engine's committed result, not reread or parsed.
    assertEquals(CREATED_RESULT_UUID, outcome.getCommittedViewUuid());
    assertTrue(outcome.isCreated());
    assertEquals(COMMITTED_POINTER, outcome.getDto().getMetadataLocation());
    assertEquals(ViewModelConstants.VIEW_ID, outcome.getDto().getViewId());
    verify(houseTableRepository, never()).findEntityById(any(HouseTablePrimaryKey.class));
    verify(houseTableRepository, never()).findViewById(any(HouseTablePrimaryKey.class));
  }

  @Test
  public void deleteByIdIsOneTypedHardDeleteWithNoReadsOrEngineWork() {
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    when(houseTableRepository.deleteViewById(any(HouseTablePrimaryKey.class))).thenReturn(true);
    ViewCommitEngine engine = Mockito.mock(ViewCommitEngine.class);
    StorageSelector storageSelector = Mockito.mock(StorageSelector.class);
    Storage defaultStorage = Mockito.mock(Storage.class);
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(houseTableRepository, engine, storageSelector, defaultStorage);

    repository.deleteById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);

    // No reread, generic/table delete overload, or purge: exactly the one typed hard delete.
    verify(houseTableRepository, times(1)).deleteViewById(viewKey());
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(engine);
    verifyNoInteractions(storageSelector, defaultStorage);
  }

  /**
   * The typed delete returns false when, after the service's typed capture, the name became absent
   * or now holds a table. That is the ordinary NO_SUCH_VIEW outcome, with one attempt and no
   * refresh, not a server fault.
   */
  @Test
  public void deleteThatFindsNoViewAfterCaptureIsNoSuchViewWithOneAttemptAndNoRefresh() {
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    when(houseTableRepository.deleteViewById(any(HouseTablePrimaryKey.class))).thenReturn(false);
    ViewCommitEngine engine = Mockito.mock(ViewCommitEngine.class);
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(houseTableRepository, engine, Mockito.mock(StorageSelector.class));

    com.linkedin.openhouse.tables.exception.ViewApiException thrown =
        org.junit.jupiter.api.Assertions.assertThrows(
            com.linkedin.openhouse.tables.exception.ViewApiException.class,
            () ->
                repository.deleteById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID));

    assertEquals(
        com.linkedin.openhouse.tables.exception.ViewErrorCode.NO_SUCH_VIEW, thrown.getErrorCode());
    assertEquals(org.springframework.http.HttpStatus.NOT_FOUND, thrown.getHttpStatus());
    verify(houseTableRepository, times(1)).deleteViewById(viewKey());
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(engine);
  }

  /** An ambiguous delete may have landed, so it is unknown mutation state, not failure. */
  @Test
  public void ambiguousDeleteIsCommitStateUnknownCarryingTheHtsCauseAfterOneAttempt() {
    HouseTableRepositoryStateUnknownException ambiguous =
        new HouseTableRepositoryStateUnknownException("", new RuntimeException("504"));
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    when(houseTableRepository.deleteViewById(any(HouseTablePrimaryKey.class))).thenThrow(ambiguous);
    ViewCommitEngine engine = Mockito.mock(ViewCommitEngine.class);
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(houseTableRepository, engine, Mockito.mock(StorageSelector.class));

    CommitStateUnknownException thrown =
        assertThrows(
            CommitStateUnknownException.class,
            () ->
                repository.deleteById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID));

    assertSame(ambiguous, thrown.getCause());
    verify(houseTableRepository, times(1)).deleteViewById(viewKey());
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(engine);
  }

  /**
   * Only the state-unknown subtype is ambiguous. A sibling repository failure (here the common
   * supertype, so a catch widened to it or to RuntimeException is caught) propagates as itself.
   */
  @Test
  public void otherDeleteFailuresPropagateUntouchedAfterOneAttempt() {
    HouseTableRepositoryException failure =
        new HouseTableRepositoryException("rejected", new RuntimeException("400"));
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    when(houseTableRepository.deleteViewById(any(HouseTablePrimaryKey.class))).thenThrow(failure);
    ViewCommitEngine engine = Mockito.mock(ViewCommitEngine.class);
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(houseTableRepository, engine, Mockito.mock(StorageSelector.class));

    HouseTableRepositoryException thrown =
        assertThrows(
            HouseTableRepositoryException.class,
            () ->
                repository.deleteById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID));

    assertSame(failure, thrown);
    verify(houseTableRepository, times(1)).deleteViewById(viewKey());
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(engine);
  }

  /**
   * The delete-time wrapper must not reach preparation: a precommit HTS outage stays raw, so the
   * service can tell it apart from an unacknowledged delete.
   */
  @Test
  public void prepareDeleteLeavesAPrecommitHtsOutageRaw() {
    HouseTableRepositoryStateUnknownException outage =
        new HouseTableRepositoryStateUnknownException("", new RuntimeException("503"));
    HouseTableRepository houseTableRepository = Mockito.mock(HouseTableRepository.class);
    when(houseTableRepository.findViewById(any(HouseTablePrimaryKey.class))).thenThrow(outage);
    ViewCommitEngine engine = Mockito.mock(ViewCommitEngine.class);
    OpenHouseInternalViewRepositoryImpl repository =
        newRepository(houseTableRepository, engine, Mockito.mock(StorageSelector.class));

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                repository.prepareDelete(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID));

    assertSame(outage, thrown);
    verify(houseTableRepository, never()).deleteViewById(any(HouseTablePrimaryKey.class));
    verifyNoInteractions(engine);
  }

  /**
   * The bridge must forward the caller's definition to the engine unchanged. Expected values are
   * the request fixture's own constants, and the schema is parsed by Iceberg directly.
   */
  private static void assertIntentCarriesRequestedDefinition(ViewCommitIntent intent) {
    assertEquals(ViewModelConstants.DATABASE_ID, intent.getDatabaseId());
    assertEquals(ViewModelConstants.VIEW_ID, intent.getViewId());
    assertEquals(
        Collections.singletonList(
            SqlViewRepresentationIntent.builder()
                .sql(ViewModelConstants.VIEW_SQL)
                .dialect(ViewModelConstants.SOURCE_DIALECT)
                .build()),
        intent.getRepresentations());
    assertEquals(ViewModelConstants.SOURCE_DIALECT, intent.getSourceDialect());
    assertTrue(
        SchemaParser.fromJson(ViewModelConstants.VIEW_SCHEMA_LITERAL)
            .sameSchema(intent.getSchema()),
        "schema: " + intent.getSchema());
    assertEquals(ViewModelConstants.DEFAULT_CATALOG, intent.getDefaultCatalog());
    assertEquals(Namespace.of(ViewModelConstants.DATABASE_ID), intent.getDefaultNamespace());
    assertEquals(ViewModelConstants.VIEW_PROPERTIES, intent.getViewProperties());
  }

  private static ViewCommitResult committedResult() {
    return ViewCommitResult.builder()
        .pointer(
            ViewPointer.builder()
                .databaseId(ViewModelConstants.DATABASE_ID)
                .viewId(ViewModelConstants.VIEW_ID)
                .metadataLocation(COMMITTED_POINTER)
                .storageType("local")
                .creationTime(ViewModelConstants.CREATION_TIME)
                .build())
        .viewUuid("view-uuid")
        .lastModifiedTime(ViewModelConstants.LAST_MODIFIED_TIME)
        .created(false)
        .metadataChanged(true)
        .build();
  }

  private static HouseTable capturedViewRow() {
    return HouseTable.builder()
        .databaseId(ViewModelConstants.DATABASE_ID)
        .tableId(ViewModelConstants.VIEW_ID)
        .tableUUID("view-uuid")
        .tableLocation(CAPTURED_POINTER)
        .storageType("local")
        .entityType("VIEW")
        .build();
  }

  /** A fully populated row, so the list DTO's unset pointer/creator fields are meaningful. */
  private static HouseTable listedViewRow(String viewId) {
    return capturedViewRow()
        .toBuilder()
        .tableId(viewId)
        .tableCreator("alice")
        .creationTime(ViewModelConstants.CREATION_TIME)
        .lastModifiedTime(ViewModelConstants.LAST_MODIFIED_TIME)
        .build();
  }

  private static HouseTablePrimaryKey viewKey() {
    return HouseTablePrimaryKey.builder()
        .databaseId(ViewModelConstants.DATABASE_ID)
        .tableId(ViewModelConstants.VIEW_ID)
        .build();
  }

  private static ViewCommitResult createdResult() {
    return ViewCommitResult.builder()
        .pointer(
            ViewPointer.builder()
                .databaseId(ViewModelConstants.DATABASE_ID)
                .viewId(ViewModelConstants.VIEW_ID)
                .metadataLocation(COMMITTED_POINTER)
                .storageType("local")
                .creationTime(ViewModelConstants.CREATION_TIME)
                .build())
        .viewUuid(CREATED_RESULT_UUID)
        .lastModifiedTime(ViewModelConstants.LAST_MODIFIED_TIME)
        .created(true)
        .metadataChanged(true)
        .build();
  }

  private static OpenHouseInternalViewRepositoryImpl newRepository(
      HouseTableRepository houseTableRepository,
      ViewCommitEngine engine,
      StorageSelector storageSelector) {
    return newRepository(
        houseTableRepository, engine, storageSelector, Mockito.mock(Storage.class));
  }

  private static OpenHouseInternalViewRepositoryImpl newRepository(
      HouseTableRepository houseTableRepository,
      ViewCommitEngine engine,
      StorageSelector storageSelector,
      Storage defaultStorage) {
    return new OpenHouseInternalViewRepositoryImpl(
        houseTableRepository, engine, storageSelector, defaultStorage);
  }
}
