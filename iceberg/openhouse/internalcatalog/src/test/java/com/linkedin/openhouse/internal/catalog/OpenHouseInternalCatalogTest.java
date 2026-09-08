package com.linkedin.openhouse.internal.catalog;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OpenHouseInternalCatalogTest {

  private static final String DB = "test_db";
  private static final String TABLE = "test_table";
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of(DB, TABLE);
  private static final String METADATA_LOCATION =
      "/data/openhouse/test_db/test_table-uuid/00001-aaa.metadata.json";
  private static final String EXPECTED_BASE = "/data/openhouse/test_db/test_table-uuid";

  @Test
  void testIsValidIdentifierRequiresDatabaseTableShape() {
    TestOpenHouseInternalCatalog catalog = new TestOpenHouseInternalCatalog();

    Assertions.assertFalse(catalog.isValidBaseIdentifier(TableIdentifier.of("db")));
    Assertions.assertTrue(catalog.isValidBaseIdentifier(TableIdentifier.of("db", "table")));
    Assertions.assertTrue(catalog.isValidBaseIdentifier(TableIdentifier.of("db", "partitions")));
    Assertions.assertFalse(
        catalog.isValidBaseIdentifier(TableIdentifier.of("db", "table", "partitions")));
  }

  @Test
  void findHouseTableReturnsRowWhenPresent() {
    HouseTableRepository repo = mock(HouseTableRepository.class);
    HouseTable row = HouseTable.builder().databaseId(DB).tableId(TABLE).tableUUID("uuid").build();
    when(repo.findById(any(HouseTablePrimaryKey.class))).thenReturn(Optional.of(row));
    OpenHouseInternalCatalog catalog = new OpenHouseInternalCatalog();
    catalog.houseTableRepository = repo;

    Optional<HouseTable> result = catalog.findHouseTable(IDENTIFIER);

    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals("uuid", result.get().getTableUUID());
  }

  @Test
  void findHouseTableReturnsEmptyOnNotFoundException() {
    HouseTableRepository repo = mock(HouseTableRepository.class);
    when(repo.findById(any(HouseTablePrimaryKey.class)))
        .thenThrow(new HouseTableNotFoundException("missing", new RuntimeException()));
    OpenHouseInternalCatalog catalog = new OpenHouseInternalCatalog();
    catalog.houseTableRepository = repo;

    Assertions.assertFalse(catalog.findHouseTable(IDENTIFIER).isPresent());
  }

  @Test
  void dropTableThrowsNoSuchTableWhenHouseTableMissing() {
    HouseTableRepository repo = mock(HouseTableRepository.class);
    when(repo.findById(any(HouseTablePrimaryKey.class))).thenReturn(Optional.empty());
    FileIO fileIO =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsPrefixOperations.class));
    OpenHouseInternalCatalog catalog = new FixedFileIOCatalog(fileIO);
    catalog.houseTableRepository = repo;

    Assertions.assertThrows(NoSuchTableException.class, () -> catalog.dropTable(IDENTIFIER, true));
    verify(repo, never()).deleteById(any(), anyBoolean());
    verify((SupportsPrefixOperations) fileIO, never()).deletePrefix(any());
  }

  @Test
  void dropTableWithPurgeDeletesHtsRowAndPrefix() {
    HouseTableRepository repo = mock(HouseTableRepository.class);
    HouseTable row =
        HouseTable.builder()
            .databaseId(DB)
            .tableId(TABLE)
            .tableUUID("uuid")
            .tableLocation(METADATA_LOCATION)
            .build();
    when(repo.findById(any(HouseTablePrimaryKey.class))).thenReturn(Optional.of(row));
    FileIO fileIO =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsPrefixOperations.class));
    OpenHouseInternalCatalog catalog = new FixedFileIOCatalog(fileIO);
    catalog.houseTableRepository = repo;

    Assertions.assertTrue(catalog.dropTable(IDENTIFIER, true));

    verify(repo).deleteById(any(HouseTablePrimaryKey.class), eq(true));
    verify((SupportsPrefixOperations) fileIO).deletePrefix(EXPECTED_BASE);
  }

  @Test
  void dropTableRefusesWhenMetadataLocationIsNotAMetadataJsonFile() {
    // Defensive: if metadata_location somehow points at a directory (bad migration, manual
    // MySQL edit, future regression), the derived parent would be a level too high — e.g. the
    // whole database directory — which deletePrefix would happily wipe. Refuse instead.
    HouseTableRepository repo = mock(HouseTableRepository.class);
    HouseTable row =
        HouseTable.builder()
            .databaseId(DB)
            .tableId(TABLE)
            .tableLocation("/data/openhouse/test_db/test_table-uuid") // directory, not file
            .build();
    when(repo.findById(any(HouseTablePrimaryKey.class))).thenReturn(Optional.of(row));
    FileIO fileIO =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsPrefixOperations.class));
    OpenHouseInternalCatalog catalog = new FixedFileIOCatalog(fileIO);
    catalog.houseTableRepository = repo;

    Assertions.assertThrows(IllegalStateException.class, () -> catalog.dropTable(IDENTIFIER, true));
    verify(repo, never()).deleteById(any(), anyBoolean());
    verify((SupportsPrefixOperations) fileIO, never()).deletePrefix(any());
  }

  @Test
  void dropTableWithoutPurgeSkipsPrefixDelete() {
    HouseTableRepository repo = mock(HouseTableRepository.class);
    HouseTable row =
        HouseTable.builder().databaseId(DB).tableId(TABLE).tableLocation(METADATA_LOCATION).build();
    when(repo.findById(any(HouseTablePrimaryKey.class))).thenReturn(Optional.of(row));
    FileIO fileIO =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsPrefixOperations.class));
    OpenHouseInternalCatalog catalog = new FixedFileIOCatalog(fileIO);
    catalog.houseTableRepository = repo;

    Assertions.assertTrue(catalog.dropTable(IDENTIFIER, false));

    verify(repo).deleteById(any(HouseTablePrimaryKey.class), eq(false));
    verify((SupportsPrefixOperations) fileIO, never()).deletePrefix(any());
  }

  /**
   * Wired so the pre-fix path can actually run: without a working {@code newTableOps} the inherited
   * {@code tableExists} would fail on its own wiring rather than on the answer it gives.
   */
  private static OpenHouseInternalCatalog catalogOver(HouseTableRepository repo, FileIO fileIO) {
    OpenHouseInternalCatalog catalog = new FixedFileIOCatalog(fileIO);
    catalog.houseTableRepository = repo;
    catalog.fileIOManager = mock(FileIOManager.class);
    catalog.storageSelector = mock(StorageSelector.class);
    catalog.meterRegistry = new SimpleMeterRegistry();
    return catalog;
  }

  private static FileIO recordingFileIO() {
    return mock(FileIO.class, withSettings().extraInterfaces(SupportsPrefixOperations.class));
  }

  /**
   * The typed table read filters out anything that is not a canonical table, as House Table does.
   */
  private static HouseTableRepository repoHolding(HouseTable occupant) {
    HouseTableRepository repo = mock(HouseTableRepository.class);
    Optional<HouseTable> row = Optional.ofNullable(occupant);
    when(repo.findEntityById(any(HouseTablePrimaryKey.class))).thenReturn(row);
    when(repo.findById(any(HouseTablePrimaryKey.class)))
        .thenReturn(row.filter(o -> "TABLE".equals(o.getEntityType())));
    return repo;
  }

  private static HouseTable occupantOfType(String entityType) {
    return HouseTable.builder()
        .databaseId(DB)
        .tableId(TABLE)
        .tableLocation(METADATA_LOCATION)
        .entityType(entityType)
        .build();
  }

  @Test
  void tableExistsRejectsAViewOccupantBeforeAnyAllocation() {
    HouseTableRepository repo = repoHolding(occupantOfType("VIEW"));
    FileIO fileIO = recordingFileIO();
    OpenHouseInternalCatalog catalog = catalogOver(repo, fileIO);

    AlreadyExistsException thrown =
        Assertions.assertThrows(
            AlreadyExistsException.class, () -> catalog.tableExists(IDENTIFIER));

    // Names the occupant, as the server's own guard does.
    Assertions.assertTrue(thrown.getMessage().contains("VIEW"), thrown.getMessage());
    Assertions.assertTrue(thrown.getMessage().contains(DB + "." + TABLE), thrown.getMessage());
    verify(catalog.storageSelector, never()).selectStorage(any(), any());
    verifyNoInteractions(catalog.fileIOManager);
    verifyNoInteractions(fileIO);
    // The probe answers a question; it may not write while answering it.
    verify(repo, never()).save(any(HouseTable.class));
    verify(repo, never()).saveView(any(HouseTable.class));
  }

  /**
   * The safety property behind the clean-409 refinement: whatever holds the name, the name is not
   * free, so no create can reach allocation. Only a view earns the tailored conflict.
   */
  @Test
  void tableExistsNeverReportsAnOccupiedNameAsFree() {
    OpenHouseInternalCatalog catalog =
        catalogOver(repoHolding(occupantOfType("Table")), recordingFileIO());

    Assertions.assertTrue(catalog.tableExists(IDENTIFIER));
  }

  /** Answered from the pointer row alone: an unreadable metadata.json is not this question. */
  @Test
  void tableExistsReportsACanonicalTableOccupantWithoutReadingItsMetadata() {
    HouseTableRepository repo = repoHolding(occupantOfType("TABLE"));
    FileIO fileIO = recordingFileIO();
    OpenHouseInternalCatalog catalog = catalogOver(repo, fileIO);

    Assertions.assertTrue(catalog.tableExists(IDENTIFIER));

    verifyNoInteractions(fileIO);
  }

  /** The parse boundary resolves a legacy null to TABLE, so the catalog never sees a null. */
  @Test
  void tableExistsReportsAFreeNameAsAbsentAndLeavesTheRaceToTheServerGuard() {
    OpenHouseInternalCatalog catalog = catalogOver(repoHolding(null), recordingFileIO());

    Assertions.assertFalse(catalog.tableExists(IDENTIFIER));
  }

  /**
   * An identifier this catalog does not own keeps the inherited answer, so metadata-table and
   * invalid-identifier handling are untouched by the create-decision change.
   */
  @Test
  void tableExistsKeepsTheInheritedAnswerForAnIdentifierThisCatalogDoesNotOwn() {
    HouseTableRepository repo = repoHolding(occupantOfType("VIEW"));
    OpenHouseInternalCatalog catalog = catalogOver(repo, recordingFileIO());

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of(TABLE)));

    verify(repo, never()).findEntityById(any(HouseTablePrimaryKey.class));
  }

  @Test
  void tableExistsReadsTheNeutralEndpointAndNotTheTableTypedOne() {
    HouseTableRepository repo = repoHolding(occupantOfType("TABLE"));
    OpenHouseInternalCatalog catalog = catalogOver(repo, recordingFileIO());

    try {
      catalog.tableExists(IDENTIFIER);
    } catch (RuntimeException tolerated) {
      // Which endpoint was read is the subject here, not what the read answered.
    }

    verify(repo, times(1))
        .findEntityById(HouseTablePrimaryKey.builder().databaseId(DB).tableId(TABLE).build());
    verify(repo, never()).findById(any(HouseTablePrimaryKey.class));
    verify(repo, never()).findViewById(any(HouseTablePrimaryKey.class));
  }

  /** Test subclass that bypasses the real {@link OpenHouseInternalCatalog#resolveFileIO} wiring. */
  private static class FixedFileIOCatalog extends OpenHouseInternalCatalog {
    private final FileIO fileIO;

    FixedFileIOCatalog(FileIO fileIO) {
      this.fileIO = fileIO;
    }

    @Override
    protected FileIO resolveFileIO(TableIdentifier identifier) {
      return fileIO;
    }
  }

  private static class TestOpenHouseInternalCatalog extends OpenHouseInternalCatalog {
    boolean isValidBaseIdentifier(TableIdentifier identifier) {
      return isValidIdentifier(identifier);
    }
  }
}
