package com.linkedin.openhouse.internal.catalog;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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

  // ---------------------------------------------------------------------------------------------
  // Table APIs must fail closed on non-table pointer rows
  // ---------------------------------------------------------------------------------------------

  private static final String DEST_TABLE = "dest_table";
  private static final TableIdentifier DEST_IDENTIFIER = TableIdentifier.of(DB, DEST_TABLE);

  private static HouseTablePrimaryKey key(String tableId) {
    return HouseTablePrimaryKey.builder().databaseId(DB).tableId(tableId).build();
  }

  private static HouseTable pointer(String tableId, String entityType) {
    return HouseTable.builder()
        .databaseId(DB)
        .tableId(tableId)
        .tableUUID("uuid")
        .tableLocation("/data/openhouse/test_db/" + tableId + "-uuid/00001-aaa.metadata.json")
        .entityType(entityType)
        .build();
  }

  /**
   * Records whether the expensive typed load / transaction path was reached. The guards under test
   * must reject before any of it runs, so the recording overrides throw if invoked in a case where
   * the test expects them not to be.
   */
  private static class RecordingCatalog extends OpenHouseInternalCatalog {
    private final FileIO fileIO;
    boolean loadTableCalled = false;

    RecordingCatalog(FileIO fileIO) {
      this.fileIO = fileIO;
    }

    @Override
    protected FileIO resolveFileIO(TableIdentifier identifier) {
      return fileIO;
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
      loadTableCalled = true;
      throw new AssertionError(
          "loadTable must not be reached for a rejected rename: " + identifier);
    }
  }

  /**
   * A VIEW (any spelling) or unknown discriminator is not a table: drop must behave as "no such
   * table" and must never delete the shared pointer row or purge the object's files.
   */
  @ParameterizedTest
  @ValueSource(strings = {"VIEW", "view", "ViEw", "UNKNOWN"})
  void dropTableRejectsNonTableValuesWithoutDeletingPointerOrFiles(String entityType) {
    HouseTableRepository repo = mock(HouseTableRepository.class);
    when(repo.findById(any(HouseTablePrimaryKey.class)))
        .thenReturn(Optional.of(pointer(TABLE, entityType)));
    FileIO fileIO =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsPrefixOperations.class));
    OpenHouseInternalCatalog catalog = new FixedFileIOCatalog(fileIO);
    catalog.houseTableRepository = repo;

    Assertions.assertThrows(NoSuchTableException.class, () -> catalog.dropTable(IDENTIFIER, true));

    verify(repo, never()).deleteById(any(), anyBoolean());
    verify((SupportsPrefixOperations) fileIO, never()).deletePrefix(any());
  }

  /**
   * The complement of the guard above: null and every spelling of TABLE remain droppable. This is
   * what proves the Java guard and the SQL predicate agree on {@code table} / {@code TaBlE} — a
   * guard that only accepted the uppercase literal would make lower/mixed-case rows visible in
   * listings yet undroppable.
   */
  @ParameterizedTest
  @CsvSource(
      nullValues = "NULL",
      value = {"NULL", "TABLE", "table", "TaBlE"})
  void dropTableAcceptsCaseVariantsOfTable(String entityType) {
    HouseTableRepository repo = mock(HouseTableRepository.class);
    when(repo.findById(any(HouseTablePrimaryKey.class)))
        .thenReturn(Optional.of(pointer(TABLE, entityType)));
    FileIO fileIO =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsPrefixOperations.class));
    OpenHouseInternalCatalog catalog = new FixedFileIOCatalog(fileIO);
    catalog.houseTableRepository = repo;

    Assertions.assertTrue(catalog.dropTable(IDENTIFIER, false));

    verify(repo).deleteById(any(HouseTablePrimaryKey.class), eq(false));
    verify((SupportsPrefixOperations) fileIO, never()).deletePrefix(any());
  }

  /**
   * A wrong-type rename SOURCE is indistinguishable from "no such table" and must be rejected
   * before the source table is loaded, before any transaction is opened, and before the pointer is
   * renamed.
   */
  @ParameterizedTest
  @ValueSource(strings = {"VIEW", "view", "ViEw", "UNKNOWN"})
  void renameTableRejectsNonTableSourceBeforeLoadingMetadata(String entityType) {
    HouseTableRepository repo = mock(HouseTableRepository.class);
    when(repo.findById(key(TABLE))).thenReturn(Optional.of(pointer(TABLE, entityType)));
    when(repo.findById(key(DEST_TABLE))).thenReturn(Optional.empty());
    FileIO fileIO =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsPrefixOperations.class));
    RecordingCatalog catalog = new RecordingCatalog(fileIO);
    catalog.houseTableRepository = repo;

    Assertions.assertThrows(
        NoSuchTableException.class, () -> catalog.renameTable(IDENTIFIER, DEST_IDENTIFIER));

    Assertions.assertFalse(catalog.loadTableCalled, "Source table must not be loaded");
    verify(repo, never()).rename(any(), any(), any(), any(), any());
    verify(repo, never()).save(any());
    verify(repo, never()).deleteById(any(), anyBoolean());
  }

  /**
   * Defense in depth for direct catalog callers: ANY occupied destination pointer — a table, a view
   * in any spelling, or an unknown type — is a name collision, and it must be detected before the
   * source is loaded or a transaction is opened.
   *
   * <p>Because the shared primary key would eventually reject the write anyway with the SAME
   * exception type, the exception alone proves nothing. The load-bearing assertions are the
   * never-verifications: correct code never loads the source, never opens a transaction, and never
   * asks the repository to rename or save.
   */
  @ParameterizedTest
  @ValueSource(strings = {"TABLE", "VIEW", "view", "ViEw", "UNKNOWN"})
  void renameTableRejectsAnyOccupiedRawDestinationBeforeSourceLoad(String destinationEntityType) {
    HouseTableRepository repo = mock(HouseTableRepository.class);
    when(repo.findById(key(TABLE))).thenReturn(Optional.of(pointer(TABLE, null)));
    when(repo.findById(key(DEST_TABLE)))
        .thenReturn(Optional.of(pointer(DEST_TABLE, destinationEntityType)));
    FileIO fileIO =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsPrefixOperations.class));
    RecordingCatalog catalog = new RecordingCatalog(fileIO);
    catalog.houseTableRepository = repo;

    Assertions.assertThrows(
        AlreadyExistsException.class, () -> catalog.renameTable(IDENTIFIER, DEST_IDENTIFIER));

    Assertions.assertFalse(
        catalog.loadTableCalled, "Destination occupancy must be checked before loading the source");
    verify(repo, never()).rename(any(), any(), any(), any(), any());
    verify(repo, never()).save(any());
    verify(repo, never()).deleteById(any(), anyBoolean());
    verify((SupportsPrefixOperations) fileIO, never()).deletePrefix(any());
  }
}
