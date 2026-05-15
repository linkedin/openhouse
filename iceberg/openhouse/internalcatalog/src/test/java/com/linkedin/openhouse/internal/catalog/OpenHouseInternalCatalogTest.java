package com.linkedin.openhouse.internal.catalog;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
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
}
