package com.linkedin.openhouse.spark.e2e.extensions;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.spark.SparkTestBase;
import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class RenameTableTest {

  @Test
  public void testRenameTable() {
    // Setup mock responses for the source table
    Object sourceTable =
        mockGetTableResponseBody(
            "dbRename",
            "t1",
            "c1",
            "dbRename.t1",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbRename", "t1"), convertSchemaToDDLComponent(baseSchema), ""),
            "V1",
            baseSchema,
            null,
            null);

    // Mock the API responses
    mockTableService.enqueue(mockResponse(200, sourceTable));

    // Execute the rename statement
    String renameStatement = "ALTER TABLE openhouse.dbRename.t1 RENAME TO openhouse.dbRename.t2";
    Assertions.assertDoesNotThrow(() -> spark.sql(renameStatement));
  }

  @Test
  public void testRenameTableWithSpecialCharacters() {
    // Setup mock responses for the source table
    Object sourceTable =
        mockGetTableResponseBody(
            "dbRename",
            "source_table_1",
            "c1",
            "dbRename.source_table_1",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbRename", "source_table_1"),
                convertSchemaToDDLComponent(baseSchema),
                ""),
            "V1",
            baseSchema,
            null,
            null);

    // Mock the API responses
    mockTableService.enqueue(mockResponse(200, sourceTable));

    // Execute the rename statement
    String renameStatement =
        "ALTER TABLE openhouse.dbRename.source_table_1 RENAME TO openhouse.dbRename.target_table_2";
    Assertions.assertDoesNotThrow(() -> spark.sql(renameStatement));
  }

  @Test
  public void testRenameTableInvalidSyntax() {

    // Setup mock responses for the source table
    Object sourceTable =
        mockGetTableResponseBody(
            "dbRename",
            "t3",
            "c1",
            "dbRename.t3",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbRename", "t3"), convertSchemaToDDLComponent(baseSchema), ""),
            "V1",
            baseSchema,
            null,
            null);

    // Mock the API responses
    mockTableService.enqueue(mockResponse(200, sourceTable));
    mockTableService.enqueue(mockResponse(200, sourceTable));
    mockTableService.enqueue(mockResponse(200, sourceTable));

    // Missing TO keyword
    Assertions.assertThrows(
        ParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.dbRename.t3 RENAME openhouse.dbRename.t4"));

    // Missing target table
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.dbRename.t3 RENAME TO"));

    // Missing source table
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE RENAME TO openhouse.dbRename.t4"));
  }

  @Test
  public void testRenameTableInvalidCatalogs() {
    // Setup mock responses for the source table
    Object sourceTable =
        mockGetTableResponseBody(
            "dbRename",
            "t4",
            "c1",
            "dbRename.t4",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbRename", "t4"), convertSchemaToDDLComponent(baseSchema), ""),
            "V1",
            baseSchema,
            null,
            null);

    // Mock the API responses
    mockTableService.enqueue(mockResponse(200, sourceTable));
    mockTableService.enqueue(mockResponse(200, sourceTable));
    mockTableService.enqueue(mockResponse(200, sourceTable));
    // Missing catalog
    Assertions.assertThrows(
        ParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.dbRename.t4 RENAME dbRename.t5"));

    // Cannot rename across catalogs
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> spark.sql("ALTER TABLE openhouse.dbRename.t4 RENAME TO testhelper.dbRename.t5"));

    // Too many levels
    Assertions.assertThrows(
        ValidationException.class,
        () -> spark.sql("ALTER TABLE openhouse.dbRename.t4 RENAME TO openhouse.db.dbRename.t5"));
  }

  @Test
  public void testRenameUseCatalog() {
    // Setup mock responses for the source table
    Object sourceTable =
        mockGetTableResponseBody(
            "dbRename",
            "t5",
            "c1",
            "dbRename.t5",
            "u1",
            mockTableLocation(
                TableIdentifier.of("dbRename", "t5"), convertSchemaToDDLComponent(baseSchema), ""),
            "V1",
            baseSchema,
            null,
            null);
    // Mock the API responses
    mockTableService.enqueue(mockResponse(200, sourceTable));

    spark.sql("USE openhouse");
    spark.sql("ALTER TABLE dbRename.t5 RENAME TO dbRename.t6");
  }
}
