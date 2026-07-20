package com.linkedin.openhouse.spark.e2e.dml;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.spark.SparkTestBase;
import java.util.Arrays;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Characterizes the existing behavior of writes to OpenHouse tables when the writer uses column
 * names whose casing differs from the stored schema (e.g. writing {@code test} to a table that
 * stores {@code TeSt}).
 *
 * <p>The tests confirm the reviewer's observation that many write paths already succeed and
 * identify the paths that currently fail, establishing a baseline before any server-side or
 * client-side fixes are applied.
 *
 * <p>Key findings:
 *
 * <ul>
 *   <li><b>Positional INSERT</b> (no column list) always succeeds — no name matching is required.
 *   <li><b>Explicit column-list INSERT</b> always fails in Spark 3.1 when casing differs from the
 *       stored schema, even with {@code caseSensitive=false}. Spark 3.1's {@code ResolveInsertInto}
 *       rule matches the INSERT column list case-sensitively regardless of the configuration
 *       setting.
 *   <li><b>{@code df.writeTo().append()}</b> succeeds with {@code caseSensitive=false} (the
 *       default): Spark's Iceberg integration maps DataFrame column names to table column names
 *       case-insensitively at analysis time. The server always receives the write using the
 *       existing stored casing, so {@code writeSchema.sameSchema(tableSchema)} is {@code true} and
 *       no validation is triggered. With {@code caseSensitive=true} it fails.
 * </ul>
 *
 * <p>The server-side normalization fix is therefore needed for non-Spark clients (Trino DML, direct
 * REST) that send column names in a different case in the PATCH body.
 */
@ExtendWith(SparkTestBase.class)
public class CaseInsensitiveWriteTest {

  // Iceberg schema JSON for a table with uppercase "ID" and optional "value" columns.
  private static final String SCHEMA_UPPERCASE_ID =
      "{\"type\":\"struct\",\"fields\":["
          + "{\"id\":1,\"name\":\"ID\",\"required\":true,\"type\":\"string\"},"
          + "{\"id\":2,\"name\":\"value\",\"required\":false,\"type\":\"string\"}"
          + "]}";

  // Iceberg schema JSON matching the reviewer's example: mixed-case "TeSt" plus "id" (bigint).
  private static final String SCHEMA_MIXED_CASE =
      "{\"type\":\"struct\",\"fields\":["
          + "{\"id\":1,\"name\":\"TeSt\",\"required\":true,\"type\":\"string\"},"
          + "{\"id\":2,\"name\":\"id\",\"required\":true,\"type\":\"long\"}"
          + "]}";

  /**
   * Writes succeed when column names are not specified (positional INSERT).
   *
   * <p>Positional inserts never need to resolve a column name against the table schema, so casing
   * differences are irrelevant. This is the most common write pattern and works regardless of the
   * {@code spark.sql.caseSensitive} setting.
   */
  @Test
  public void testPositionalInsert_succeedsRegardlessOfStoredCasing() {
    TableIdentifier tableId = TableIdentifier.of("dbCaseWrite", "positional");
    Object tableResponse =
        mockGetTableResponseBody(
            "dbCaseWrite",
            "positional",
            "c1",
            "dbCaseWrite.positional",
            "UUID1",
            mockTableLocation(tableId, "ID string, value string", ""),
            "v1",
            SCHEMA_UPPERCASE_ID,
            null,
            null);
    Object tableAfterInsert =
        mockGetTableResponseBody(
            "dbCaseWrite",
            "positional",
            "c1",
            "dbCaseWrite.positional",
            "UUID1",
            mockTableLocationAfterOperation(tableId, "INSERT INTO %t VALUES ('row1', 'v1')"),
            "v2",
            SCHEMA_UPPERCASE_ID,
            null,
            null);

    mockTableService.enqueue(mockResponse(200, tableResponse)); // doRefresh
    mockTableService.enqueue(mockResponse(200, tableResponse)); // doRefresh
    mockTableService.enqueue(mockResponse(200, tableAfterInsert)); // doCommit
    mockTableService.enqueue(mockResponse(200, tableAfterInsert)); // doRefresh
    mockTableService.enqueue(mockResponse(200, tableAfterInsert)); // doRefresh

    // No column list → no name resolution → casing is irrelevant.
    Assertions.assertDoesNotThrow(
        () -> spark.sql("INSERT INTO openhouse.dbCaseWrite.positional VALUES ('row1', 'v1')"));
  }

  /**
   * Writes fail with an explicit (differently-cased) column list even when {@code
   * spark.sql.caseSensitive=false} (the Spark default) in Spark 3.1.
   *
   * <p>Spark 3.1's {@code ResolveInsertInto} rule does <em>not</em> apply the {@code caseSensitive}
   * configuration when resolving the explicit column list of an INSERT statement. It matches column
   * names exactly (case-sensitively) regardless of the setting. Here {@code id} does not exactly
   * match the stored {@code ID}, so the column is silently dropped and only one value column
   * survives analysis, causing an {@code AnalysisException: not enough data columns}.
   *
   * <p>This means the {@code spark.sql.caseSensitive} flag only applies to column references in
   * SELECT expressions, not to the column list of INSERT statements in Spark 3.1.
   */
  @Test
  public void testExplicitColumnInsert_failsEvenWithDefaultCaseSensitivity() {
    TableIdentifier tableId = TableIdentifier.of("dbCaseWrite", "explicit");
    Object tableResponse =
        mockGetTableResponseBody(
            "dbCaseWrite",
            "explicit",
            "c1",
            "dbCaseWrite.explicit",
            "UUID2",
            mockTableLocation(tableId, "ID string, value string", ""),
            "v1",
            SCHEMA_UPPERCASE_ID,
            null,
            null);

    // Analysis fails before the write is submitted; only doRefresh responses are needed.
    mockTableService.enqueue(mockResponse(200, tableResponse)); // doRefresh
    mockTableService.enqueue(mockResponse(200, tableResponse)); // doRefresh

    // caseSensitive defaults to false, but Spark 3.1 still matches the INSERT column list
    // case-sensitively: "id" does not match stored "ID" → AnalysisException.
    Assertions.assertThrows(
        Exception.class,
        () ->
            spark.sql(
                "INSERT INTO openhouse.dbCaseWrite.explicit (id, value) VALUES ('row1', 'v1')"),
        "Spark 3.1 INSERT column list is case-sensitive: 'id' must not match stored 'ID'");
  }

  /**
   * Writes fail with an explicit (differently-cased) column list when {@code
   * spark.sql.caseSensitive=true}, confirming that the failure observed in {@link
   * #testExplicitColumnInsert_failsEvenWithDefaultCaseSensitivity()} is not made worse by enabling
   * case sensitivity — both modes fail for the same reason.
   *
   * <p>With case-sensitive resolution, Spark cannot match {@code id} to the stored column {@code
   * ID} and throws {@code AnalysisException} on the client before the request reaches the server.
   * This gap exists regardless of any server-side normalization.
   */
  @Test
  public void testExplicitColumnInsert_failsWhenCaseSensitiveEnabled() {
    TableIdentifier tableId = TableIdentifier.of("dbCaseWrite", "caseSensitive");
    Object tableResponse =
        mockGetTableResponseBody(
            "dbCaseWrite",
            "caseSensitive",
            "c1",
            "dbCaseWrite.caseSensitive",
            "UUID3",
            mockTableLocation(tableId, "ID string, value string", ""),
            "v1",
            SCHEMA_UPPERCASE_ID,
            null,
            null);

    // The table must be loadable (ResolveRelations runs before ResolveReferences),
    // but analysis fails before the write is submitted so no doCommit response is needed.
    mockTableService.enqueue(mockResponse(200, tableResponse)); // doRefresh (ResolveRelations)

    spark.conf().set("spark.sql.caseSensitive", "true");
    try {
      Assertions.assertThrows(
          Exception.class,
          () ->
              spark.sql(
                  "INSERT INTO openhouse.dbCaseWrite.caseSensitive (id, value)"
                      + " VALUES ('row1', 'v1')"),
          "With caseSensitive=true, 'id' cannot resolve against stored column 'ID'");
    } finally {
      spark.conf().set("spark.sql.caseSensitive", "false");
    }
  }

  /**
   * {@code df.writeTo().append()} succeeds when DataFrame column names differ in casing from the
   * stored schema (the reviewer's scenario: lowercase {@code test} and {@code TEST} against stored
   * {@code TeSt}).
   *
   * <p>Why it works: with {@code spark.sql.caseSensitive=false} (the default) Spark's plan analysis
   * maps each DataFrame column to the Iceberg table column case-insensitively. The data files are
   * written using the stored column names. No schema evolution occurs, so the commit carries the
   * existing schema unchanged — {@code writeSchema.sameSchema(tableSchema)} is {@code true} and the
   * server's {@code validateWriteSchema} is never invoked.
   */
  @Test
  public void testDataFrameWriteTo_succeedsWithLowercasedColumns() {
    TableIdentifier tableId = TableIdentifier.of("dbCaseWrite", "dfWriteLower");
    Object tableResponse =
        mockGetTableResponseBody(
            "dbCaseWrite",
            "dfWriteLower",
            "c1",
            "dbCaseWrite.dfWriteLower",
            "UUID4",
            mockTableLocation(tableId, "TeSt string, id bigint", ""),
            "v1",
            SCHEMA_MIXED_CASE,
            null,
            null);
    Object tableAfterWrite =
        mockGetTableResponseBody(
            "dbCaseWrite",
            "dfWriteLower",
            "c1",
            "dbCaseWrite.dfWriteLower",
            "UUID4",
            mockTableLocationAfterOperation(tableId, "INSERT INTO %t (TeSt, id) VALUES ('foo', 1)"),
            "v2",
            SCHEMA_MIXED_CASE,
            null,
            null);

    mockTableService.enqueue(mockResponse(200, tableResponse)); // doRefresh
    mockTableService.enqueue(mockResponse(200, tableResponse)); // doRefresh
    mockTableService.enqueue(mockResponse(200, tableAfterWrite)); // putSnapshots (doCommit)
    mockTableService.enqueue(mockResponse(200, tableAfterWrite)); // doRefresh
    mockTableService.enqueue(mockResponse(200, tableAfterWrite)); // doRefresh

    // DataFrame with lowercase "test" — Spark maps it to stored "TeSt" (caseSensitive=false).
    StructType dfSchema =
        new StructType()
            .add("test", DataTypes.StringType, false)
            .add("id", DataTypes.LongType, false);
    Dataset<Row> df =
        spark.createDataFrame(Arrays.asList(RowFactory.create("TestValue2", 2L)), dfSchema);

    Assertions.assertDoesNotThrow(() -> df.writeTo("openhouse.dbCaseWrite.dfWriteLower").append());
  }

  /**
   * {@code df.writeTo().append()} succeeds when the DataFrame uses ALL-CAPS column names (the
   * reviewer's Variant B: {@code TEST} against stored {@code TeSt}).
   *
   * <p>Same mechanism as the lowercase variant: Spark resolves {@code TEST} → {@code TeSt}
   * case-insensitively; the commit carries the unchanged schema.
   */
  @Test
  public void testDataFrameWriteTo_succeedsWithUppercasedColumns() {
    TableIdentifier tableId = TableIdentifier.of("dbCaseWrite", "dfWriteUpper");
    Object tableResponse =
        mockGetTableResponseBody(
            "dbCaseWrite",
            "dfWriteUpper",
            "c1",
            "dbCaseWrite.dfWriteUpper",
            "UUID5",
            mockTableLocation(tableId, "TeSt string, id bigint", ""),
            "v1",
            SCHEMA_MIXED_CASE,
            null,
            null);
    Object tableAfterWrite =
        mockGetTableResponseBody(
            "dbCaseWrite",
            "dfWriteUpper",
            "c1",
            "dbCaseWrite.dfWriteUpper",
            "UUID5",
            mockTableLocationAfterOperation(tableId, "INSERT INTO %t (TeSt, id) VALUES ('foo', 3)"),
            "v2",
            SCHEMA_MIXED_CASE,
            null,
            null);

    mockTableService.enqueue(mockResponse(200, tableResponse)); // doRefresh
    mockTableService.enqueue(mockResponse(200, tableResponse)); // doRefresh
    mockTableService.enqueue(mockResponse(200, tableAfterWrite)); // putSnapshots (doCommit)
    mockTableService.enqueue(mockResponse(200, tableAfterWrite)); // doRefresh
    mockTableService.enqueue(mockResponse(200, tableAfterWrite)); // doRefresh

    // DataFrame with ALL-CAPS "TEST" — Spark maps it to stored "TeSt" (caseSensitive=false).
    StructType dfSchema =
        new StructType()
            .add("TEST", DataTypes.StringType, false)
            .add("id", DataTypes.LongType, false);
    Dataset<Row> df =
        spark.createDataFrame(Arrays.asList(RowFactory.create("TestValue3", 3L)), dfSchema);

    Assertions.assertDoesNotThrow(() -> df.writeTo("openhouse.dbCaseWrite.dfWriteUpper").append());
  }

  /**
   * {@code df.writeTo().append()} fails when {@code spark.sql.caseSensitive=true} and the DataFrame
   * column names differ in casing from the stored schema.
   *
   * <p>With case-sensitive resolution, Spark cannot match {@code test} or {@code TEST} to the
   * stored column {@code TeSt} and throws {@code AnalysisException} before the write reaches the
   * server. Both the lowercase and ALL-CAPS variants fail for the same reason.
   */
  @Test
  public void testDataFrameWriteTo_failsWhenCaseSensitiveEnabled() {
    TableIdentifier tableId = TableIdentifier.of("dbCaseWrite", "dfWriteCaseSensitive");
    Object tableResponse =
        mockGetTableResponseBody(
            "dbCaseWrite",
            "dfWriteCaseSensitive",
            "c1",
            "dbCaseWrite.dfWriteCaseSensitive",
            "UUID6",
            mockTableLocation(tableId, "TeSt string, id bigint", ""),
            "v1",
            SCHEMA_MIXED_CASE,
            null,
            null);

    // Two doRefresh responses for Variant A (analysis fails before commit).
    mockTableService.enqueue(mockResponse(200, tableResponse));
    mockTableService.enqueue(mockResponse(200, tableResponse));

    spark.conf().set("spark.sql.caseSensitive", "true");
    try {
      // Variant A: lowercase "test" vs stored "TeSt".
      StructType lowerSchema =
          new StructType()
              .add("test", DataTypes.StringType, false)
              .add("id", DataTypes.LongType, false);
      Dataset<Row> dfLower =
          spark.createDataFrame(Arrays.asList(RowFactory.create("TestValue2", 2L)), lowerSchema);
      Assertions.assertThrows(
          Exception.class,
          () -> dfLower.writeTo("openhouse.dbCaseWrite.dfWriteCaseSensitive").append(),
          "With caseSensitive=true, lowercase 'test' must not match stored 'TeSt'");

      // Two doRefresh responses for Variant B (analysis fails before commit).
      mockTableService.enqueue(mockResponse(200, tableResponse));
      mockTableService.enqueue(mockResponse(200, tableResponse));

      // Variant B: ALL-CAPS "TEST" vs stored "TeSt".
      StructType upperSchema =
          new StructType()
              .add("TEST", DataTypes.StringType, false)
              .add("id", DataTypes.LongType, false);
      Dataset<Row> dfUpper =
          spark.createDataFrame(Arrays.asList(RowFactory.create("TestValue3", 3L)), upperSchema);
      Assertions.assertThrows(
          Exception.class,
          () -> dfUpper.writeTo("openhouse.dbCaseWrite.dfWriteCaseSensitive").append(),
          "With caseSensitive=true, ALL-CAPS 'TEST' must not match stored 'TeSt'");
    } finally {
      spark.conf().set("spark.sql.caseSensitive", "false");
    }
  }
}
