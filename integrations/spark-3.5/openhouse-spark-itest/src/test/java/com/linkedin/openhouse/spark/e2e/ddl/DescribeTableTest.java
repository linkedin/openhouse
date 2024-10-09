package com.linkedin.openhouse.spark.e2e.ddl;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.openhouse.spark.SparkTestBase;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class DescribeTableTest {

  @Test
  public void testDescribeTable() {
    String schemaForDdl = convertSchemaToDDLComponent(baseSchema);
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetTableResponseBody(
                "dbDesc",
                "tb1",
                "c1",
                "dbDesc.tb1.c1",
                "UUID",
                mockTableLocation(TableIdentifier.of("dbDesc", "tb1"), schemaForDdl, ""),
                "v1",
                baseSchema,
                null,
                null)));

    Dataset<Row> rows = spark.sql("DESCRIBE TABLE openhouse.dbDesc.tb1");
    validateSchema(rows, baseSchema);
  }

  @Test
  public void testDescribeTableDoesNotExist() {
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody()));
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody()));

    AnalysisException ex =
        Assertions.assertThrows(
            AnalysisException.class,
            () -> spark.sql("DESCRIBE TABLE openhouse.dbDesc.tbNotExist").show());

    Assertions.assertEquals(
        "Table or view not found for 'DESCRIBE TABLE': openhouse.dbDesc.tbNotExist; line 1 pos 0;\n"
            + "'DescribeRelation false\n"
            + "+- 'UnresolvedTableOrView [openhouse, dbDesc, tbNotExist], DESCRIBE TABLE, true\n",
        ex.getMessage());
  }

  @Test
  public void testDescribeTableOnValidPartitionedTable() {
    for (String transform : ImmutableList.of("days", "months", "hours", "years")) {
      String tbName = "tbDescPartitioned" + transform;
      String transformedPartitioned = "$TRANSFORM(timestampCol)".replace("$TRANSFORM", transform);
      mockTableService.enqueue(
          mockResponse(
              201,
              mockGetTableResponseBody(
                  "dbDesc",
                  tbName,
                  "c1",
                  "dbDesc.tbpartitioned.c1",
                  "UUID",
                  mockTableLocation(
                      TableIdentifier.of("dbDesc", tbName),
                      convertSchemaToDDLComponent(baseSchema),
                      String.format("PARTITIONED BY (%s(timestampCol))", transform)),
                  "v1",
                  baseSchema,
                  null,
                  null))); // doRefresh()

      Dataset<Row> rows =
          spark.sql("DESCRIBE TABLE openhouse.dbDesc.$TB_NAME".replace("$TB_NAME", tbName));
      validateSchema(rows, baseSchema);
      validatePartitioning(rows, transformedPartitioned);
    }
  }

  /** Validating the collect rows contains expected partitioning. */
  private static void validatePartitioning(Dataset<Row> rows, String transformedPartitioned) {
    List<Row> rowsCollected = rows.collectAsList();
    Assertions.assertTrue(
        rowsCollected.contains(
            new GenericRowWithSchema(
                new String[] {"Part 0", transformedPartitioned, ""}, rows.schema())));
  }

  /** Validating the collect rows contains expected schema. */
  public static void validateSchema(Dataset<Row> rows, String expectedSchema) {
    List<Row> rowsCollected = rows.collectAsList();

    for (String[] fieldInArray : convertSchemaToFieldArray(expectedSchema)) {
      Assertions.assertTrue(
          rowsCollected.contains(new GenericRowWithSchema(fieldInArray, rows.schema())));
    }
  }
}
