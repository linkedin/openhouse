package com.linkedin.openhouse.integrationtests

import java.util.Collections
import org.apache.spark.sql.{AnalysisException, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, never, verify, when}
import org.mockito.invocation.InvocationOnMock

final class ScenarioDmlContractTest {
  private val columns = CoreTable.columnNames.mkString(", ")
  private val schema = StructType(Seq(
    StructField(CoreTable.long0.columnName, LongType),
    StructField(CoreTable.int0.columnName, IntegerType),
    StructField(CoreTable.string0.columnName, StringType),
    StructField(CoreTable.double0.columnName, DoubleType),
    StructField(CoreTable.boolean0.columnName, BooleanType),
    StructField(CoreTable.date0.columnName, StringType)))
  private val seedRows: Seq[Row] = (1 to 3).map(rowIndex =>
    new GenericRowWithSchema(
      Array[Any](rowIndex.toLong, rowIndex, s"row-$rowIndex", rowIndex + 0.5,
        rowIndex % 2 == 0, CoreTable.dateLiteral(rowIndex)),
      schema))
  private val nullStringRow = new GenericRowWithSchema(
    Array[Any](99L, 99, null, 99.5, false, "2024-01-01-00"),
    schema)

  @Test
  def nullDeleteRequiresExactlyOnePreparedNull(): Unit = {
    Seq(seedRows, seedRows ++ Seq(nullStringRow, nullStringRow)).foreach { preparedRows =>
      val table = mockTable(TableState(preparedRows, 1L), TableState(preparedRows, 2L))

      assertThrows(
        classOf[AssertionError],
        () => Scenarios.nullStringRowTestCases.head.run(table))
      verify(table.spark, never()).sql(
        "DELETE FROM test.table WHERE foo_col_string IS NULL")
    }
  }

  @Test
  def nullDeleteAcceptsOnlyRemovalOfThePreparedNull(): Unit = {
    val table = mockTable(
      TableState(seedRows :+ nullStringRow, 1L),
      TableState(seedRows, 2L))

    Scenarios.nullStringRowTestCases.head.run(table)

    verify(table.spark).sql("DELETE FROM test.table WHERE foo_col_string IS NULL")
  }

  @Test
  def partialInsertRequiresTheMissingIntegerColumnAnalysisError(): Unit = {
    val statement =
      "INSERT INTO test.table (foo_col_long, foo_col_string) " +
        "VALUES (CAST(4 AS BIGINT), 'd'), (CAST(5 AS BIGINT), 'e')"
    val expectedFailure = mock(classOf[AnalysisException])
    when(expectedFailure.getErrorClass).thenReturn("INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA")
    when(expectedFailure.getMessageParameters)
      .thenReturn(Collections.singletonMap("colName", "`foo_col_int`"))
    when(expectedFailure.getMessage).thenReturn("Cannot find data for the output column `foo_col_int`")
    val table = mockTable(TableState(seedRows, 1L), TableState(seedRows, 1L))
    when(table.spark.sql(statement)).thenAnswer((_: InvocationOnMock) => throw expectedFailure)

    Scenarios.allDmlTestCases.find(_.id == "insert.explicitColumns").get.run(table)

    val castFailure = mock(classOf[AnalysisException])
    when(castFailure.getErrorClass).thenReturn("INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST")
    val wrongColumnFailure = mock(classOf[AnalysisException])
    when(wrongColumnFailure.getErrorClass).thenReturn("INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA")
    when(wrongColumnFailure.getMessageParameters)
      .thenReturn(Collections.singletonMap("colName", "`foo_col_string`"))
    Seq(castFailure, wrongColumnFailure, new IllegalStateException(expectedFailure.getMessage))
      .foreach { unrelatedFailure =>
        val rejectedTable = mockTable(TableState(seedRows, 1L), TableState(seedRows, 1L))
        when(rejectedTable.spark.sql(statement)).thenAnswer((_: InvocationOnMock) => throw unrelatedFailure)

        assertThrows(
          classOf[AssertionError],
          () => Scenarios.allDmlTestCases.find(_.id == "insert.explicitColumns").get.run(rejectedTable))
      }
  }

  @Test
  def partitionMoveReadsBothSourceAndDestinationDates(): Unit = {
    val movedRow = Row.fromSeq(seedRows(1).toSeq.updated(5, "2099-12-31-23"))
    val afterRows = seedRows.updated(1, movedRow)
    val sourceQuery =
      s"SELECT $columns FROM test.table WHERE foo_col_date = '2024-01-01-01' ORDER BY foo_col_long"
    val destinationQuery =
      s"SELECT $columns FROM test.table WHERE foo_col_date = '2099-12-31-23' ORDER BY foo_col_long"

    Seq(
      (Seq.empty[Row], Seq(movedRow), true),
      (Seq(seedRows(1)), Seq(movedRow), false),
      (Seq.empty[Row], Seq.empty[Row], false)).foreach {
      case (sourceRows, destinationRows, accepted) =>
        val table = mockTable(TableState(seedRows, 1L), TableState(afterRows, 2L))
        val sourceResult = mock(classOf[Dataset[Row]])
        val destinationResult = mock(classOf[Dataset[Row]])
        when(table.spark.sql(sourceQuery)).thenReturn(sourceResult)
        when(sourceResult.collect()).thenReturn(sourceRows.toArray)
        when(table.spark.sql(destinationQuery)).thenReturn(destinationResult)
        when(destinationResult.collect()).thenReturn(destinationRows.toArray)

        if (accepted) {
          Scenarios.allDmlTestCases.find(_.id == "update.movePartition").get.run(table)
          verify(table.spark).sql(sourceQuery)
          verify(table.spark).sql(destinationQuery)
        } else {
          assertThrows(
            classOf[AssertionError],
            () => Scenarios.allDmlTestCases.find(_.id == "update.movePartition").get.run(table))
        }
    }
  }

  @Test
  def partitionMoveUsesDatePartitionedPreparationsWithoutAddingCases(): Unit = {
    assertEquals(96, Scenarios.dmlOperationCases.size)
    assertEquals(108, Scenarios.dmlCoreCases.size + Scenarios.dmlOperationCases.size)
    assertEquals(
      List(
        "update.movePartition @ partitioned/parquet",
        "update.movePartition @ partitioned/orc"),
      Scenarios.dmlOperationCases.filter(_.id.startsWith("update.movePartition @")).map(_.id))
    Scenarios.preparedPartitionedCoreTables.foreach { preparation =>
      val spark = mock(classOf[SparkSession])
      preparation.preparation.steps.head.execute(spark, "test.table", CoreTable)
      verify(spark).sql(
        s"CREATE TABLE test.table (foo_col_long bigint, foo_col_int int, foo_col_string string, " +
          s"foo_col_double double, foo_col_boolean boolean, foo_col_date string) USING ${Scenarios.dataSource} " +
          s"PARTITIONED BY (foo_col_date) TBLPROPERTIES ('write.format.default'='${preparation.label.split("/").last}')")
    }
  }

  private def mockTable(before: TableState, after: TableState): PreparedTable[CoreTable.type] = {
    val spark = mock(classOf[SparkSession])
    val rows = mock(classOf[Dataset[Row]])
    val snapshots = mock(classOf[Dataset[Row]])
    when(spark.sql(s"SELECT $columns FROM test.table ORDER BY foo_col_long")).thenReturn(rows)
    when(rows.collect()).thenReturn(before.rows.toArray, after.rows.toArray)
    when(spark.sql("SELECT count(*) FROM test.table.snapshots")).thenReturn(snapshots)
    when(snapshots.collect()).thenReturn(Array(Row(before.snapshotCount)), Array(Row(after.snapshotCount)))
    PreparedTable(spark, "test.table", CoreTable, before.rows, before.snapshotCount)
  }
}
