package harness

import org.apache.spark.sql.AnalysisException

/**
 * DML rejection: the DML statements the analyzer and the row-level rewrite refuse, and the message each rejection
 * carries.
 *
 * Operations: DELETE on a column the table does not declare, DELETE and UPDATE with a nondeterministic predicate,
 * INSERT INTO with fewer values than the table has columns, a MERGE whose UPDATE SET assigns one target column twice,
 * and a MERGE whose source matches one target row twice.
 *
 * Preparation axes: the standard seeded core table in each columnar format.
 *
 * Case families: six families contributing 12 cases.
 */
trait ScenarioDmlRejection extends TableTestFixtures {

  /** Every rejected-DML case, one file format at a time. */
  lazy val dmlRejectionCases: List[TestCase] =
    preparedCoreFormats.flatMap { preparation =>
      List(
        nonExistentColumnCase(preparation),
        nonDeterministicDeleteCase(preparation),
        nonDeterministicUpdateCase(preparation),
        insertArityCase(preparation),
        mergeConflictingUpdatesCase(preparation),
        mergeCardinalityViolationCase(preparation))
    }

  /** DELETE with a WHERE clause on a nonexistent column is rejected with an AnalysisException naming that column. */
  private def nonExistentColumnCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("dmlValidation.nonExistentColumn") { table =>
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE no_such_column = 1"))

      assert(exception.getMessage.contains("no_such_column"))
    }

  /**
   * DELETE with a nondeterministic WHERE clause (rand() < 0.5) is rejected with an AnalysisException about
   * determinism.
   */
  private def nonDeterministicDeleteCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("dmlValidation.nonDeterministicDelete") { table =>
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE rand() < 0.5"))

      assert(exception.getMessage.toLowerCase.contains("deterministic"))
    }

  /**
   * UPDATE with a nondeterministic WHERE clause (rand() < 0.5) is rejected with an AnalysisException about
   * determinism.
   */
  private def nonDeterministicUpdateCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("dmlValidation.nonDeterministicUpdate") { table =>
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'x' WHERE rand() < 0.5"))

      assert(exception.getMessage.toLowerCase.contains("deterministic"))
    }

  /**
   * INSERT INTO with too few values for the table's columns is rejected with an AnalysisException about the missing
   * data columns.
   */
  private def insertArityCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("dmlValidation.insertArity") { table =>
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES (CAST(1 AS BIGINT), 1)"))

      assert(exception.getMessage.toLowerCase.contains("not enough data columns"))
    }

  /**
   * A MERGE whose UPDATE SET assigns the same target column twice is rejected with an AnalysisException about multiple
   * assignments.
   */
  private def mergeConflictingUpdatesCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("dmlValidation.mergeConflictingUpdates") { table =>
      val keyColumn = Core.long0.columnName
      val stringColumn = Core.string0.columnName
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"""MERGE INTO ${table.name} target USING (
                    SELECT * FROM VALUES (CAST(2 AS BIGINT)) AS source($keyColumn)
                  ) source
                  ON target.$keyColumn = source.$keyColumn
                  WHEN MATCHED THEN UPDATE
                  SET target.$stringColumn = 'a', target.$stringColumn = 'b'"""))

      assert(exception.getMessage.contains("Multiple assignments"))
    }

  /**
   * A MERGE whose source has two rows matching the same target row fails with a cardinality-violation error naming the
   * multi-row match.
   */
  private def mergeCardinalityViolationCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("dmlValidation.mergeCardinalityViolation") { table =>
      val keyColumn = Core.long0.columnName
      val stringColumn = Core.string0.columnName
      val before = table.state
      val exception = Check.intercept[Exception](
        table.spark.sql(
          s"""MERGE INTO ${table.name} target USING (
                    SELECT * FROM VALUES
                      (CAST(2 AS BIGINT), 'a'),
                      (CAST(2 AS BIGINT), 'b')
                    AS source($keyColumn, $stringColumn)
                  ) source
                  ON target.$keyColumn = source.$keyColumn
                  WHEN MATCHED THEN UPDATE SET target.$stringColumn = source.$stringColumn"""))

      assert(
        Exceptions.causeChain(exception).exists { cause =>
          Option(cause.getMessage).exists(
            _.contains("matched a single row from the target table"))
        },
        s"expected a MERGE cardinality-violation message, got: ${exception.getMessage}")
      val after = table.state
      assert(after == before, s"rejected MERGE changed table state: before=$before, after=$after")
    }
}
