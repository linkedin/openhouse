package harness

/**
 * Namespaces: the catalog serves the databases it is configured with, and it rejects the statements that would create
 * or drop one, leaving the namespace set exactly as it found it.
 *
 * Operations: CREATE NAMESPACE for a fresh database name, and DROP NAMESPACE for the database the working table lives
 * in.
 *
 * Preparation axes: the standard seeded core table in each of the two columnar formats, which gives each case a live
 * catalog session, a table lifecycle, and a namespace to prove survives a rejected drop.
 *
 * Case families: two families contributing 4 cases.
 */
trait ScenarioNamespace extends ScenarioKit {

  /** Every namespace case, one file format at a time. */
  lazy val namespaceCases: List[TestCase] =
    fileFormats.flatMap { format =>
      List(
        createRejectedCase(preparedStandardTable(format)),
        dropRejectedCase(preparedStandardTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * CREATE NAMESPACE is rejected with an UnsupportedOperationException naming the unsupported operation, and the
   * catalog keeps serving the same databases: the candidate database the statement named stays absent from the
   * supported single-argument listing. The candidate name extends the table's generated unique suffix, so it cannot
   * collide with a database another case relies on.
   */
  private def createRejectedCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("namespace.create.rejected") { table =>
      val candidateDatabase = s"${table.name.split('.').last}_db"

      val exception = Check.intercept[UnsupportedOperationException](
        table.spark.sql(s"CREATE NAMESPACE openhouse.$candidateDatabase"))

      val servedDatabases =
        table.spark.sql("SHOW NAMESPACES IN openhouse").collect().toSeq.map(_.getString(0))

      assert(
        exception.getMessage.contains("not supported"),
        s"unexpected message: ${exception.getMessage.take(160)}")
      assert(
        !servedDatabases.contains(candidateDatabase),
        s"the rejected create should leave the served databases without $candidateDatabase")
    }

  /**
   * DROP NAMESPACE on the database the working table lives in is rejected with an UnsupportedOperationException naming
   * the unsupported operation, and the database keeps serving that table with all its seed rows, so a rejected drop
   * leaves the namespace and its contents intact.
   */
  private def dropRejectedCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("namespace.drop.rejected") { table =>
      val database = table.name.substring(0, table.name.lastIndexOf('.'))

      val exception = Check.intercept[UnsupportedOperationException](
        table.spark.sql(s"DROP NAMESPACE $database"))

      assert(
        exception.getMessage.contains("not supported"),
        s"unexpected message: ${exception.getMessage.take(160)}")
      assert(
        table.rows.size == standardSeedRowCount,
        s"the rejected drop must leave $database serving the table with its seed rows")
    }

}
