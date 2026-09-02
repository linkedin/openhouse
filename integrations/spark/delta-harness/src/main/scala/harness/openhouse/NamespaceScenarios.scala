package harness

/**
 * Namespaces: the catalog serves the databases it is configured with, and it rejects the statements that would create
 * or drop one.
 *
 * Operations: CREATE NAMESPACE and DROP NAMESPACE.
 *
 * Preparation axes: the standard seeded core table in each of the two columnar formats, which gives each case a live
 * catalog session and a table lifecycle.
 *
 * Case families: two families contributing 4 cases.
 */
trait NamespaceScenarios extends ScenarioKit {

  /** Every namespace case, one file format at a time. */
  lazy val namespaceCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        createRejectedCase(preparedStandardTable(format)),
        dropRejectedCase(preparedStandardTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** CREATE NAMESPACE is rejected with an UnsupportedOperationException naming the unsupported operation. */
  private def createRejectedCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("namespace.create.rejected") { table =>
      val exception = Check.intercept[UnsupportedOperationException](
        table.spark.sql("CREATE NAMESPACE openhouse.a_new_db"))

      assert(
        exception.getMessage.contains("not supported"),
        s"unexpected message: ${exception.getMessage.take(160)}")
    }

  /** DROP NAMESPACE is rejected with an UnsupportedOperationException naming the unsupported operation. */
  private def dropRejectedCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("namespace.drop.rejected") { table =>
      val exception = Check.intercept[UnsupportedOperationException](
        table.spark.sql("DROP NAMESPACE openhouse.dbMatrix"))

      assert(
        exception.getMessage.contains("not supported"),
        s"unexpected message: ${exception.getMessage.take(160)}")
    }

}
