package harness

/**
 * Sort order: ALTER TABLE WRITE ORDERED BY records a write sort order on the table, which the catalog pairs with range
 * distribution, and the table keeps accepting writes under it.
 *
 * Operations: WRITE ORDERED BY a single column, and WRITE ORDERED BY two columns with an explicit direction and null
 * ordering followed by an insert.
 *
 * Preparation axes: the standard seeded core table in each of the two columnar formats.
 *
 * Case families: two families contributing 4 cases.
 */
trait ScenarioSortOrder extends ScenarioKit {

  /** Every sort-order case, one file format at a time. */
  lazy val sortOrderCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        orderedByCase(preparedStandardTable(format)),
        orderedByMultipleColumnsCase(preparedStandardTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** ALTER TABLE WRITE ORDERED BY a single column sets write.distribution-mode to range. */
  private def orderedByCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("sortOrder.orderedBy") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} WRITE ORDERED BY ${Core.long0.columnName}")

      val distributionMode =
        tableProps(table.spark, table.name).get("write.distribution-mode")

      assert(
        distributionMode.contains("range"),
        s"a write sort order should set range distribution, got $distributionMode")
    }

  /**
   * ALTER TABLE WRITE ORDERED BY multiple columns sets range distribution and the table remains writable, growing from
   * 3 to 5 rows after a follow-up insert.
   */
  private def orderedByMultipleColumnsCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("sortOrder.orderedByMultipleColumns") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} WRITE ORDERED BY " +
          s"${Core.string0.columnName} DESC NULLS FIRST, ${Core.long0.columnName}")

      assert(
        tableProps(table.spark, table.name).get("write.distribution-mode").contains("range"),
        "a multi-column write sort order should set range distribution")

      table.spark.sql(
        s"INSERT INTO ${table.name} ${RowGenerator.valuesClause(Core, 2)}")

      assert(table.rows.size == 5, "the multi-column ordered write path should accept two rows")
    }

}
