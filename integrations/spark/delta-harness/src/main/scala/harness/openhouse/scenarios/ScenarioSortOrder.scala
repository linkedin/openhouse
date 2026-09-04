package harness

import org.apache.iceberg.{NullOrder, SortOrder}

/**
 * Sort order: ALTER TABLE WRITE ORDERED BY records an exact write sort order in the table's Iceberg metadata, giving it
 * a non-default order id, and a later write keeps that recorded order in place.
 *
 * Operations: WRITE ORDERED BY a single column, and WRITE ORDERED BY two columns with an explicit direction and null
 * ordering; each is read back from the persisted Iceberg sort order and re-read after an insert.
 *
 * The catalog's write path leaves each data file's sort-order id at the default, so the observable sort-order contract
 * is the persisted order the catalog stores and keeps across writes, asserted here against an exact Iceberg SortOrder
 * built with the intended fields, directions and null orderings.
 *
 * Preparation axes: the standard seeded core table in each of the two columnar formats.
 *
 * Case families: two families contributing 4 cases.
 */
trait ScenarioSortOrder extends CatalogDdlSupport {

  /** Every sort-order case, one file format at a time. */
  lazy val sortOrderCases: List[TestCase] =
    fileFormats.flatMap { format =>
      List(
        orderedByCase(preparedStandardTable(format)),
        orderedByMultipleColumnsCase(preparedStandardTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * ALTER TABLE WRITE ORDERED BY a single column records a sort order of that column ascending with nulls first and
   * gives it a non-default order id. A follow-up insert of two out-of-key-order rows grows the table to five rows and
   * leaves the recorded sort order unchanged.
   */
  private def orderedByCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("sortOrder.orderedBy") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} WRITE ORDERED BY ${Core.long0.columnName}")

      val orderedTable = icebergTableOf(table.spark, table.name)
      val expectedSortOrder =
        SortOrder.builderFor(orderedTable.schema()).asc(Core.long0.columnName).build()
      val sortOrderId = orderedTable.sortOrder().orderId()

      assert(
        orderedTable.sortOrder() == expectedSortOrder,
        s"expected sort order $expectedSortOrder, got ${orderedTable.sortOrder()}")
      assert(
        sortOrderId > 0,
        s"a write sort order should persist a non-default order id, got $sortOrderId")

      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES ${coreRow(5, "row-5")}, ${coreRow(4, "row-4")}")

      val reloadedTable = icebergTableOf(table.spark, table.name)
      assert(table.rows.size == 5, "the ordered write path should accept two more rows")
      assert(
        reloadedTable.sortOrder() == expectedSortOrder &&
          reloadedTable.sortOrder().orderId() == sortOrderId,
        s"an insert should preserve sort order $expectedSortOrder, got ${reloadedTable.sortOrder()}")
    }

  /**
   * ALTER TABLE WRITE ORDERED BY two columns records the string column descending with nulls first followed by the long
   * column ascending with nulls first and gives the order a non-default id. A follow-up insert of two out-of-key-order
   * rows grows the table to five rows and leaves the recorded sort order unchanged.
   */
  private def orderedByMultipleColumnsCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("sortOrder.orderedByMultipleColumns") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} WRITE ORDERED BY " +
          s"${Core.string0.columnName} DESC NULLS FIRST, ${Core.long0.columnName}")

      val orderedTable = icebergTableOf(table.spark, table.name)
      val expectedSortOrder = SortOrder
        .builderFor(orderedTable.schema())
        .desc(Core.string0.columnName, NullOrder.NULLS_FIRST)
        .asc(Core.long0.columnName)
        .build()
      val sortOrderId = orderedTable.sortOrder().orderId()

      assert(
        orderedTable.sortOrder() == expectedSortOrder,
        s"expected sort order $expectedSortOrder, got ${orderedTable.sortOrder()}")
      assert(
        sortOrderId > 0,
        s"a multi-column write sort order should persist a non-default order id, got $sortOrderId")

      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES ${coreRow(5, "row-5")}, ${coreRow(4, "row-4")}")

      val reloadedTable = icebergTableOf(table.spark, table.name)
      assert(table.rows.size == 5, "the multi-column ordered write path should accept two more rows")
      assert(
        reloadedTable.sortOrder() == expectedSortOrder &&
          reloadedTable.sortOrder().orderId() == sortOrderId,
        s"an insert should preserve sort order $expectedSortOrder, got ${reloadedTable.sortOrder()}")
    }

}
