package harness

import org.apache.iceberg.exceptions.BadRequestException

/**
 * Table properties: which properties a table keeps as written, which ones the catalog owns and overrides, and which
 * ones it refuses to change.
 *
 * Operations: SET and UNSET TBLPROPERTIES for a user property; SET TBLPROPERTIES on the reserved openhouse.tableUUID
 * property; reading format-version back from a table created with format-version=1; reading
 * write.metadata.previous-versions-max back from a table that requested 7; reading write.target-file-size-bytes back
 * from a table that requested 1048576; and SET TBLPROPERTIES on openhouse.tableType.
 *
 * Preparation axes: in each of the two columnar formats, the standard seeded core table for the two families that
 * change properties after creation, plus one purpose-built table per family that asserts a property requested at
 * creation.
 *
 * Case families: six families contributing 12 cases.
 */
trait TablePropertyScenarios extends ScenarioKit {

  /** Every table-property case, one file format at a time. */
  lazy val tablePropertyCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        userRoundTripCase(preparedStandardTable(format)),
        reservedPropertyRejectedCase(preparedStandardTable(format)),
        tableTypeImmutableCase(preparedStandardTable(format)),
        formatVersionForcedCase(format),
        previousVersionsHonoredCase(format),
        targetFileSizeCase(format))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** SET TBLPROPERTIES adds a user property that reads back, and UNSET TBLPROPERTIES removes it. */
  private def userRoundTripCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("tableProperty.userRoundTrip") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('my_key'='my_val')")
      assert(
        tableProps(table.spark, table.name).get("my_key").contains("my_val"),
        "user property was not set")

      table.spark.sql(s"ALTER TABLE ${table.name} UNSET TBLPROPERTIES ('my_key')")
      assert(
        !tableProps(table.spark, table.name).contains("my_key"),
        "user property was not removed")
    }

  /**
   * SET TBLPROPERTIES on the reserved openhouse.tableUUID property is rejected with a BadRequestException about the
   * restriction.
   */
  private def reservedPropertyRejectedCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("tableProperty.reservedOpenhouse.rejected") { table =>
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES (" +
            "'openhouse.tableUUID'='deadbeef')"))

      assert(
        exception.getMessage.toLowerCase.contains("restriction"),
        s"unexpected message: ${exception.getMessage.take(200)}")
    }

  /**
   * ALTER TABLE SET TBLPROPERTIES ('openhouse.tableType'='REPLICA_TABLE') is rejected with a BadRequestException,
   * since the table type is fixed at creation.
   */
  private def tableTypeImmutableCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("tableProperty.tableTypeImmutable") { table =>
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES (" +
            "'openhouse.tableType'='REPLICA_TABLE')"))

      assert(
        exception.getMessage.contains("restriction"),
        s"unexpected message: ${exception.getMessage.take(160)}")
    }

  /**
   * Even though format-version=1 was requested at creation, the catalog stores the table at format-version=2 and the
   * table remains writable there.
   */
  private def formatVersionForcedCase(format: String): Plan.Case =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
            s"'write.format.default'='$format', 'format-version'='1')")()
        .insert(standardSeedRowCount)())
      .test("tableProperty.formatVersionForced") { table =>
        val formatVersion = tableProps(table.spark, table.name).get("format-version")

        assert(
          formatVersion.contains("2"),
          s"expected the catalog to store format-version=2, got $formatVersion")
        assert(
          table.rows.size == standardSeedRowCount,
          "table not writable at the stored format-version")
      }

  /** The write.metadata.previous-versions-max property requested at creation is honored and reads back as 7. */
  private def previousVersionsHonoredCase(format: String): Plan.Case =
    TablePreparation(
      format,
      TableTest(Core).sql("create")(table =>
        s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
          s"'write.format.default'='$format', 'write.metadata.previous-versions-max'='7')")())
      .test("tableProperty.previousVersionsHonored") { table =>
        val previousVersions =
          tableProps(table.spark, table.name).get("write.metadata.previous-versions-max")

        assert(
          previousVersions.contains("7"),
          s"expected previous-versions-max=7, got $previousVersions")
      }

  /**
   * The write.target-file-size-bytes=1048576 property requested at creation is retained and the table holds its 3 seed
   * rows.
   */
  private def targetFileSizeCase(format: String): Plan.Case =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
            s"'write.format.default'='$format', 'write.target-file-size-bytes'='1048576')")()
        .insert(standardSeedRowCount)())
      .test("tableProperty.targetFileSize") { table =>
        assert(
          tableProps(table.spark, table.name)
            .get("write.target-file-size-bytes")
            .contains("1048576"),
          "target file size should be retained")
        assert(
          table.rows.size == standardSeedRowCount,
          "the custom target-size table should hold its seed rows")
      }

}
