package harness

import org.apache.iceberg.exceptions.BadRequestException

/**
 * Access control: the SET POLICY statements that govern how a table may be shared, retained and replicated, and the
 * GRANT and REVOKE statements that decide who may read it.
 *
 * Operations: SET POLICY (SHARING), SET POLICY (HISTORY), SET POLICY (REPLICATION) followed by UNSET POLICY
 * (REPLICATION), SET POLICY (RETENTION) on the date column, the out-of-range SET POLICY (HISTORY MAX_AGE) and SET
 * POLICY (HISTORY VERSIONS) forms, GRANT SELECT on an unshared table, and GRANT then REVOKE SELECT on a shared table
 * with SHOW GRANTS in between.
 *
 * Preparation axes: the standard seeded core table in each of the two columnar formats, except the retention family,
 * which starts from a date-partitioned core table seeded with the standard rows because RETENTION names a partition
 * column.
 *
 * Case families: eight families contributing 16 cases.
 */
trait ScenarioAccessControl extends ScenarioKit {

  /** Every access-control case, one file format at a time. */
  lazy val accessControlCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        policySharingCase(preparedStandardTable(format)),
        policyHistoryCase(preparedStandardTable(format)),
        policyReplicationCase(preparedStandardTable(format)),
        policyRetentionCase(format),
        policyHistoryMaxAgeRejectedCase(preparedStandardTable(format)),
        policyHistoryVersionsRejectedCase(preparedStandardTable(format)),
        grantUnsharedRejectedCase(preparedStandardTable(format)),
        grantAndRevokeCase(preparedStandardTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** SET POLICY (SHARING=TRUE) records the sharing policy and the table remains queryable. */
  private def policySharingCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("accessControl.policy.sharing") { table =>
      table.spark.sql(s"ALTER TABLE ${table.name} SET POLICY (SHARING=TRUE)")

      val policies = tableProps(table.spark, table.name).getOrElse("policies", "")

      assert(
        policies.toLowerCase.contains("true") || policies.toLowerCase.contains("sharing"),
        s"sharing policy not stored: $policies")
      assert(
        table.rows.size == standardSeedRowCount,
        "table not queryable after SET POLICY (SHARING)")
    }

  /** SET POLICY (HISTORY MAX_AGE=2D VERSIONS=20) records the history policy and the table remains queryable. */
  private def policyHistoryCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("accessControl.policy.history") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} SET POLICY (HISTORY MAX_AGE=2D VERSIONS=20)")

      val policies = tableProps(table.spark, table.name).getOrElse("policies", "")

      assert(
        policies.contains("20") || policies.toLowerCase.contains("history"),
        s"history policy not stored: $policies")
      assert(
        table.rows.size == standardSeedRowCount,
        "table not queryable after SET POLICY (HISTORY)")
    }

  /**
   * SET POLICY (REPLICATION) followed by UNSET POLICY (REPLICATION) leaves the table queryable with its 3 rows intact.
   */
  private def policyReplicationCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("accessControl.policy.replication") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} SET POLICY (REPLICATION = ({destination:'WAR'}))")
      table.spark.sql(
        s"ALTER TABLE ${table.name} UNSET POLICY (REPLICATION)")

      assert(table.rows.size == standardSeedRowCount)
    }

  /**
   * SET POLICY (RETENTION = 30d ON COLUMN foo_col_date ...) records the retention policy and the table remains
   * queryable.
   */
  private def policyRetentionCase(format: String): Plan.Case =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"PARTITIONED BY (${Core.date0.columnName}) " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(standardSeedRowCount)())
      .test("accessControl.policy.retention") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET POLICY (" +
            s"RETENTION = 30d ON COLUMN ${Core.date0.columnName} WHERE pattern = 'yyyy-MM-dd-HH')")

        val policies = tableProps(table.spark, table.name).getOrElse("policies", "")

        assert(
          policies.toLowerCase.contains("retention") || policies.contains("30"),
          s"retention policy not stored: $policies")
        assert(
          table.rows.size == standardSeedRowCount,
          "table not queryable after SET POLICY (RETENTION)")
      }

  /**
   * SET POLICY (HISTORY MAX_AGE=5D) exceeds the allowed range and is rejected with a BadRequestException stating the
   * 1-to-3-day limit.
   */
  private def policyHistoryMaxAgeRejectedCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("accessControl.policy.history.maxAge.rejected") { table =>
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET POLICY (HISTORY MAX_AGE=5D)"))

      assert(
        exception.getMessage.contains("max age must be between 1 to 3 days"),
        s"unexpected message: ${exception.getMessage.take(160)}")
    }

  /**
   * SET POLICY (HISTORY VERSIONS=200) exceeds the allowed range and is rejected with a BadRequestException stating the
   * 2-to-100-version limit.
   */
  private def policyHistoryVersionsRejectedCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("accessControl.policy.history.versions.rejected") { table =>
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET POLICY (HISTORY VERSIONS=200)"))

      assert(
        exception.getMessage.contains("must be between 2 to 100 versions"),
        s"unexpected message: ${exception.getMessage.take(160)}")
    }

  /**
   * GRANT SELECT on a table that is not marked shared is rejected with an IllegalArgumentException stating the table
   * is not shared.
   */
  private def grantUnsharedRejectedCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("accessControl.grantUnshared.rejected") { table =>
      val exception = Check.intercept[IllegalArgumentException](
        table.spark.sql(s"GRANT SELECT ON TABLE ${table.name} TO PUBLIC"))

      assert(
        exception.getMessage.contains("is not a shared table"),
        s"unexpected message: ${exception.getMessage.take(160)}")
    }

  /**
   * On a shared table, GRANT SELECT TO PUBLIC makes SHOW GRANTS list SELECT for PUBLIC and the table stays queryable;
   * REVOKE SELECT then removes that grant from SHOW GRANTS.
   */
  private def grantAndRevokeCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation
      .test("accessControl.grantAndRevoke") { table =>
        table.spark.sql(s"ALTER TABLE ${table.name} SET POLICY (SHARING=TRUE)")
        table.spark.sql(s"GRANT SELECT ON TABLE ${table.name} TO PUBLIC")

        val grantsAfterGrant = table.spark
          .sql(s"SHOW GRANTS ON TABLE ${table.name}")
          .collect()
          .map(row => (row.getString(0), row.getString(1)))
          .toSet
        assert(
          grantsAfterGrant.contains(("SELECT", "PUBLIC")),
          s"SHOW GRANTS did not include SELECT for PUBLIC: $grantsAfterGrant")
        assert(
          table.rows.size == standardSeedRowCount,
          "the shared and granted table should stay queryable")

        table.spark.sql(s"REVOKE SELECT ON TABLE ${table.name} FROM PUBLIC")
        val grantsAfterRevoke = table.spark
          .sql(s"SHOW GRANTS ON TABLE ${table.name}")
          .collect()
          .map(row => (row.getString(0), row.getString(1)))
          .toSet
        assert(
          !grantsAfterRevoke.contains(("SELECT", "PUBLIC")),
          s"SHOW GRANTS retained SELECT for PUBLIC: $grantsAfterRevoke")
      }
      .copy(embeddedSkipReason = Some(
        "The embedded test server has no OPA endpoint configured, so grantRole and " +
          "listAclPolicies are no-ops that always report an empty ACL list. GRANT and REVOKE " +
          "succeed without error, while SHOW GRANTS always returns an empty ACL list. The " +
          "li-openhouse acceptance environment runs the assertions against its configured " +
          "authorization service."))

}
