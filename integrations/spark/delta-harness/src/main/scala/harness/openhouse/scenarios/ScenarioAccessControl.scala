package harness

import com.google.gson.{JsonArray, JsonElement, JsonObject, JsonParser, JsonPrimitive}
import org.apache.iceberg.exceptions.BadRequestException
import java.math.{BigDecimal => JavaBigDecimal}
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Access control: table policies and SQL grants that govern sharing, retention, history, and replication.
 *
 * Operations: SET POLICY for sharing, history, replication and retention; UNSET POLICY for replication; rejected
 * out-of-range history policies; rejected grants on an unshared table; and the full grant, show, revoke cycle on a
 * shared table.
 *
 * Preparation axes: the standard seeded core table in each columnar format. The retention family uses a
 * date-partitioned table in each format because the policy names the partition column.
 *
 * Case families: eight families contributing 16 cases.
 */
trait ScenarioAccessControl extends ScenarioKit {

  /** Every access-control case, one file format at a time. */
  lazy val accessControlCases: List[TestCase] =
    fileFormats.flatMap { format =>
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

  /** SET POLICY (SHARING=TRUE) records sharingEnabled=true and leaves the table's rows unchanged. */
  private def policySharingCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("accessControl.policy.sharing") { table =>
      table.spark.sql(s"ALTER TABLE ${table.name} SET POLICY (SHARING=TRUE)")

      val policies = policiesFor(table)

      assert(
        GovernancePolicies.booleanField(policies, "sharingEnabled").contains(true),
        s"sharingEnabled should be true after SET POLICY (SHARING): $policies")
      assertRowsPreserved(table, "SET POLICY (SHARING)")
    }

  /** SET POLICY (HISTORY MAX_AGE=2D VERSIONS=20) records the exact history policy and preserves rows. */
  private def policyHistoryCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("accessControl.policy.history") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} SET POLICY (HISTORY MAX_AGE=2D VERSIONS=20)")

      val policies = policiesFor(table)
      val historyPolicy = GovernancePolicies.objectField(policies, "history")

      assert(
        historyPolicy.exists(history =>
          GovernancePolicies.intField(history, "maxAge").contains(2) &&
            GovernancePolicies.stringField(history, "granularity").contains("DAY") &&
            GovernancePolicies.intField(history, "versions").contains(20)),
        s"history policy should be exactly maxAge=2, granularity=DAY and versions=20: $policies")
      assertRowsPreserved(table, "SET POLICY (HISTORY)")
    }

  /** SET POLICY (REPLICATION) stores destination WAR, and UNSET POLICY leaves an empty replication config. */
  private def policyReplicationCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("accessControl.policy.replication") { table =>
      val expectedDestination = "'WAR'"
      table.spark.sql(
        s"ALTER TABLE ${table.name} SET POLICY (REPLICATION = ({destination:'WAR'}))")
      val policiesAfterSet = policiesFor(table)
      val replicationConfigAfterSet =
        GovernancePolicies
          .objectField(policiesAfterSet, "replication")
          .flatMap(GovernancePolicies.arrayField(_, "config"))
          .map(_.iterator().asScala.toList)
          .getOrElse(List.empty)

      assert(
        replicationConfigAfterSet.size == 1 &&
          replicationConfigAfterSet.headOption.exists(replicationConfig =>
            replicationConfig.isJsonObject &&
              GovernancePolicies
                .stringField(replicationConfig.getAsJsonObject, "destination")
                .contains(expectedDestination)),
        s"replication policy should contain exactly destination $expectedDestination after SET: $policiesAfterSet")
      assertRowsPreserved(table, "SET POLICY (REPLICATION)")

      table.spark.sql(
        s"ALTER TABLE ${table.name} UNSET POLICY (REPLICATION)")
      val policiesAfterUnset = policiesFor(table)
      val replicationConfigAfterUnset =
        GovernancePolicies
          .objectField(policiesAfterUnset, "replication")
          .flatMap(GovernancePolicies.arrayField(_, "config"))

      assert(
        replicationConfigAfterUnset.exists(_.size == 0),
        s"replication policy should have an empty config after UNSET POLICY (REPLICATION): $policiesAfterUnset")
      assertRowsPreserved(table, "UNSET POLICY (REPLICATION)")
    }

  /** SET POLICY (RETENTION = 30d ON COLUMN foo_col_date ...) records the exact retention policy. */
  private def policyRetentionCase(format: String): TestCase =
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

        val policies = policiesFor(table)
        val retentionPolicy = GovernancePolicies.objectField(policies, "retention")

        assert(
          retentionPolicy.exists(retention =>
            GovernancePolicies.intField(retention, "count").contains(30) &&
              GovernancePolicies.stringField(retention, "granularity").contains("DAY") &&
              GovernancePolicies.objectField(retention, "columnPattern").exists(columnPattern =>
                GovernancePolicies.stringField(columnPattern, "columnName").contains(Core.date0.columnName) &&
                  GovernancePolicies.stringField(columnPattern, "pattern").contains("'yyyy-MM-dd-HH'"))),
          s"retention policy should be exactly 30 DAY on ${Core.date0.columnName}: $policies")
        assertRowsPreserved(table, "SET POLICY (RETENTION)")
      }

  /** SET POLICY (HISTORY MAX_AGE=5D) is rejected and leaves the history policy absent. */
  private def policyHistoryMaxAgeRejectedCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("accessControl.policy.history.maxAge.rejected") { table =>
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET POLICY (HISTORY MAX_AGE=5D)"))

      assert(
        exception.getMessage.contains("max age must be between 1 to 3 days"),
        s"unexpected message: ${exception.getMessage.take(160)}")
      assert(
        GovernancePolicies.isAbsent(policiesFor(table), "history"),
        s"rejected SET POLICY (HISTORY MAX_AGE=5D) should leave history absent: ${policiesFor(table)}")
      assertRowsPreserved(table, "rejected SET POLICY (HISTORY MAX_AGE=5D)")
    }

  /** SET POLICY (HISTORY VERSIONS=200) is rejected and leaves the history policy absent. */
  private def policyHistoryVersionsRejectedCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("accessControl.policy.history.versions.rejected") { table =>
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET POLICY (HISTORY VERSIONS=200)"))

      assert(
        exception.getMessage.contains("must be between 2 to 100 versions"),
        s"unexpected message: ${exception.getMessage.take(160)}")
      assert(
        GovernancePolicies.isAbsent(policiesFor(table), "history"),
        s"rejected SET POLICY (HISTORY VERSIONS=200) should leave history absent: ${policiesFor(table)}")
      assertRowsPreserved(table, "rejected SET POLICY (HISTORY VERSIONS=200)")
    }

  /** GRANT SELECT on an unshared table is rejected with the catalog's shared-table requirement. */
  private def grantUnsharedRejectedCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("accessControl.grantUnshared.rejected") { table =>
      val exception = Check.intercept[IllegalArgumentException](
        table.spark.sql(s"GRANT SELECT ON TABLE ${table.name} TO PUBLIC"))

      assert(
        exception.getMessage.contains("is not a shared table"),
        s"unexpected message: ${exception.getMessage.take(160)}")
      assert(
        GovernancePolicies.isAbsent(policiesFor(table), "sharingEnabled"),
        s"rejected GRANT SELECT should leave sharing metadata absent: ${policiesFor(table)}")
      assertRowsPreserved(table, "rejected GRANT SELECT")
    }

  /** On a shared table, GRANT lists SELECT for PUBLIC and REVOKE removes that grant. */
  private def grantAndRevokeCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation
      .test("accessControl.grantAndRevoke") { table =>
        table.spark.sql(s"ALTER TABLE ${table.name} SET POLICY (SHARING=TRUE)")
        assert(
          GovernancePolicies.booleanField(policiesFor(table), "sharingEnabled").contains(true),
          s"sharingEnabled should be true before granting access: ${policiesFor(table)}")

        table.spark.sql(s"GRANT SELECT ON TABLE ${table.name} TO PUBLIC")

        val grantsAfterGrant = table.spark
          .sql(s"SHOW GRANTS ON TABLE ${table.name}")
          .collect()
          .map(row => (row.getString(0), row.getString(1)))
          .toSet
        assert(
          grantsAfterGrant.contains(("SELECT", "PUBLIC")),
          s"SHOW GRANTS should include SELECT for PUBLIC: $grantsAfterGrant")
        assertRowsPreserved(table, "GRANT SELECT")

        table.spark.sql(s"REVOKE SELECT ON TABLE ${table.name} FROM PUBLIC")
        val grantsAfterRevoke = table.spark
          .sql(s"SHOW GRANTS ON TABLE ${table.name}")
          .collect()
          .map(row => (row.getString(0), row.getString(1)))
          .toSet
        assert(
          !grantsAfterRevoke.contains(("SELECT", "PUBLIC")),
          s"SHOW GRANTS should remove SELECT for PUBLIC: $grantsAfterRevoke")
        assertRowsPreserved(table, "REVOKE SELECT")
      }
      .copy(embeddedSkipReason = Some(
        "The embedded test server has no OPA endpoint configured, so grantRole and " +
          "listAclPolicies are no-ops that always report an empty ACL list. GRANT and REVOKE " +
          "succeed without error, while SHOW GRANTS always returns an empty ACL list. The " +
          "li-openhouse acceptance environment runs the assertions against its configured " +
          "authorization service."))

  private def policiesFor(table: PreparedTable[CoreTable.type]): JsonObject =
    GovernancePolicies.parse(tableProps(table.spark, table.name))

  private def assertRowsPreserved(table: PreparedTable[CoreTable.type], operation: String): Unit =
    assert(
      table.rows == table.preparedRows,
      s"$operation should preserve the prepared rows; expected ${table.preparedRows}, got ${table.rows}")

}

private[harness] object GovernancePolicies {
  def parse(properties: Map[String, String]): JsonObject =
    properties
      .get("policies")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(policyJson => JsonParser.parseString(policyJson).getAsJsonObject)
      .getOrElse(new JsonObject)

  def isAbsent(document: JsonObject, fieldName: String): Boolean =
    !document.has(fieldName)

  def objectField(document: JsonObject, fieldName: String): Option[JsonObject] =
    fieldValue(document, fieldName).filter(_.isJsonObject).map(_.getAsJsonObject)

  def arrayField(document: JsonObject, fieldName: String): Option[JsonArray] =
    fieldValue(document, fieldName).filter(_.isJsonArray).map(_.getAsJsonArray)

  def stringField(document: JsonObject, fieldName: String): Option[String] =
    primitiveField(document, fieldName).filter(_.isString).map(_.getAsString)

  def intField(document: JsonObject, fieldName: String): Option[Int] =
    primitiveField(document, fieldName)
      .filter(_.isNumber)
      .flatMap(primitive => Try(new JavaBigDecimal(primitive.getAsString).intValueExact()).toOption)

  def booleanField(document: JsonObject, fieldName: String): Option[Boolean] =
    primitiveField(document, fieldName).filter(_.isBoolean).map(_.getAsBoolean)

  def stringArrayField(document: JsonObject, fieldName: String): Option[Seq[String]] =
    arrayField(document, fieldName).flatMap { array =>
      val elements = array.iterator().asScala.toSeq
      if (elements.forall(element => element.isJsonPrimitive && element.getAsJsonPrimitive.isString)) {
        Some(elements.map(_.getAsString))
      } else {
        None
      }
    }

  private def primitiveField(document: JsonObject, fieldName: String): Option[JsonPrimitive] =
    fieldValue(document, fieldName).filter(_.isJsonPrimitive).map(_.getAsJsonPrimitive)

  private def fieldValue(document: JsonObject, fieldName: String): Option[JsonElement] =
    Option(document.get(fieldName)).filter(element => !element.isJsonNull)

}
