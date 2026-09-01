package harness

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.iceberg.exceptions.BadRequestException
import org.apache.iceberg.exceptions.ValidationException
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

trait NegativeDdlScenarios extends ScenarioKit {
  import Rows._

  private val S = CoreTable.string0.columnName

  /** DELETE with a WHERE clause on a nonexistent column is rejected with an AnalysisException naming that column. */
  private def negativeNonExistentColumnCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("negative.nonExistentColumn") { table =>
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE no_such_column = 1"))

      assert(exception.getMessage.contains("no_such_column"))
    }

  /**
   * DELETE with a nondeterministic WHERE clause (rand() < 0.5) is rejected with an AnalysisException about determinism.
   */
  private def negativeNonDeterministicDeleteCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("negative.nonDeterministicDelete") { table =>
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE rand() < 0.5"))

      assert(
        exception.getMessage.toLowerCase.contains("deterministic"))
    }

  /**
   * UPDATE with a nondeterministic WHERE clause (rand() < 0.5) is rejected with an AnalysisException about determinism.
   */
  private def negativeNonDeterministicUpdateCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("negative.nonDeterministicUpdate") { table =>
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"UPDATE ${table.name} SET $S = 'x' WHERE rand() < 0.5"))

      assert(
        exception.getMessage.toLowerCase.contains("deterministic"))
    }

  /**
   * INSERT INTO with too few values for the table's columns is rejected with an AnalysisException about the missing
   * data columns.
   */
  private def negativeInsertArityCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("negative.insertArity") { table =>
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES (CAST(1 AS BIGINT), 1)"))

      assert(
        exception.getMessage.toLowerCase.contains(
          "not enough data columns"))
    }

  /**
   * A MERGE whose UPDATE SET assigns the same target column twice is rejected with an AnalysisException about multiple
   * assignments.
   */
  private def negativeMergeConflictingUpdatesCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("negative.mergeConflictingUpdates") { table =>
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"""MERGE INTO ${table.name} target USING (
                    SELECT * FROM VALUES (CAST(2 AS BIGINT)) AS source($L)
                  ) source
                  ON target.$L = source.$L
                  WHEN MATCHED THEN UPDATE
                  SET target.$S = 'a', target.$S = 'b'"""))

      assert(exception.getMessage.contains("Multiple assignments"))
    }

  /**
   * A MERGE whose source has two rows matching the same target row fails with a cardinality-violation error naming the
   * multi-row match.
   */
  private def negativeMergeCardinalityViolationCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("negative.mergeCardinalityViolation") { table =>
      val exception = Check.intercept[Exception](
        table.spark.sql(
          s"""MERGE INTO ${table.name} target USING (
                    SELECT * FROM VALUES
                      (CAST(2 AS BIGINT), 'a'),
                      (CAST(2 AS BIGINT), 'b')
                    AS source($L, $S)
                  ) source
                  ON target.$L = source.$L
                  WHEN MATCHED THEN UPDATE SET target.$S = source.$S"""))

      assert(
        Exceptions.causeChain(exception).exists { cause =>
          Option(cause.getMessage).exists(
            _.contains("matched a single row from the target table"))
        },
        "expected a MERGE cardinality-violation message, got: " +
          exception.getMessage)
    }

  /**
   * CREATE TABLE PARTITIONED BY a nonexistent column is rejected with an AnalysisException naming that column, and no
   * scratch table is left behind.
   */
  private def negativePartitionByNonExistentCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("negative.partitionByNonExistent") { table =>
      val scratchTable = table.name + "_x"
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"CREATE TABLE $scratchTable ($columnDefinitions) " +
            s"USING $dataSource PARTITIONED BY (no_such_column) " +
            s"TBLPROPERTIES ('write.format.default'='${preparation.label}')"))

      table.spark.sql(s"DROP TABLE IF EXISTS $scratchTable")
      assert(exception.getMessage.contains("no_such_column"))
    }

  /** The rejected DML statements, on the preparedCoreFormats preparations. */
  val negativeCases: List[Plan.Case] =
    preparedCoreFormats.flatMap { preparation =>
      List(
        negativeNonExistentColumnCase(preparation),
        negativeNonDeterministicDeleteCase(preparation),
        negativeNonDeterministicUpdateCase(preparation),
        negativeInsertArityCase(preparation),
        negativeMergeConflictingUpdatesCase(preparation),
        negativeMergeCardinalityViolationCase(preparation),
        negativePartitionByNonExistentCase(preparation))
    }

  /** ALTER TABLE DROP COLUMN is rejected with a BadRequestException naming the column that would be dropped. */
  private def ddlNegDropColumnCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.neg.dropColumn") { table =>
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} DROP COLUMN ${Core.int0.columnName}"))

      assert(
        exception.getMessage.contains("not found in newSchema"),
        s"unexpected message: ${exception.getMessage.take(160)}")
      assert(
        exception.getMessage.contains(Core.int0.columnName),
        s"message should name the dropped column: ${exception.getMessage.take(160)}")
    }

  /**
   * ALTER TABLE ALTER COLUMN to a narrower type (bigint to int) is rejected with an AnalysisException about the
   * unsupported column change.
   */
  private def ddlNegNarrowTypeCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.neg.narrowType") { table =>
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} ALTER COLUMN ${Core.long0.columnName} TYPE int"))

      assert(
        exception.getMessage.contains("NOT_SUPPORTED_CHANGE_COLUMN"),
        s"unexpected message: ${exception.getMessage.take(160)}")
    }

  /**
   * ALTER TABLE ALTER COLUMN SET NOT NULL on a nullable column is rejected with an AnalysisException about the
   * nullable-to-non-nullable change.
   */
  private def ddlNegSetNotNullCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.neg.setNotNull") { table =>
      val exception = Check.intercept[AnalysisException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} ALTER COLUMN ${Core.string0.columnName} SET NOT NULL"))

      assert(
        exception.getMessage.contains("Cannot change nullable column to non-nullable"),
        s"unexpected message: ${exception.getMessage.take(160)}")
    }

  /** The rejected schema changes, on the preparedCoreFormats preparations. */
  val ddlNegativeCases: List[Plan.Case] = preparedCoreFormats.flatMap { preparation =>
    List(
      ddlNegDropColumnCase(preparation),
      ddlNegNarrowTypeCase(preparation),
      ddlNegSetNotNullCase(preparation))
  }

  /** SET TBLPROPERTIES adds a user property that reads back, and UNSET TBLPROPERTIES removes it. */
  private def ddlPropsUserRoundTripCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.props.userRoundTrip") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('my_key'='my_val')")
      assert(
        tableProps(table.spark, table.name).get("my_key").contains("my_val"),
        "user prop not set")

      table.spark.sql(s"ALTER TABLE ${table.name} UNSET TBLPROPERTIES ('my_key')")
      assert(
        !tableProps(table.spark, table.name).contains("my_key"),
        "user prop not removed")
    }

  /**
   * SET TBLPROPERTIES on the reserved openhouse.tableUUID property is rejected with a BadRequestException about the
   * restriction.
   */
  private def ddlPropsReservedOpenhouseCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.props.reservedOpenhouse") { table =>
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES (" +
            "'openhouse.tableUUID'='deadbeef')"))

      assert(
        exception.getMessage.toLowerCase.contains("restriction"),
        s"msg: ${exception.getMessage.take(200)}")
    }

  /**
   * Even though format-version=1 was requested at creation, the table is forced to format-version=2 and remains
   * writable.
   */
  private def ddlPropsFormatVersionForcedCase(
      formatVersionPreparation: TablePreparation[CoreTable.type]): Plan.Case =
    formatVersionPreparation.test("ddl.props.formatVersionForced") { table =>
      val formatVersion = tableProps(table.spark, table.name).get("format-version")

      assert(
        formatVersion.contains("2"),
        s"expected forced format-version=2, got $formatVersion")
      assert(
        table.rows.size == 3,
        "table not writable at the forced format-version")
    }

  /** The write.metadata.previous-versions-max property requested at creation is honored and reads back as 7. */
  private def ddlPropsPreviousVersionsHonoredCase(
      previousVersionsPreparation: TablePreparation[CoreTable.type]): Plan.Case =
    previousVersionsPreparation.test("ddl.props.previousVersionsHonored") { table =>
      val previousVersions =
        tableProps(table.spark, table.name).get("write.metadata.previous-versions-max")

      assert(
        previousVersions.contains("7"),
        s"expected previous-versions-max=7, got $previousVersions")
    }

  /**
   * The table-property cases. Two of them start from the preparedCoreFormats preparation for the file format, one from
   * a table created with format-version=1 requested, and one from an unseeded table created with
   * write.metadata.previous-versions-max=7.
   */
  val ddlPropertyCases: List[Plan.Case] = preparedCoreFormats.flatMap { preparation =>
    val format = preparation.label
    val formatVersionPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
            s"'write.format.default'='$format', 'format-version'='1')")()
        .insert(3)())
    val previousVersionsPreparation = TablePreparation(
      format,
      TableTest(Core).sql("create")(table =>
        s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
          s"'write.format.default'='$format', 'write.metadata.previous-versions-max'='7')")())

    List(
      ddlPropsUserRoundTripCase(preparation),
      ddlPropsReservedOpenhouseCase(preparation),
      ddlPropsFormatVersionForcedCase(formatVersionPreparation),
      ddlPropsPreviousVersionsHonoredCase(previousVersionsPreparation))
  }

  /** ALTER TABLE WRITE ORDERED BY a single column sets write.distribution-mode to range. */
  private def ddlSortOrderOrderedByCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.sortOrder.orderedBy") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} WRITE ORDERED BY ${Core.long0.columnName}")

      val distributionMode =
        tableProps(table.spark, table.name).get("write.distribution-mode")

      assert(
        distributionMode.contains("range"),
        s"distribution-mode not range: $distributionMode")
    }

  /**
   * ALTER TABLE WRITE ORDERED BY multiple columns sets range distribution and the table remains writable, growing from
   * 3 to 5 rows after a follow-up insert.
   */
  private def ddlSortOrderOrderedByMultiCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.sortOrder.orderedByMulti") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} WRITE ORDERED BY " +
          s"${Core.string0.columnName} DESC NULLS FIRST, ${Core.long0.columnName}")

      assert(
        tableProps(table.spark, table.name).get("write.distribution-mode").contains("range"),
        "multi-col ordered-by should set range")

      table.spark.sql(
        s"INSERT INTO ${table.name} ${RowGenerator.valuesClause(Core, 2)}")

      assert(table.rows.size == 5, "multi-col ordered write path failed")
    }

  /**
   * ALTER TABLE RENAME TO moves the table to the new name with its 3 rows intact, and the old name stops resolving. A
   * second rename puts the table back under its original name, which teardown drops.
   */
  private def ddlRenameTableCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.renameTable") { table =>
      val renamedTable = s"${table.name}_ren"

      table.spark.sql(s"ALTER TABLE ${table.name} RENAME TO $renamedTable")
      assert(
        table.spark.sql(s"SELECT count(*) FROM $renamedTable").collect()(0).getLong(0) == 3,
        "renamed table lost rows")
      Check.intercept[Exception](
        table.spark.sql(s"SELECT 1 FROM ${table.name} LIMIT 1"))
      table.spark.sql(s"ALTER TABLE $renamedTable RENAME TO ${table.name}")
    }

  /** ALTER TABLE RENAME TO a name that already exists is rejected with an error naming the conflict. */
  private def ddlRenameTableConflictCase(
      preparation: TablePreparation[CoreTable.type],
      format: String): Plan.Case =
    preparation.test("ddl.renameTable.conflict") { table =>
      val conflictingTable = s"${table.name}_other"

      table.spark.sql(s"DROP TABLE IF EXISTS $conflictingTable")
      table.spark.sql(
        s"CREATE TABLE $conflictingTable ($columnDefinitions) USING $dataSource " +
          s"TBLPROPERTIES ('write.format.default'='$format')")
      val exception = Check.intercept[WebClientResponseWithMessageException](
        table.spark.sql(s"ALTER TABLE ${table.name} RENAME TO $conflictingTable"))

      assert(
        exception.getMessage.contains("already exists"),
        s"msg: ${exception.getMessage.take(160)}")
      table.spark.sql(s"DROP TABLE IF EXISTS $conflictingTable")
    }

  /**
   * CREATE NAMESPACE is rejected with an UnsupportedOperationException, since this catalog does not support creating
   * namespaces.
   */
  private def ddlNsCreateRejectedCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.ns.createRejected") { table =>
      val exception = Check.intercept[UnsupportedOperationException](
        table.spark.sql("CREATE NAMESPACE openhouse.a_new_db"))

      assert(
        exception.getMessage.contains("not supported"),
        s"msg: ${exception.getMessage.take(160)}")
    }

  /**
   * DROP NAMESPACE is rejected with an UnsupportedOperationException, since this catalog does not support dropping
   * namespaces.
   */
  private def ddlNsDropRejectedCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.ns.dropRejected") { table =>
      val exception = Check.intercept[UnsupportedOperationException](
        table.spark.sql("DROP NAMESPACE openhouse.dbMatrix"))

      assert(
        exception.getMessage.contains("not supported"),
        s"msg: ${exception.getMessage.take(160)}")
    }

  /** The remaining DDL cases, on the preparedCoreFormats preparations. */
  val ddlMiscellaneousCases: List[Plan.Case] = preparedCoreFormats.flatMap { preparation =>
    val format = preparation.label

    List(
      ddlSortOrderOrderedByCase(preparation),
      ddlSortOrderOrderedByMultiCase(preparation),
      ddlRenameTableCase(preparation),
      ddlRenameTableConflictCase(preparation, format),
      ddlNsCreateRejectedCase(preparation),
      ddlNsDropRejectedCase(preparation))
  }

  /** SET POLICY (SHARING=TRUE) records the sharing policy and the table remains queryable. */
  private def ddlPolicySharingCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.policy.sharing") {
      table =>
      table.spark.sql(s"ALTER TABLE ${table.name} SET POLICY (SHARING=TRUE)")

      val policies = tableProps(table.spark, table.name).getOrElse("policies", "")

      assert(
        policies.toLowerCase.contains("true") ||
          policies.toLowerCase.contains("sharing"),
        s"sharing policy not stored: $policies")
      assert(
        table.rows.size == 3,
        "table not queryable after SET POLICY (SHARING)")
    }

  /** SET POLICY (HISTORY MAX_AGE=2D VERSIONS=20) records the history policy and the table remains queryable. */
  private def ddlPolicyHistoryCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.policy.history") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} SET POLICY (HISTORY MAX_AGE=2D VERSIONS=20)")

      val policies = tableProps(table.spark, table.name).getOrElse("policies", "")

      assert(
        policies.contains("20") || policies.toLowerCase.contains("history"),
        s"history policy not stored: $policies")
      assert(
        table.rows.size == 3,
        "table not queryable after SET POLICY (HISTORY)")
    }

  /**
   * SET POLICY (REPLICATION) followed by UNSET POLICY (REPLICATION) leaves the table queryable with its 3 rows intact.
   */
  private def ddlPolicyReplicationCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.policy.replication") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} SET POLICY (REPLICATION = ({destination:'WAR'}))")
      table.spark.sql(
        s"ALTER TABLE ${table.name} UNSET POLICY (REPLICATION)")

      assert(table.rows.size == 3)
    }

  /**
   * SET POLICY (RETENTION = 30d ON COLUMN foo_col_date ...) records the retention policy and the table remains
   * queryable.
   */
  private def ddlPolicyRetentionCase(
      retentionPreparation: TablePreparation[CoreTable.type]): Plan.Case =
    retentionPreparation.test("ddl.policy.retention") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} SET POLICY (" +
          s"RETENTION = 30d ON COLUMN ${Core.date0.columnName} WHERE pattern = 'yyyy-MM-dd-HH')")

      val policies = tableProps(table.spark, table.name).getOrElse("policies", "")

      assert(
        policies.toLowerCase.contains("retention") || policies.contains("30"),
        s"retention policy not stored: $policies")
      assert(
        table.rows.size == 3,
        "table not queryable after SET POLICY (RETENTION)")
    }

  /**
   * SET POLICY (HISTORY MAX_AGE=5D) exceeds the allowed range and is rejected with a BadRequestException stating the
   * 1-to-3-day limit.
   */
  private def ddlPolicyNegHistoryMaxAgeCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.policy.neg.historyMaxAge") { table =>
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET POLICY (HISTORY MAX_AGE=5D)"))

      assert(
        exception.getMessage.contains("max age must be between 1 to 3 days"),
        s"msg: ${exception.getMessage.take(160)}")
    }

  /**
   * SET POLICY (HISTORY VERSIONS=200) exceeds the allowed range and is rejected with a BadRequestException stating the
   * 2-to-100-version limit.
   */
  private def ddlPolicyNegHistoryVersionsCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.policy.neg.historyVersions") { table =>
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET POLICY (HISTORY VERSIONS=200)"))

      assert(
        exception.getMessage.contains("must be between 2 to 100 versions"),
        s"msg: ${exception.getMessage.take(160)}")
    }

  /**
   * The table-policy cases. They start from the preparedCoreFormats preparation for the file format, except the
   * retention case, which starts from three seed rows in a table partitioned by the date column.
   */
  val ddlPolicyCases: List[Plan.Case] = preparedCoreFormats.flatMap { preparation =>
    val format = preparation.label
    val retentionPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"PARTITIONED BY (${Core.date0.columnName}) " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())

    List(
      ddlPolicySharingCase(preparation),
      ddlPolicyHistoryCase(preparation),
      ddlPolicyReplicationCase(preparation),
      ddlPolicyRetentionCase(retentionPreparation),
      ddlPolicyNegHistoryMaxAgeCase(preparation),
      ddlPolicyNegHistoryVersionsCase(preparation))
  }

  /**
   * ALTER TABLE MODIFY COLUMN SET TAG = (PII) tags a column without masking or changing the values that queries return.
   */
  private def ddlColTagCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.colTag") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} MODIFY COLUMN " +
          s"${Core.string0.columnName} SET TAG = (PII)")

      val values = table.spark
        .sql(
          s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
            s"ORDER BY ${Core.long0.columnName}")
        .collect()
        .toSeq
        .map(_.getString(0))

      assert(
        values == Seq("row-1", "row-2", "row-3"),
        s"SET TAG changed query results (should not mask): $values")
    }

  /**
   * GRANT SELECT on a table that is not marked shared is rejected with an IllegalArgumentException stating the table is
   * not shared.
   */
  private def ddlAclGrantUnsharedCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.acl.grantUnshared") { table =>
      val exception = Check.intercept[IllegalArgumentException](
        table.spark.sql(s"GRANT SELECT ON TABLE ${table.name} TO PUBLIC"))

      assert(
        exception.getMessage.contains("is not a shared table"),
        s"msg: ${exception.getMessage.take(160)}")
    }

  /**
   * On a shared table, GRANT SELECT TO PUBLIC makes SHOW GRANTS list SELECT for PUBLIC and the table stays queryable;
   * REVOKE SELECT then removes that grant from SHOW GRANTS.
   */
  private def ddlAclGrantSharedCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation
      .test("ddl.acl.grantShared") { table =>
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
        assert(table.rows.size == 3, "shared/granted table not queryable")

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

  /**
   * The write.distribution-mode=none property requested at creation is honored and the table remains writable under it.
   */
  private def ddlFeatureFlagDistributionModeCase(
      distributionModePreparation: TablePreparation[CoreTable.type]): Plan.Case =
    distributionModePreparation.test("ddl.featureFlag.distributionMode") { table =>
      val distributionMode =
        tableProps(table.spark, table.name).get("write.distribution-mode")

      assert(
        distributionMode.contains("none"),
        s"distribution-mode not honored: $distributionMode")
      assert(
        table.rows.size == 3,
        "table not writable under distribution-mode=none")
    }

  /**
   * ALTER TABLE SET TBLPROPERTIES ('openhouse.tableType'='REPLICA_TABLE') is rejected with a BadRequestException, since
   * table type cannot be changed after creation.
   */
  private def ddlReplTableTypeImmutableCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("ddl.repl.tableTypeImmutable") { table =>
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES (" +
            "'openhouse.tableType'='REPLICA_TABLE')"))

      assert(
        exception.getMessage.contains("restriction"),
        s"msg: ${exception.getMessage.take(160)}")
    }

  /**
   * The column-tag, ACL and feature-flag cases. They start from the preparedCoreFormats preparation for the file
   * format, except the distribution-mode case, which starts from three seed rows in a table created with
   * write.distribution-mode=none.
   */
  val ddlTagAclFeatureCases: List[Plan.Case] = preparedCoreFormats.flatMap { preparation =>
    val format = preparation.label
    val distributionModePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
            s"'write.format.default'='$format', 'write.distribution-mode'='none')")()
        .insert(3)())

    List(
      ddlColTagCase(preparation),
      ddlAclGrantUnsharedCase(preparation),
      ddlAclGrantSharedCase(preparation),
      ddlFeatureFlagDistributionModeCase(distributionModePreparation),
      ddlReplTableTypeImmutableCase(preparation))
  }

}
