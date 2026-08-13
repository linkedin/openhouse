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

  // ── negative / contract tests ───────────────────────────────────────────────────────────
  // Create + seed a valid CoreTable, then assert the bad operation is rejected.
  private def coreNegative(label: String)(bad: (SparkSession, String) => Unit): TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(table => s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES ('write.format.default'='$seedFmt')")()
      .insert(3)()
      .step(label)(bad)()

  private val S = CoreTable.string0.columnName

  // Each negative asserts BOTH the exception type and a message substring, so it verifies the
  // operation was rejected for the RIGHT reason (not merely that something threw).
  val negNonExistentColumn: TableTest[CoreTable.type] =
    coreNegative("negative.nonExistentColumn") { (spark, table) =>
      val e = Check.intercept[AnalysisException](spark.sql(s"DELETE FROM $table WHERE no_such_column = 1"))
      assert(e.getMessage.contains("no_such_column"))
    }

  val negNonDeterministicDelete: TableTest[CoreTable.type] =
    coreNegative("negative.nonDeterministicDelete") { (spark, table) =>
      val e = Check.intercept[AnalysisException](spark.sql(s"DELETE FROM $table WHERE rand() < 0.5"))
      assert(e.getMessage.toLowerCase.contains("deterministic"))
    }

  val negNonDeterministicUpdate: TableTest[CoreTable.type] =
    coreNegative("negative.nonDeterministicUpdate") { (spark, table) =>
      val e = Check.intercept[AnalysisException](spark.sql(s"UPDATE $table SET $S = 'x' WHERE rand() < 0.5"))
      assert(e.getMessage.toLowerCase.contains("deterministic"))
    }

  val negInsertArity: TableTest[CoreTable.type] =
    coreNegative("negative.insertArity") { (spark, table) =>
      val e = Check.intercept[AnalysisException](spark.sql(s"INSERT INTO $table VALUES (CAST(1 AS BIGINT), 1)")) // too few columns
      assert(e.getMessage.toLowerCase.contains("not enough data columns"))
    }

  // Two UPDATE assignments to the same column in one MERGE clause → analysis error.
  val negMergeConflictingUpdates: TableTest[CoreTable.type] =
    coreNegative("negative.mergeConflictingUpdates") { (spark, table) =>
      val e = Check.intercept[AnalysisException](spark.sql(
        s"""MERGE INTO $table t USING (SELECT * FROM VALUES (CAST(2 AS BIGINT)) AS s($L)) s
            ON t.$L = s.$L
            WHEN MATCHED THEN UPDATE SET t.$S = 'a', t.$S = 'b'"""))
      assert(e.getMessage.contains("Multiple assignments"))
    }

  // Source has two rows matching the same target row → cardinality violation at RUNTIME. The
  // concrete runtime exception class (SparkRuntimeException) is package-private, so we anchor on
  // the specific message across the cause chain (the error may be wrapped in a task failure).
  val negMergeCardinalityViolation: TableTest[CoreTable.type] =
    coreNegative("negative.mergeCardinalityViolation") { (spark, table) =>
      val e = Check.intercept[Exception](spark.sql(
        s"""MERGE INTO $table t USING (
              SELECT * FROM VALUES (CAST(2 AS BIGINT), 'a'), (CAST(2 AS BIGINT), 'b') AS s($L, $S)
            ) s ON t.$L = s.$L
            WHEN MATCHED THEN UPDATE SET t.$S = s.$S"""))
      assert(
        Exceptions.causeChain(e).exists(t => Option(t.getMessage).exists(_.contains("matched a single row from the target table"))),
        s"expected a MERGE cardinality-violation message, got: ${e.getMessage}")
    }

  // CREATE partitioned by a non-existent column (on a scratch name, valid managed table stays).
  val negPartitionByNonExistent: TableTest[CoreTable.type] =
    coreNegative("negative.partitionByNonExistent") { (spark, table) =>
      val scratch = table + "_x"
      val e = Check.intercept[AnalysisException](spark.sql(
        s"CREATE TABLE $scratch ($columnDefinitions) USING $dataSource PARTITIONED BY (no_such_column) TBLPROPERTIES ('write.format.default'='$seedFmt')"))
      spark.sql(s"DROP TABLE IF EXISTS $scratch")
      assert(e.getMessage.contains("no_such_column"))
    }

  val negatives: List[(String, TableTest[CoreTable.type])] = List(
    "negative.nonExistentColumn"        -> negNonExistentColumn,
    "negative.nonDeterministicDelete"   -> negNonDeterministicDelete,
    "negative.nonDeterministicUpdate"   -> negNonDeterministicUpdate,
    "negative.insertArity"              -> negInsertArity,
    "negative.mergeConflictingUpdates"  -> negMergeConflictingUpdates,
    "negative.mergeCardinalityViolation" -> negMergeCardinalityViolation,
    "negative.partitionByNonExistent"   -> negPartitionByNonExistent
  )

  // ── DDL Phase 13: schema-evolution negatives ────────────────────────────────────────────
  // DROP COLUMN fails at COMMIT (server 400 → Iceberg BadRequestException); the message carries the
  // full body incl. schema dump (AUDIT-FINDINGS B — a "dumb" message), so we anchor on the meaningful
  // "Some columns are dropped" reason. Narrowing / SET NOT NULL are caught earlier at Spark analysis
  // (ExtendedAnalysisException, a subtype of AnalysisException) with clean messages.
  // NOTE: RENAME COLUMN is NOT rejected — it is supported (see ddlRenameColumn in Phase 12).
  // DROP COLUMN rejects — but the message is `Column[foo_col_int] not found in newSchema` (buried in a
  // double schema dump); it never says "you cannot drop columns" (AUDIT-FINDINGS B, a readability gap).
  val ddlNegDropColumn: TableTest[CoreTable.type] =
    coreNegative("ddl.neg.dropColumn") { (spark, table) =>
      val e = Check.intercept[BadRequestException](spark.sql(s"ALTER TABLE $table DROP COLUMN ${Core.int0.columnName}"))
      assert(e.getMessage.contains("not found in newSchema"), s"unexpected message: ${e.getMessage.take(160)}")
      assert(e.getMessage.contains(Core.int0.columnName), s"message should name the dropped column: ${e.getMessage.take(160)}")
    }

  val ddlNegNarrowType: TableTest[CoreTable.type] =
    coreNegative("ddl.neg.narrowType") { (spark, table) =>
      val e = Check.intercept[AnalysisException](spark.sql(s"ALTER TABLE $table ALTER COLUMN ${Core.long0.columnName} TYPE int"))
      assert(e.getMessage.contains("NOT_SUPPORTED_CHANGE_COLUMN"), s"unexpected message: ${e.getMessage.take(160)}")
    }

  val ddlNegSetNotNull: TableTest[CoreTable.type] =
    coreNegative("ddl.neg.setNotNull") { (spark, table) =>
      val e = Check.intercept[AnalysisException](spark.sql(s"ALTER TABLE $table ALTER COLUMN ${Core.string0.columnName} SET NOT NULL"))
      assert(e.getMessage.contains("Cannot change nullable column to non-nullable"), s"unexpected message: ${e.getMessage.take(160)}")
    }

  val ddlNegatives: List[(String, TableTest[CoreTable.type])] = List(
    "ddl.neg.dropColumn" -> ddlNegDropColumn,
    "ddl.neg.narrowType" -> ddlNegNarrowType,
    "ddl.neg.setNotNull" -> ddlNegSetNotNull
  )

  // ── DDL Phase 14: table properties (user keys, reserved-key rejection, forced-override findings) ─
  // Self-contained pipelines (parquet) — property behavior is layout-invariant. `tableProps` reads
  // back via SHOW TBLPROPERTIES.

  private def propsCreate(label: String, tblprops: String)(check: StepView[CoreTable.type] => Unit): TableTest[CoreTable.type] =
    TableTest(Core).sql(label)(table =>
      s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES ($tblprops)")(check)

  // user key round-trips: SET then read back, UNSET removes it
  val ddlPropsUserRoundTrip: TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("ddl.props.userRoundTrip.create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES ('write.format.default'='$seedFmt')")()
      .sql("ddl.props.userRoundTrip.set")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('my_key'='my_val')") { view =>
        assert(tableProps(view.spark, view.table).get("my_key").contains("my_val"), "user prop not set")
      }
      .sql("ddl.props.userRoundTrip.unset")(t => s"ALTER TABLE $t UNSET TBLPROPERTIES ('my_key')") { view =>
        assert(!tableProps(view.spark, view.table).contains("my_key"), "user prop not removed")
      }

  // reserved-key rejection: an openhouse.* key hits the clean server guard (ALTER_RESERVED_TBLPROPS →
  // 400 → BadRequestException). NOTE: `policies` specifically is value-parsed on the CLIENT first, so
  // SET('policies'='x') throws a Gson JsonParseException before the guard — recorded in AUDIT-FINDINGS.
  val ddlPropsReservedOpenhouse: TableTest[CoreTable.type] =
    coreNegative("ddl.props.reservedOpenhouse") { (spark, table) =>
      val e = Check.intercept[BadRequestException](spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('openhouse.tableUUID'='deadbeef')"))
      assert(e.getMessage.toLowerCase.contains("restriction"), s"msg: ${e.getMessage.take(200)}")
    }

  // finding: format-version is forced to the cluster default (2) — a create with '1' still reads 2
  val ddlPropsFormatVersionForced: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES ('write.format.default'='$seedFmt', 'format-version'='1')")()
      .insert(3)()
      .check("ddl.props.formatVersionForced") { view =>
        val fv = tableProps(view.spark, view.table).get("format-version")
        assert(fv.contains("2"), s"expected forced format-version=2, got $fv")
        assert(view.after.size == 3, "table not writable at the forced format-version")   // DML-after-DDL
      }

  // honored-if-set: previous-versions-max the user provides survives
  val ddlPropsPreviousVersionsHonored: TableTest[CoreTable.type] =
    propsCreate("ddl.props.previousVersionsHonored", "'write.format.default'='$seedFmt', 'write.metadata.previous-versions-max'='7'") { view =>
      val v = tableProps(view.spark, view.table).get("write.metadata.previous-versions-max")
      assert(v.contains("7"), s"expected previous-versions-max=7, got $v")
    }

  val ddlPropsOperations: List[(String, TableTest[CoreTable.type])] = List(
    "ddl.props.userRoundTrip"          -> ddlPropsUserRoundTrip,
    "ddl.props.reservedOpenhouse"      -> ddlPropsReservedOpenhouse,
    "ddl.props.formatVersionForced"    -> ddlPropsFormatVersionForced,
    "ddl.props.previousVersionsHonored"-> ddlPropsPreviousVersionsHonored
  )

  // Per-case "current seed format" (default parquet). The assembly's `crossFmt` sets it around each case
  // so a block multiplexes across formats WITHOUT every builder taking an explicit fmt param. Safe because
  // each case runs sequentially on its own worker thread (session-per-worker, parallel runner). This is
  // how format-INERT-by-hypothesis blocks (DDL/props/policy/branch/surface/negatives) get run on ORC too —

  // ── DDL Phase 16: sort order / write distribution ───────────────────────────────────────
  // WRITE ORDERED BY sets the sort order; the observable side effect is write.distribution-mode=range
  // (the recon's CatalogOperationTest asserts this). WRITE UNORDERED clears the order.
  val ddlWriteOrderedBy: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("ddl.sortOrder.orderedBy")(t => s"ALTER TABLE $t WRITE ORDERED BY ${Core.long0.columnName}") { view =>
        assert(tableProps(view.spark, view.table).get("write.distribution-mode").contains("range"),
          s"distribution-mode not range: ${tableProps(view.spark, view.table).get("write.distribution-mode")}")
      }

  val ddlWriteOrderedByMulti: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("ddl.sortOrder.orderedByMulti")(t =>
        s"ALTER TABLE $t WRITE ORDERED BY ${Core.string0.columnName} DESC NULLS FIRST, ${Core.long0.columnName}") { view =>
        assert(tableProps(view.spark, view.table).get("write.distribution-mode").contains("range"), "multi-col ordered-by should set range")
      }
      .insert(2) { view => assert(view.after.size == 5, "multi-col ordered write path failed") }   // DML-after-DDL

  // ── DDL Phase 17: rename table (rename to scratch + back, so the harness's fixed table name resolves) ─
  val ddlRenameTable: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("ddl.renameTable") { (spark, table) =>
        val scratch = s"${table}_ren"
        spark.sql(s"ALTER TABLE $table RENAME TO $scratch")
        assert(spark.sql(s"SELECT count(*) FROM $scratch").collect()(0).getLong(0) == 3, "renamed table lost rows")
        Check.intercept[Exception](spark.sql(s"SELECT 1 FROM $table LIMIT 1"))          // old name is gone
        spark.sql(s"ALTER TABLE $scratch RENAME TO $table")                             // restore for teardown
      }()

  val ddlRenameTableConflict: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("ddl.renameTable.conflict") { (spark, table) =>
        val other = s"${table}_other"
        spark.sql(s"DROP TABLE IF EXISTS $other")
        spark.sql(coreCreateParquet(other))
        val e = Check.intercept[WebClientResponseWithMessageException](spark.sql(s"ALTER TABLE $table RENAME TO $other")) // target exists
        assert(e.getMessage.contains("already exists"), s"msg: ${e.getMessage.take(160)}")
        spark.sql(s"DROP TABLE IF EXISTS $other")
      }()

  // ── DDL Phase 19: namespace DDL negatives (OpenHouse rejects create/drop) ──────────────────
  // Both CREATE and DROP NAMESPACE surface `UnsupportedOperationException: "Describing database is not
  // supported"` — Spark calls loadNamespaceMetadata first, so the user gets a *describe* message for a
  // create/drop (a misleading message — AUDIT-FINDINGS B). We anchor on the stable "not supported".
  val ddlNegCreateNamespace: TableTest[CoreTable.type] =
    coreNegative("ddl.ns.createRejected") { (spark, _) =>
      val e = Check.intercept[UnsupportedOperationException](spark.sql("CREATE NAMESPACE openhouse.a_new_db"))
      assert(e.getMessage.contains("not supported"), s"msg: ${e.getMessage.take(160)}")
    }

  val ddlNegDropNamespace: TableTest[CoreTable.type] =
    coreNegative("ddl.ns.dropRejected") { (spark, _) =>
      val e = Check.intercept[UnsupportedOperationException](spark.sql("DROP NAMESPACE openhouse.dbMatrix"))
      assert(e.getMessage.contains("not supported"), s"msg: ${e.getMessage.take(160)}")
    }

  val ddlMiscOperations: List[(String, TableTest[CoreTable.type])] = List(
    "ddl.sortOrder.orderedBy"      -> ddlWriteOrderedBy,
    "ddl.sortOrder.orderedByMulti" -> ddlWriteOrderedByMulti,
    "ddl.renameTable"              -> ddlRenameTable,
    "ddl.renameTable.conflict"     -> ddlRenameTableConflict,
    "ddl.ns.createRejected"        -> ddlNegCreateNamespace,
    "ddl.ns.dropRejected"          -> ddlNegDropNamespace
  )

  // ── DDL Phase 20: policy DDL (OpenHouse SQL extension: ALTER TABLE … SET/UNSET POLICY) ──────
  private def policiesBlob(view: StepView[CoreTable.type]): String =
    tableProps(view.spark, view.table).getOrElse("policies", "")

  val ddlPolicySharing: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("ddl.policy.sharing")(t => s"ALTER TABLE $t SET POLICY (SHARING=TRUE)") { view =>
        assert(policiesBlob(view).toLowerCase.contains("true") || policiesBlob(view).toLowerCase.contains("sharing"),
          s"sharing policy not stored: ${policiesBlob(view)}")
        assert(view.after.size == 3, "table not queryable after SET POLICY (SHARING)")     // DML-after-DDL
      }

  val ddlPolicyHistory: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("ddl.policy.history")(t => s"ALTER TABLE $t SET POLICY (HISTORY MAX_AGE=2D VERSIONS=20)") { view =>
        assert(policiesBlob(view).contains("20") || policiesBlob(view).toLowerCase.contains("history"),
          s"history policy not stored: ${policiesBlob(view)}")
        assert(view.after.size == 3, "table not queryable after SET POLICY (HISTORY)")     // DML-after-DDL
      }

  val ddlPolicyReplicationRoundTrip: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("ddl.policy.replication.set")(t => s"ALTER TABLE $t SET POLICY (REPLICATION = ({destination:'WAR'}))")()
      .sql("ddl.policy.replication.unset")(t => s"ALTER TABLE $t UNSET POLICY (REPLICATION)") { view =>
        assert(view.after.size == 3)                                                    // survives set+unset
      }

  val ddlPolicyNegHistoryMaxAge: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("ddl.policy.neg.historyMaxAge") { (spark, table) =>
        val e = Check.intercept[BadRequestException](spark.sql(s"ALTER TABLE $table SET POLICY (HISTORY MAX_AGE=5D)")) // > 3 days
        assert(e.getMessage.contains("max age must be between 1 to 3 days"), s"msg: ${e.getMessage.take(160)}")
      }()

  val ddlPolicyNegHistoryVersions: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("ddl.policy.neg.historyVersions") { (spark, table) =>
        val e = Check.intercept[BadRequestException](spark.sql(s"ALTER TABLE $table SET POLICY (HISTORY VERSIONS=200)")) // > 100
        assert(e.getMessage.contains("must be between 2 to 100 versions"), s"msg: ${e.getMessage.take(160)}")
      }()

  // Retention on a (string) time-partitioned column requires a column pattern (a valid DateTimeFormatter).
  val ddlPolicyRetention: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource PARTITIONED BY (datepartition) TBLPROPERTIES ('write.format.default'='$seedFmt')")().insert(3)()
      .sql("ddl.policy.retention")(t => s"ALTER TABLE $t SET POLICY (RETENTION = 30d ON COLUMN datepartition WHERE pattern = 'yyyy-MM-dd-HH')") { view =>
        assert(policiesBlob(view).toLowerCase.contains("retention") || policiesBlob(view).contains("30"),
          s"retention policy not stored: ${policiesBlob(view)}")
        assert(view.after.size == 3, "table not queryable after SET POLICY (RETENTION)")   // DML-after-DDL
      }

  val ddlPolicyOperations: List[(String, TableTest[CoreTable.type])] = List(
    "ddl.policy.sharing"               -> ddlPolicySharing,
    "ddl.policy.history"               -> ddlPolicyHistory,
    "ddl.policy.replication"           -> ddlPolicyReplicationRoundTrip,
    "ddl.policy.retention"             -> ddlPolicyRetention,
    "ddl.policy.neg.historyMaxAge"     -> ddlPolicyNegHistoryMaxAge,
    "ddl.policy.neg.historyVersions"   -> ddlPolicyNegHistoryVersions
  )

  // ── DDL Phase 18: CTAS / RTAS ───────────────────────────────────────────────────────────
  val ddlCtas: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("ddl.ctas") { (spark, table) =>
        val tgt = s"${table}_ctas"
        spark.sql(s"DROP TABLE IF EXISTS $tgt")
        spark.sql(s"CREATE TABLE $tgt USING $dataSource AS SELECT * FROM $table")
        assert(spark.sql(s"SELECT count(*) FROM $tgt").collect()(0).getLong(0) == 3, "CTAS lost rows")
        spark.sql(s"DROP TABLE IF EXISTS $tgt")
      }()

  val ddlRtasEnabled: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("ddl.rtas.enable")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('replace.enabled'='true')")()
      .step("ddl.rtas.enabled") { (spark, table) =>
        spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= 2")
        assert(spark.sql(s"SELECT count(*) FROM $table").collect()(0).getLong(0) == 2, "RTAS did not replace")
      }()

  val ddlRtasDisabled: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("ddl.rtas.disabled") { (spark, table) =>
        val e = Check.intercept[BadRequestException](spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table"))
        assert(e.getMessage.contains("REPLACE TABLE AS SELECT is not enabled"), s"msg: ${e.getMessage.take(160)}")
      }()

  val ddlRtasReplicationConflict: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("ddl.rtas.repl.enable")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('replace.enabled'='true')")()
      .sql("ddl.rtas.repl.policy")(t => s"ALTER TABLE $t SET POLICY (REPLICATION = ({destination:'WAR'}))")()
      .step("ddl.rtas.replicationConflict") { (spark, table) =>
        val e = Check.intercept[BadRequestException](spark.sql(s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT * FROM $table"))
        assert(e.getMessage.contains("while replication is enabled"), s"msg: ${e.getMessage.take(160)}")
      }()

  val ddlCtasRtasOperations: List[(String, TableTest[CoreTable.type])] = List(
    "ddl.ctas"                     -> ddlCtas,
    "ddl.rtas.enabled"             -> ddlRtasEnabled,
    "ddl.rtas.disabled"            -> ddlRtasDisabled,
    "ddl.rtas.replicationConflict" -> ddlRtasReplicationConflict
  )

  // ── DDL Phase 22: column tags + ACL (metadata/ACL-plane; tags do NOT mask query results) ────
  val ddlColumnTag: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("ddl.colTag")(t => s"ALTER TABLE $t MODIFY COLUMN ${Core.string0.columnName} SET TAG = (PII)") { view =>
        val vals = view.spark.sql(s"SELECT ${Core.string0.columnName} FROM ${view.table} ORDER BY ${Core.long0.columnName}").collect().toSeq.map(_.getString(0))
        assert(vals == Seq("row-1", "row-2", "row-3"), s"SET TAG changed query results (should not mask): $vals")
      }

  val ddlAclGrantUnshared: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .step("ddl.acl.grantUnshared") { (spark, table) =>
        val e = Check.intercept[IllegalArgumentException](spark.sql(s"GRANT SELECT ON TABLE $table TO PUBLIC"))
        assert(e.getMessage.contains("is not a shared table"), s"msg: ${e.getMessage.take(160)}")
      }()

  // After SHARING=TRUE the grant is accepted (the embedded auth handler records it, no throw).
  val ddlAclGrantShared: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(coreCreateParquet)().insert(3)()
      .sql("ddl.acl.share")(t => s"ALTER TABLE $t SET POLICY (SHARING=TRUE)")()
      .sql("ddl.acl.grantShared")(t => s"GRANT SELECT ON TABLE $t TO PUBLIC") { view =>
        assert(view.after.size == 3, "shared/granted table not queryable")               // DML-after-DDL
      }

  // ── DDL Phase 15: feature-flag property (write.distribution-mode governs the write path) ─
  val ddlFeatureDistributionMode: TableTest[CoreTable.type] =
    TableTest(Core).sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES ('write.format.default'='$seedFmt', 'write.distribution-mode'='none')")()
      .insert(3)()
      .check("ddl.featureFlag.distributionMode") { view =>
        assert(tableProps(view.spark, view.table).get("write.distribution-mode").contains("none"),
          s"distribution-mode not honored: ${tableProps(view.spark, view.table).get("write.distribution-mode")}")
        assert(view.after.size == 3, "table not writable under distribution-mode=none")   // DML-after-DDL
      }

  // ── DDL Phase 23: replication / table-type contract (SQL-reachable) ─────────────────────────
  val ddlReplTableTypeImmutable: TableTest[CoreTable.type] =
    coreNegative("ddl.repl.tableTypeImmutable") { (spark, table) =>
      val e = Check.intercept[BadRequestException](spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('openhouse.tableType'='REPLICA_TABLE')"))
      assert(e.getMessage.contains("restriction"), s"msg: ${e.getMessage.take(160)}")
    }

  val ddlTagAclFeatureOperations: List[(String, TableTest[CoreTable.type])] = List(
    "ddl.colTag"                       -> ddlColumnTag,
    "ddl.acl.grantUnshared"            -> ddlAclGrantUnshared,
    "ddl.acl.grantShared"              -> ddlAclGrantShared,
    "ddl.featureFlag.distributionMode" -> ddlFeatureDistributionMode,
    "ddl.repl.tableTypeImmutable"      -> ddlReplTableTypeImmutable
  )

  // ── DDL Phase 24b: encryption — asserts the INTENDED behavior, tagged SKIP in OSS ─────────────
  // The KMS plugin is external/private (a repo-wide search finds no EncryptionManager /
  // KeyManagementClient / crypto factory / interface / mock). This test asserts what SHOULD happen —
  // with encryption configured, the data file must NOT be readable as plaintext parquet. In OSS the
  // hook is un-wired so files are plaintext and this would fail; it is tagged in Plan.knownBugs and
  // reports SKIP until the private plugin is present (then unskip to validate encryption-ON).
  val ddlEncryptionActive: TableTest[CoreTable.type] =
    TableTest(Core)
      .sql("create")(t => s"CREATE TABLE $t ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
        s"'write.format.default'='parquet', 'encryption.key-id'='k1', 'write.metadata.encryption.gcm-key-id'='k1')")()
      .insert(3)()
      .check("ddl.encryption.active") { view =>
        val filePath = view.spark.sql(s"SELECT file_path FROM ${view.table}.files LIMIT 1").collect()(0).getString(0).stripPrefix("file:")
        val head = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(filePath)).take(4))
        assert(head != "PAR1", s"encryption not in force — data file is plaintext parquet (magic=$head); requires the private KMS plugin")
      }

  val ddlEncryptionOperations: List[(String, TableTest[CoreTable.type])] = List(
    "ddl.encryption.active" -> ddlEncryptionActive
  )

  // ═══ Feature-INTERACTION axis (INTERACTION-AUDIT.md) — behaviors, single layout ══════════════
  // Characterization stance: rejections are PINS of current behavior (tripwires), not contracts;
  // a pin that starts failing means the product changed — update the pin and activate the dormant
  // coverage it gates (see the pin inventory in INTERACTION-AUDIT.md §2b).


}
