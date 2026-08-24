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

  val negativeCases: List[Plan.Case] =
    preparedCoreFormats.flatMap { preparation =>
      List(
        preparation.test("negative.nonExistentColumn") { table =>
          val exception = Check.intercept[AnalysisException](
            table.spark.sql(
              s"DELETE FROM ${table.name} WHERE no_such_column = 1"))

          assert(exception.getMessage.contains("no_such_column"))
        },
        preparation.test("negative.nonDeterministicDelete") { table =>
          val exception = Check.intercept[AnalysisException](
            table.spark.sql(
              s"DELETE FROM ${table.name} WHERE rand() < 0.5"))

          assert(
            exception.getMessage.toLowerCase.contains("deterministic"))
        },
        preparation.test("negative.nonDeterministicUpdate") { table =>
          val exception = Check.intercept[AnalysisException](
            table.spark.sql(
              s"UPDATE ${table.name} SET $S = 'x' WHERE rand() < 0.5"))

          assert(
            exception.getMessage.toLowerCase.contains("deterministic"))
        },
        preparation.test("negative.insertArity") { table =>
          val exception = Check.intercept[AnalysisException](
            table.spark.sql(
              s"INSERT INTO ${table.name} VALUES (CAST(1 AS BIGINT), 1)"))

          assert(
            exception.getMessage.toLowerCase.contains(
              "not enough data columns"))
        },
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
        },
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
        },
        preparation.test("negative.partitionByNonExistent") { table =>
          val scratchTable = table.name + "_x"
          val exception = Check.intercept[AnalysisException](
            table.spark.sql(
              s"CREATE TABLE $scratchTable ($columnDefinitions) " +
                s"USING $dataSource PARTITIONED BY (no_such_column) " +
                s"TBLPROPERTIES ('write.format.default'='${preparation.label}')"))

          table.spark.sql(s"DROP TABLE IF EXISTS $scratchTable")
          assert(exception.getMessage.contains("no_such_column"))
        })
    }

  val ddlNegativeCases: List[Plan.Case] = preparedCoreFormats.flatMap { preparation =>
    List(
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
      },
      preparation.test("ddl.neg.narrowType") { table =>
        val exception = Check.intercept[AnalysisException](
          table.spark.sql(
            s"ALTER TABLE ${table.name} ALTER COLUMN ${Core.long0.columnName} TYPE int"))

        assert(
          exception.getMessage.contains("NOT_SUPPORTED_CHANGE_COLUMN"),
          s"unexpected message: ${exception.getMessage.take(160)}")
      },
      preparation.test("ddl.neg.setNotNull") { table =>
        val exception = Check.intercept[AnalysisException](
          table.spark.sql(
            s"ALTER TABLE ${table.name} ALTER COLUMN ${Core.string0.columnName} SET NOT NULL"))

        assert(
          exception.getMessage.contains("Cannot change nullable column to non-nullable"),
          s"unexpected message: ${exception.getMessage.take(160)}")
      })
  }

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
      },
      preparation.test("ddl.props.reservedOpenhouse") { table =>
        val exception = Check.intercept[BadRequestException](
          table.spark.sql(
            s"ALTER TABLE ${table.name} SET TBLPROPERTIES (" +
              "'openhouse.tableUUID'='deadbeef')"))

        assert(
          exception.getMessage.toLowerCase.contains("restriction"),
          s"msg: ${exception.getMessage.take(200)}")
      },
      formatVersionPreparation.test("ddl.props.formatVersionForced") { table =>
        val formatVersion = tableProps(table.spark, table.name).get("format-version")

        assert(
          formatVersion.contains("2"),
          s"expected forced format-version=2, got $formatVersion")
        assert(
          table.rows.size == 3,
          "table not writable at the forced format-version")
      },
      previousVersionsPreparation.test("ddl.props.previousVersionsHonored") { table =>
        val previousVersions =
          tableProps(table.spark, table.name).get("write.metadata.previous-versions-max")

        assert(
          previousVersions.contains("7"),
          s"expected previous-versions-max=7, got $previousVersions")
      })
  }

  // Per-case "current seed format" (default parquet). The assembly's `crossFmt` sets it around each case
  // so a block multiplexes across formats WITHOUT every builder taking an explicit fmt param. Safe because
  // each case runs sequentially on its own worker thread (session-per-worker, parallel runner). This is
  // how format-INERT-by-hypothesis blocks (DDL/props/policy/branch/surface/negatives) get run on ORC too —

  val ddlMiscellaneousCases: List[Plan.Case] = preparedCoreFormats.flatMap { preparation =>
    val format = preparation.label

    List(
      preparation.test("ddl.sortOrder.orderedBy") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} WRITE ORDERED BY ${Core.long0.columnName}")

        val distributionMode =
          tableProps(table.spark, table.name).get("write.distribution-mode")

        assert(
          distributionMode.contains("range"),
          s"distribution-mode not range: $distributionMode")
      },
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
      },
      preparation.test("ddl.renameTable") { table =>
        val renamedTable = s"${table.name}_ren"

        table.spark.sql(s"ALTER TABLE ${table.name} RENAME TO $renamedTable")
        assert(
          table.spark.sql(s"SELECT count(*) FROM $renamedTable").collect()(0).getLong(0) == 3,
          "renamed table lost rows")
        Check.intercept[Exception](
          table.spark.sql(s"SELECT 1 FROM ${table.name} LIMIT 1"))
        table.spark.sql(s"ALTER TABLE $renamedTable RENAME TO ${table.name}")
      },
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
      },
      preparation.test("ddl.ns.createRejected") { table =>
        val exception = Check.intercept[UnsupportedOperationException](
          table.spark.sql("CREATE NAMESPACE openhouse.a_new_db"))

        assert(
          exception.getMessage.contains("not supported"),
          s"msg: ${exception.getMessage.take(160)}")
      },
      preparation.test("ddl.ns.dropRejected") { table =>
        val exception = Check.intercept[UnsupportedOperationException](
          table.spark.sql("DROP NAMESPACE openhouse.dbMatrix"))

        assert(
          exception.getMessage.contains("not supported"),
          s"msg: ${exception.getMessage.take(160)}")
      })
  }

  val ddlPolicyCases: List[Plan.Case] = preparedCoreFormats.flatMap { preparation =>
    val format = preparation.label
    val retentionPreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            "PARTITIONED BY (datepartition) " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())

    List(
      preparation.test("ddl.policy.sharing") { table =>
        table.spark.sql(s"ALTER TABLE ${table.name} SET POLICY (SHARING=TRUE)")

        val policies = tableProps(table.spark, table.name).getOrElse("policies", "")

        assert(
          policies.toLowerCase.contains("true") ||
            policies.toLowerCase.contains("sharing"),
          s"sharing policy not stored: $policies")
        assert(
          table.rows.size == 3,
          "table not queryable after SET POLICY (SHARING)")
      },
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
      },
      preparation.test("ddl.policy.replication") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET POLICY (REPLICATION = ({destination:'WAR'}))")
        table.spark.sql(
          s"ALTER TABLE ${table.name} UNSET POLICY (REPLICATION)")

        assert(table.rows.size == 3)
      },
      retentionPreparation.test("ddl.policy.retention") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET POLICY (" +
            "RETENTION = 30d ON COLUMN datepartition WHERE pattern = 'yyyy-MM-dd-HH')")

        val policies = tableProps(table.spark, table.name).getOrElse("policies", "")

        assert(
          policies.toLowerCase.contains("retention") || policies.contains("30"),
          s"retention policy not stored: $policies")
        assert(
          table.rows.size == 3,
          "table not queryable after SET POLICY (RETENTION)")
      },
      preparation.test("ddl.policy.neg.historyMaxAge") { table =>
        val exception = Check.intercept[BadRequestException](
          table.spark.sql(
            s"ALTER TABLE ${table.name} SET POLICY (HISTORY MAX_AGE=5D)"))

        assert(
          exception.getMessage.contains("max age must be between 1 to 3 days"),
          s"msg: ${exception.getMessage.take(160)}")
      },
      preparation.test("ddl.policy.neg.historyVersions") { table =>
        val exception = Check.intercept[BadRequestException](
          table.spark.sql(
            s"ALTER TABLE ${table.name} SET POLICY (HISTORY VERSIONS=200)"))

        assert(
          exception.getMessage.contains("must be between 2 to 100 versions"),
          s"msg: ${exception.getMessage.take(160)}")
      })
  }

  val ddlCtasRtasCases: List[Plan.Case] = preparedCoreFormats.flatMap { preparation =>
    List(
      preparation.test("ddl.ctas") { table =>
        val targetTable = s"${table.name}_ctas"

        table.spark.sql(s"DROP TABLE IF EXISTS $targetTable")
        table.spark.sql(
          s"CREATE TABLE $targetTable USING $dataSource AS SELECT * FROM ${table.name}")

        assert(
          table.spark.sql(s"SELECT count(*) FROM $targetTable").collect()(0).getLong(0) == 3,
          "CTAS lost rows")

        table.spark.sql(s"DROP TABLE IF EXISTS $targetTable")
      },
      preparation.test("ddl.rtas.enabled") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('replace.enabled'='true')")
        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            s"AS SELECT * FROM ${table.name} WHERE ${Core.long0.columnName} <= 2")

        assert(
          table.spark.sql(s"SELECT count(*) FROM ${table.name}").collect()(0).getLong(0) == 2,
          "RTAS did not replace")
      },
      preparation.test("ddl.rtas.disabled") { table =>
        val exception = Check.intercept[BadRequestException](
          table.spark.sql(
            s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
              s"AS SELECT * FROM ${table.name}"))

        assert(
          exception.getMessage.contains("REPLACE TABLE AS SELECT is not enabled"),
          s"msg: ${exception.getMessage.take(160)}")
      },
      preparation.test("ddl.rtas.replicationConflict") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('replace.enabled'='true')")
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET POLICY (REPLICATION = ({destination:'WAR'}))")

        val exception = Check.intercept[BadRequestException](
          table.spark.sql(
            s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
              s"AS SELECT * FROM ${table.name}"))

        assert(
          exception.getMessage.contains("while replication is enabled"),
          s"msg: ${exception.getMessage.take(160)}")
      })
  }

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
      },
      preparation.test("ddl.acl.grantUnshared") { table =>
        val exception = Check.intercept[IllegalArgumentException](
          table.spark.sql(s"GRANT SELECT ON TABLE ${table.name} TO PUBLIC"))

        assert(
          exception.getMessage.contains("is not a shared table"),
          s"msg: ${exception.getMessage.take(160)}")
      },
      preparation.test("ddl.acl.grantShared") { table =>
        table.spark.sql(s"ALTER TABLE ${table.name} SET POLICY (SHARING=TRUE)")
        table.spark.sql(s"GRANT SELECT ON TABLE ${table.name} TO PUBLIC")

        assert(table.rows.size == 3, "shared/granted table not queryable")
      },
      distributionModePreparation.test("ddl.featureFlag.distributionMode") { table =>
        val distributionMode =
          tableProps(table.spark, table.name).get("write.distribution-mode")

        assert(
          distributionMode.contains("none"),
          s"distribution-mode not honored: $distributionMode")
        assert(
          table.rows.size == 3,
          "table not writable under distribution-mode=none")
      },
      preparation.test("ddl.repl.tableTypeImmutable") { table =>
        val exception = Check.intercept[BadRequestException](
          table.spark.sql(
            s"ALTER TABLE ${table.name} SET TBLPROPERTIES (" +
              "'openhouse.tableType'='REPLICA_TABLE')"))

        assert(
          exception.getMessage.contains("restriction"),
          s"msg: ${exception.getMessage.take(160)}")
      })
  }

  // ── DDL Phase 24b: encryption — asserts the INTENDED behavior, tagged SKIP in OSS ─────────────
  // The KMS plugin is external/private (a repo-wide search finds no EncryptionManager /
  // KeyManagementClient / crypto factory / interface / mock). This test asserts what SHOULD happen —
  // with encryption configured, the data file must NOT be readable as plaintext parquet. In OSS the
  // hook is un-wired so files are plaintext and this would fail; it is tagged in Plan.knownBugs and
  // reports SKIP until the private plugin is present (then unskip to validate encryption-ON).
  val ddlEncryptionCases: List[Plan.Case] = {
    val preparation = TablePreparation(
      "parquet",
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
            "'write.format.default'='parquet', 'encryption.key-id'='k1', " +
            "'write.metadata.encryption.gcm-key-id'='k1')")()
        .insert(3)())

    List(preparation.test("ddl.encryption.active") { table =>
      val filePath = table.spark
        .sql(s"SELECT file_path FROM ${table.name}.files LIMIT 1")
        .collect()(0)
        .getString(0)
        .stripPrefix("file:")
      val fileHeader = new String(
        java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(filePath)).take(4))

      assert(
        fileHeader != "PAR1",
        s"encryption not in force: data file is plaintext parquet (magic=$fileHeader); " +
          "requires the private KMS plugin")
    })
  }

  // ═══ Feature-INTERACTION axis (INTERACTION-AUDIT.md) — behaviors, single layout ══════════════
  // Characterization stance: rejections are PINS of current behavior (tripwires), not contracts;
  // a pin that starts failing means the product changed — update the pin and activate the dormant
  // coverage it gates (see the pin inventory in INTERACTION-AUDIT.md §2b).


}
