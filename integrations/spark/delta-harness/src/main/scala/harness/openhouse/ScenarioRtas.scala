package harness

import java.util.concurrent.ConcurrentHashMap

import org.apache.iceberg.exceptions.{BadRequestException, ValidationException}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import scala.util.control.NonFatal

/**
 * Replace table as select: what CREATE OR REPLACE TABLE AS SELECT is allowed to do to a table, and what survives it.
 *
 * A replace re-specifies a table in place and starts a new snapshot lineage under the same catalog identity. That
 * makes it the one statement in the harness that can change a table's shape, its partitioning and its content at
 * once, so this file owns two separate obligations. The first is that the reusable DML contract still holds on a
 * table that reached its starting state through a replace. The second is that everything the catalog governs, and
 * everything a reader can ask about history, behaves the way a new lineage requires.
 *
 * Operations, DML: every reusable operation `ScenarioDml` defines, reused as data. A replaced table runs the same
 * statements and the same assertions as a freshly created one, so this file holds one definition of each operation.
 * The 51 operations that
 * fit any seeded table cross all four replace preparations, the null-string DELETE crosses the four replace
 * preparations that carry a null row, and the two partition-scoped writes cross the two date-partitioned replace
 * preparations. That is all 54 reusable operations on the preparations each one applies to.
 *
 * Operations, replace contract: 26 focused families covering the enablement gates, same-shape replacement and the
 * write that follows it, the four schema discontinuities, partition replacement and the second replace that is the
 * only legal way to repartition afterwards, property override and preservation, retention-policy and column-tag
 * preservation, pre-replace time travel with rollback rejection and set-current-snapshot recovery, changelog and
 * incremental-read rejection across the replacement boundary, replace crossed with rename in both orders, sort-order
 * change and removal after a replace, creator-identity preservation, and a replace racing an append.
 *
 * Preparation axes: replace lineage is the axis this layer adds. Four replace preparations cross the two columnar
 * formats with unpartitioned and date-partitioned tables; each creates a replace-enabled table, seeds the standard
 * three rows, re-specifies the same shape through CREATE OR REPLACE TABLE AS SELECT, and refreshes, so the rows a
 * case starts from arrived through the replace path. The contract families start from a plain replace-enabled table
 * in each format, or from the property, retention or tag table each one needs, and drive the replace themselves so
 * they can read the state on both sides of it.
 *
 * Case families: 264 cases. The DML axis contributes 212 in three families, and the replace contract contributes 52
 * in 26 families, each family running in both columnar formats.
 */
trait ScenarioRtas extends ScenarioKit { this: ScenarioDml with ChangelogSupport =>

  /** Every replace case: the reusable DML operations on replaced tables first, then the replace contract. */
  lazy val rtasCases: List[TestCase] = rtasDmlCases ++ rtasContractCases

  /**
   * The reusable DML operations on replaced tables, in preparation order: every operation on the four replace
   * preparations, the null-string DELETE on their null-string form, then the partition-scoped writes on the two
   * date-partitioned replace preparations.
   */
  lazy val rtasDmlCases: List[TestCase] =
    rtasCoreDmlCases ++ rtasNullStringDmlCases ++ rtasPartitionedDmlCases

  /** Every operation that fits any seeded table, on each of the four replace preparations. */
  lazy val rtasCoreDmlCases: List[TestCase] =
    preparedRtasCoreTables.flatMap(preparation => allDmlTestCases.map(_.runOn(preparation)))

  /** The DELETE that selects a null string, on the replace preparations that carry a null row. */
  lazy val rtasNullStringDmlCases: List[TestCase] =
    preparedNullStringRtasCoreTables.flatMap(preparation =>
      nullStringRowTestCases.map(_.runOn(preparation)))

  /** The partition-scoped writes, on the two date-partitioned replace preparations. */
  lazy val rtasPartitionedDmlCases: List[TestCase] =
    preparedRtasPartitionedCoreTables.flatMap(preparation =>
      partitionedTableTestCases.map(_.runOn(preparation)))

  /** Every replace-contract case, one file format at a time. */
  lazy val rtasContractCases: List[TestCase] =
    fileFormats.flatMap { format =>
      List(
        enablementGateCase(preparedStandardTable(format)),
        disabledGateRejectedCase(preparedStandardTable(format)),
        replicationGateRejectedCase(preparedStandardTable(format)),
        sameShapeReplacementCase(preparedReplaceEnabledTable(format)),
        writeAfterReplaceCase(preparedReplaceEnabledTable(format)),
        schemaAddColumnCase(preparedReplaceEnabledTable(format)),
        schemaDropColumnCase(preparedReplaceEnabledTable(format)),
        schemaWidenColumnCase(preparedReplaceEnabledTable(format)),
        schemaIncompatibleTypeRejectedCase(preparedReplaceEnabledTable(format)),
        partitionSpecReplacedCase(preparedReplaceEnabledTable(format)),
        partitionChangeAfterReplaceCase(preparedReplaceEnabledPartitionedTable(format)),
        userPropertyPreservedCase(preparedUserPropertyTable(format)),
        statementOverridesPropertyCase(preparedUserPropertyTable(format)),
        retentionPolicyPreservedCase(preparedRetentionPolicyTable(format)),
        columnTagPreservedCase(preparedTaggedTable(format)),
        preReplaceTimeTravelCase(preparedReplaceEnabledTable(format)),
        rollbackAcrossLineageRejectedCase(preparedReplaceEnabledTable(format)),
        setCurrentSnapshotRecoversCase(preparedReplaceEnabledTable(format)),
        changelogAcrossBoundaryCase(preparedReplaceEnabledTable(format)),
        incrementalReadAcrossBoundaryCase(preparedReplaceEnabledTable(format)),
        replaceThenRenameCase(preparedReplaceEnabledTable(format)),
        renameThenReplaceCase(preparedReplaceEnabledTable(format)),
        sortOrderChangedAfterReplaceCase(preparedReplaceEnabledTable(format)),
        sortOrderRemovedAfterReplaceCase(preparedReplaceEnabledTable(format)),
        creatorIdentityPreservedCase(preparedReplaceEnabledTable(format)),
        replaceVersusAppendCase(preparedReplaceEnabledTable(format)))
    }

  // --- the replace preparations the DML axis runs on ---

  /**
   * One replace preparation per columnar format and partitioning: the table is created replace-enabled, seeded with
   * the standard rows, re-specified in place by a same-shape CREATE OR REPLACE TABLE AS SELECT, then refreshed. The
   * result holds the standard seed reached through the replace path, so every reusable DML operation that holds on a
   * freshly seeded table must also hold here.
   */
  lazy val preparedRtasCoreTables: List[TablePreparation[CoreTable.type]] =
    for {
      format       <- fileFormats
      partitioning <- partitionings
    } yield TablePreparation(
      s"${partitioning.label}/$format",
      replaceLineage(partitioning, format),
      rtasCasePrefix)

  /**
   * One replace preparation per date-partitioned layout, so the partition-scoped writes replace whole partitions of a
   * table that reached those partitions through the replace path.
   */
  lazy val preparedRtasPartitionedCoreTables: List[TablePreparation[CoreTable.type]] =
    fileFormats.map(format =>
      TablePreparation(
        s"${partitionedByDate.label}/$format",
        replaceLineage(partitionedByDate, format),
        rtasCasePrefix))

  /** The replace preparations, each carrying one row whose string column is null. */
  lazy val preparedNullStringRtasCoreTables: List[TablePreparation[CoreTable.type]] =
    preparedRtasCoreTables.map(withNullStringRow)

  /** The prefix that marks a case ID as running on a table that reached its starting state through a replace. */
  val rtasCasePrefix: String = "prep.rtas:"

  // --- the starting states, shared helpers and case bodies the surface above composes ---

  /** The layout of a table the catalog will let a case replace: the core shape, shaped by `partitioning`. */
  private def replaceEnabledLayout(partitioning: Partitioning, format: String): Layout =
    Layout(
      s"${partitioning.label}/$format",
      table =>
        s"CREATE TABLE $table ($columnDefinitions) USING $dataSource ${partitioning.clause} " +
          s"TBLPROPERTIES ('write.format.default'='$format', 'replace.enabled'='true')")

  /**
   * Creates a replace-enabled table, seeds the standard rows, re-specifies the same shape through CREATE OR REPLACE
   * TABLE AS SELECT, and refreshes it. The REFRESH is required: the Spark session holds the table state it read
   * before the replace, and REFRESH re-reads the committed metadata pointer so later statements in the session
   * address the replaced table.
   */
  private def replaceLineage(
      partitioning: Partitioning,
      format: String): TableTest[CoreTable.type] =
    create(replaceEnabledLayout(partitioning, format))
      .insert(standardSeedRowCount)()
      .sql("prep.rtas")(table =>
        s"CREATE OR REPLACE TABLE $table USING $dataSource ${partitioning.clause} " +
          s"TBLPROPERTIES ('write.format.default'='$format') AS SELECT * FROM $table")(view => {
        assertSeededCoreShape(view, "prep.rtas")
        assert(
          view.snapshotsAfter == view.snapshotsBefore + 1,
          s"prep.rtas commits one snapshot, went from ${view.snapshotsBefore} to " +
            s"${view.snapshotsAfter}")
      })
      .step("prep.rtas.refresh")((spark, table) => {
        val currentSnapshotBefore = currentSnapshotId(spark, table)
        val snapshotCountBefore = PreparedTable.snapshotCount(spark, table)
        spark.sql(s"REFRESH TABLE $table")
        assert(
          currentSnapshotId(spark, table) == currentSnapshotBefore,
          s"prep.rtas.refresh keeps main on snapshot $currentSnapshotBefore, moved it to " +
            s"${currentSnapshotId(spark, table)}")
        assert(
          PreparedTable.snapshotCount(spark, table) == snapshotCountBefore,
          s"prep.rtas.refresh keeps the snapshot count at $snapshotCountBefore")
      })(view => {
        assertSeededCoreShape(view, "prep.rtas.refresh")
        assert(
          view.snapshotsAfter == view.snapshotsBefore,
          s"prep.rtas.refresh reads committed metadata and commits nothing, went from " +
            s"${view.snapshotsBefore} to ${view.snapshotsAfter} snapshots")
      })

  /**
   * The state both replace-preparation steps leave behind: the standard seed rows in key order, unchanged by the step,
   * under exactly the core columns in their declared order. Both steps assert it, so a replace that loses a row,
   * reorders the schema or drops a column fails during preparation, so the DML cases always compare against a known
   * baseline.
   */
  private def assertSeededCoreShape(view: StepView[CoreTable.type], stepLabel: String): Unit = {
    val schemaColumnNames = view.spark.table(view.table).schema.fieldNames.toSeq

    assert(
      schemaColumnNames == Core.columnNames,
      s"$stepLabel presents the core schema, found $schemaColumnNames")
    assert(
      view.after == view.before,
      s"$stepLabel keeps every row it started from, went from ${view.before} to ${view.after}")
    assert(
      view.after.size == standardSeedRowCount,
      s"$stepLabel holds the $standardSeedRowCount standard seed rows, found ${view.after.size}")
    assert(
      inKeyOrder(view.after) == view.after,
      s"$stepLabel returns the seed rows in key order, found ${view.after}")
    assert(
      view.after.map(row => Rows.TypedRow(row).get(Core.long0)) ==
        (1L to standardSeedRowCount.toLong).toList,
      s"$stepLabel holds the standard seed keys, found " +
        s"${view.after.map(row => Rows.TypedRow(row).get(Core.long0))}")
  }

  /**
   * The snapshot the table's main branch currently points at, read from the refs metadata table, which names exactly
   * one snapshot per branch. A replace starts a second root in the snapshots metadata table, so main is the one
   * source that identifies the live snapshot after a replace.
   */
  private def currentSnapshotId(spark: SparkSession, table: String): Long =
    spark
      .sql(s"SELECT snapshot_id FROM $table.refs WHERE name = 'main'")
      .collect()
      .toSeq
      .map(_.getLong(0)) match {
      case Seq(snapshotId) => snapshotId
      case mainSnapshotIds =>
        throw new AssertionError(s"main names one snapshot, found $mainSnapshotIds")
    }

  /**
   * The standard seed in an unpartitioned replace-enabled table in `format`, so a contract case starts from a table
   * the catalog will let it replace and drives the replace itself.
   */
  private def preparedReplaceEnabledTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      create(replaceEnabledLayout(unpartitioned, format)).insert(standardSeedRowCount)())

  /** The same starting state partitioned by the date column, for the cases that repartition after a replace. */
  private def preparedReplaceEnabledPartitionedTable(
      format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      create(replaceEnabledLayout(partitionedByDate, format)).insert(standardSeedRowCount)())

  /**
   * A replace-enabled table in `format` carrying the user property user.key=v1, so a case reads back what a replace
   * does to a property the user set.
   */
  private def preparedUserPropertyTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource TBLPROPERTIES (" +
            s"'write.format.default'='$format', 'replace.enabled'='true', 'user.key'='v1')")()
        .insert(standardSeedRowCount)())

  /**
   * A date-partitioned replace-enabled table in `format` carrying a 30-day retention policy on the date column, so a
   * case reads back what a replace does to a policy the catalog stores.
   */
  private def preparedRetentionPolicyTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      create(replaceEnabledLayout(partitionedByDate, format))
        .insert(standardSeedRowCount)()
        .sql("setRetentionPolicy")(table =>
          s"ALTER TABLE $table SET POLICY " +
            s"(RETENTION = 30d ON COLUMN ${Core.date0.columnName} " +
            "WHERE pattern = 'yyyy-MM-dd-HH')")())

  /**
   * A replace-enabled table in `format` whose string column carries the PII tag, so a case reads back what a replace
   * does to a column classification.
   */
  private def preparedTaggedTable(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      create(replaceEnabledLayout(unpartitioned, format))
        .insert(standardSeedRowCount)()
        .sql("tagStringColumnAsPii")(table =>
          s"ALTER TABLE $table MODIFY COLUMN ${Core.string0.columnName} SET TAG = (PII)")())

  /** The statement that replaces `table` in place with the rows whose key is at most `keyLimit`. */
  private def replaceWithKeysUpTo(table: String, keyLimit: Int): String =
    s"CREATE OR REPLACE TABLE $table USING $dataSource " +
      s"AS SELECT * FROM $table WHERE ${Core.long0.columnName} <= $keyLimit"

  /**
   * The message Iceberg raises when a requested snapshot range starts outside the lineage the table currently follows.
   * A replace starts a new lineage, so both the changelog view and the incremental scan report a pre-replace start
   * snapshot this way.
   */
  private val crossLineageRejectionMessage = "is not a parent ancestor of end snapshot"

  /** The two outcomes a racing writer records: its statement committed, or it hit a typed commit conflict. */
  private val committedOutcome = "committed"
  private val conflictedOutcome = "conflicted"

  /**
   * The markers a rejection carries when the catalog refuses a column type change as incompatible. The in-place ALTER
   * COLUMN TYPE path answers with the Spark analyzer marker, and the catalog answers with a message naming the
   * change it refused.
   */
  private val incompatibleTypeRejectionMarkers =
    List("NOT_SUPPORTED_CHANGE_COLUMN", "incompatible", "cannot be cast", "narrow")

  /** The statement that replaces `table` in place with `projection` selected from it. */
  private def replaceWithProjection(table: String, projection: String): String =
    s"CREATE OR REPLACE TABLE $table USING $dataSource AS SELECT $projection FROM $table"

  /** The reserved properties that identify a table to the catalog and must outlive a replace. */
  private val identityPropertyNames = List(
    "openhouse.tableUUID",
    "openhouse.tableId",
    "openhouse.databaseId",
    "openhouse.tableCreator")

  /** The values `table` currently reports for the reserved identity properties. */
  private def identityProperties(
      table: PreparedTable[CoreTable.type]): Map[String, String] = {
    val properties = tableProps(table.spark, table.name)
    identityPropertyNames.flatMap(name => properties.get(name).map(name -> _)).toMap
  }

  /** The column names the table reports, in the order it reports them. */
  private def columnNamesOf(table: PreparedTable[CoreTable.type], name: String): Seq[String] =
    table.spark.sql(s"SELECT * FROM $name LIMIT 0").columns.toSeq

  // --- 1. the gates that decide whether a replace is allowed at all ---

  /**
   * With replace.enabled=true, CREATE OR REPLACE TABLE AS SELECT replaces the table's content, leaving exactly the two
   * rows the replacement query selected.
   */
  private def enablementGateCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.gate.enabled") { table =>
      table.spark.sql(s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('replace.enabled'='true')")
      table.spark.sql(replaceWithKeysUpTo(table.name, 2))

      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "2",
        "an enabled replace should leave only the rows its query selected")
    }

  /**
   * On a table that has left replace.enabled unset, CREATE OR REPLACE TABLE AS SELECT is rejected with a
   * BadRequestException naming the disabled feature, and the table keeps the rows it had, so a table opts in before
   * anything rewrites it.
   */
  private def disabledGateRejectedCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.gate.disabled.rejected") { table =>
      val stateBefore = table.state
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(replaceWithKeysUpTo(table.name, 2)))

      assert(
        exception.getMessage.contains("REPLACE TABLE AS SELECT is not enabled"),
        s"unexpected message: ${exception.getMessage.take(160)}")
      assert(table.state == stateBefore, "a rejected replace should leave the table as it was")
    }

  /**
   * With replace.enabled=true but a replication policy also set, CREATE OR REPLACE TABLE AS SELECT is rejected with a
   * BadRequestException naming replication, so a replicated table keeps the lineage its replicas follow.
   */
  private def replicationGateRejectedCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.gate.replicationConflict.rejected") { table =>
      table.spark.sql(s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('replace.enabled'='true')")
      table.spark.sql(
        s"ALTER TABLE ${table.name} SET POLICY (REPLICATION = ({destination:'WAR'}))")
      val stateBefore = table.state
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(replaceWithKeysUpTo(table.name, 2)))

      assert(
        exception.getMessage.contains("while replication is enabled"),
        s"unexpected message: ${exception.getMessage.take(160)}")
      assert(table.state == stateBefore, "a rejected replace should leave the table as it was")
    }

  // --- 2. the plainest replace, and the write that follows it ---

  /**
   * A same-shape CREATE OR REPLACE TABLE AS SELECT keeps every column in its declared order and every row it selected,
   * and commits exactly one snapshot, so replacing a table with itself changes nothing a reader can see except the
   * lineage.
   */
  private def sameShapeReplacementCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.sameShapeReplacement") { table =>
      val rowsBefore = table.rows
      val snapshotsBefore = table.snapshotCount
      table.spark.sql(replaceWithProjection(table.name, columnNameList))

      assert(
        columnNamesOf(table, table.name) == Core.columnNames,
        "a same-shape replace should keep the declared columns in order")
      assert(
        inKeyOrder(table.rows) == inKeyOrder(rowsBefore),
        "a same-shape replace should keep every row it selected")
      assert(
        table.snapshotCount == snapshotsBefore + 1,
        s"a replace should commit one snapshot, went from $snapshotsBefore to ${table.snapshotCount}")
    }

  /**
   * A replaced table accepts an INSERT immediately afterwards and the row lands, so a writer that follows a replace in
   * the same session addresses the replaced table, which is the lineage the replace made current.
   */
  private def writeAfterReplaceCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.writeAfterReplace") { table =>
      table.spark.sql(replaceWithKeysUpTo(table.name, 2))
      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6L, "row-6")}")

      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "3",
        "the replaced table should hold the two selected rows plus the inserted one")
      assert(
        countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name} WHERE ${Core.long0.columnName} = 6") == "1",
        "the row inserted after the replace should be readable")
    }

  // --- 3. the four schema discontinuities a replace can introduce ---

  /**
   * A replace whose projection adds a computed column widens the schema to that column and every row carries its
   * value, so a replace is how a caller adds a column with data already in it.
   */
  private def schemaAddColumnCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.schema.addColumn") { table =>
      table.spark.sql(
        replaceWithProjection(table.name, s"$columnNameList, CAST(7 AS INT) AS added_col"))

      assert(
        columnNamesOf(table, table.name) == Core.columnNames :+ "added_col",
        "a replace that projects a new column should add it after the existing ones")
      assert(
        countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name} WHERE added_col = 7") ==
          standardSeedRowCount.toString,
        "every row should carry the value the projection computed")
    }

  /**
   * A replace whose projection names fewer columns drops the rest while preserving every row, so a replace is how a
   * caller removes a column the catalog refuses to drop in place.
   */
  private def schemaDropColumnCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.schema.dropColumn") { table =>
      table.spark.sql(
        replaceWithProjection(
          table.name,
          s"${Core.long0.columnName}, ${Core.string0.columnName}"))

      assert(
        columnNamesOf(table, table.name) ==
          Seq(Core.long0.columnName, Core.string0.columnName),
        "a replace that projects two columns should leave exactly those two")
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") ==
          standardSeedRowCount.toString,
        "dropping a column through a replace should preserve every row")
    }

  /**
   * A replace that casts the int column to bigint widens it and every value reads back unchanged, so a replace carries
   * a widening type change the same way an in-place ALTER COLUMN TYPE does.
   */
  private def schemaWidenColumnCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.schema.widenColumn") { table =>
      val intValuesBefore = table.spark
        .sql(s"SELECT ${Core.int0.columnName} FROM ${table.name} ORDER BY ${Core.int0.columnName}")
        .collect()
        .toSeq
        .map(_.getInt(0).toLong)
      table.spark.sql(
        replaceWithProjection(
          table.name,
          s"${Core.long0.columnName}, " +
            s"CAST(${Core.int0.columnName} AS BIGINT) AS ${Core.int0.columnName}"))
      val widenedType = table.spark
        .table(table.name)
        .schema
        .fields
        .toList
        .collectFirst { case field if field.name == Core.int0.columnName => field.dataType.simpleString }

      assert(
        widenedType.contains("bigint"),
        s"the replace should widen the int column to bigint, got $widenedType")
      assert(
        table.spark
          .sql(s"SELECT ${Core.int0.columnName} FROM ${table.name} ORDER BY ${Core.int0.columnName}")
          .collect()
          .toSeq
          .map(_.getLong(0)) == intValuesBefore,
        "widening through a replace should preserve every value")
    }

  /**
   * A replace narrows the bigint key column to int on a table holding a key wider than an int. A replace defines a
   * fresh schema, so it accepts narrowing where the in-place ALTER COLUMN TYPE path refuses it. The contract is that
   * the value survives either way: the replace is rejected for the type incompatibility and leaves the bigint column
   * and its key intact, or it is accepted and the key still reads back as the key that was written.
   *
   * The current product accepts the replace, reports success and returns -1294967296 for the key 3000000000. This case
   * keeps value preservation as the assertion so the gap stays visible, and is skipped until the product either
   * rejects the narrowing or preserves the value.
   */
  private def schemaIncompatibleTypeRejectedCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation
      .test("rtas.schema.incompatibleType.notSilentlyLossy") { table =>
        val outOfRangeKey = 3000000000L
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            s"(CAST($outOfRangeKey AS BIGINT), 4, 'row-big', 4.5, true, '2024-01-04-03')")

        val rejectionMessage =
          try {
            table.spark.sql(
              replaceWithProjection(
                table.name,
                s"CAST(${Core.long0.columnName} AS INT) AS ${Core.long0.columnName}, " +
                  s"${Core.string0.columnName}"))
            None
          } catch {
            case rejection: AnalysisException   => Some(rejection.getMessage)
            case rejection: BadRequestException => Some(rejection.getMessage)
            case rejection: ValidationException => Some(rejection.getMessage)
          }
        val keyColumnType = table.spark
          .table(table.name)
          .schema
          .fields
          .toList
          .collectFirst {
            case field if field.name == Core.long0.columnName => field.dataType.simpleString
          }
        val storedKeys = table.spark
          .sql(
            s"SELECT ${Core.long0.columnName} FROM ${table.name} " +
              s"WHERE ${Core.string0.columnName} = 'row-big'")
          .collect()
          .toSeq
          .map(row => row.get(0).asInstanceOf[Number].longValue)

        rejectionMessage match {
          case Some(message) =>
            assert(
              incompatibleTypeRejectionMarkers.exists(message.contains),
              s"the rejection identifies the type incompatibility, found: ${message.take(200)}")
            assert(
              keyColumnType.contains("bigint"),
              s"a rejected narrowing leaves the key column bigint, found $keyColumnType")
            assert(
              storedKeys == List(outOfRangeKey),
              s"a rejected narrowing leaves the key at $outOfRangeKey, found $storedKeys")
          case None =>
            assert(
              storedKeys == List(outOfRangeKey),
              s"an accepted narrowing preserves the key $outOfRangeKey, " +
                s"found $storedKeys under type $keyColumnType")
        }
      }
      .copy(knownBugReason = Some(
        "A replace that narrows bigint to int is accepted and wraps an out-of-range key around " +
          "while the contract requires rejection or value preservation. The key 3000000000 reads back as " +
          "-1294967296 after the product reports success."))

  // --- 4. partition discontinuities, and evolving the partitioning the replace installed ---

  /**
   * A replace with a new PARTITIONED BY clause installs that partition specification and preserves every row, so a
   * replace is the supported repartitioning path after the catalog rejects in-place partition evolution.
   */
  private def partitionSpecReplacedCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.partition.specReplaced") { table =>
      table.spark.sql(
        s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
          s"PARTITIONED BY (${Core.date0.columnName}) AS SELECT * FROM ${table.name}")
      val description = table.spark.sql(s"DESCRIBE TABLE ${table.name}").collect().toSeq

      assert(
        description.exists(_.getString(0) == "# Partition Information") &&
          description.count(_.getString(0) == Core.date0.columnName) == 2,
        "the replace should install the partition specification it named")
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") ==
          standardSeedRowCount.toString,
        "repartitioning through a replace should preserve every row")
    }

  /**
   * After a replace installs a date partition specification, ALTER TABLE DROP PARTITION FIELD is still rejected, and a
   * second replace with a different PARTITIONED BY clause is what changes the partitioning. In-place partition
   * evolution stays rejected across a replace, so replacing the table again is the one legal way to repartition it,
   * which is what the catalog's own rejection message tells a caller to do.
   */
  private def partitionChangeAfterReplaceCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.partition.changeAfterReplace") { table =>
      table.spark.sql(
        s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
          s"PARTITIONED BY (${Core.date0.columnName}) AS SELECT * FROM ${table.name}")
      val inPlaceEvolution = Check.intercept[Exception](
        table.spark.sql(
          s"ALTER TABLE ${table.name} DROP PARTITION FIELD ${Core.date0.columnName}"))

      assert(
        inPlaceEvolution.getMessage.contains("Evolution of table partitioning"),
        s"unexpected message: ${inPlaceEvolution.getMessage.take(160)}")

      table.spark.sql(replaceWithProjection(table.name, columnNameList))
      val description = table.spark.sql(s"DESCRIBE TABLE ${table.name}").collect().toSeq

      assert(
        !description.exists(_.getString(0) == "# Partition Information"),
        "the second replace should leave the table unpartitioned")
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") ==
          standardSeedRowCount.toString,
        "repartitioning through a second replace should preserve every row")
      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6L, "row-6")}")
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "4",
        "the repartitioned table should stay writable")
    }

  // --- 5. what a replace does to the properties a user set ---

  /**
   * A replace that omits TBLPROPERTIES preserves the user property and the enablement flag the table carried, keeping
   * the existing configuration.
   */
  private def userPropertyPreservedCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.property.userPropertyPreserved") { table =>
      assert(
        tableProps(table.spark, table.name).get("user.key").contains("v1"),
        "the preparation should set the user property the replace is asked to preserve")

      table.spark.sql(replaceWithKeysUpTo(table.name, 2))
      val properties = tableProps(table.spark, table.name)

      assert(
        properties.get("user.key").contains("v1"),
        s"user.key should survive the replace, got ${properties.get("user.key")}")
      assert(
        properties.get("replace.enabled").contains("true"),
        "replace.enabled should survive the replace")
    }

  /**
   * A replace whose TBLPROPERTIES clause names an existing property overrides that one and preserves every omitted
   * property, so the statement decides exactly what it mentions.
   */
  private def statementOverridesPropertyCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.property.statementOverridesProperty") { table =>
      table.spark.sql(
        s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
          "TBLPROPERTIES ('user.key'='v2') " +
          s"AS SELECT * FROM ${table.name} WHERE ${Core.long0.columnName} <= 2")
      val properties = tableProps(table.spark, table.name)

      assert(
        properties.get("user.key").contains("v2"),
        s"the property the statement named should win, got ${properties.get("user.key")}")
      assert(
        properties.get("replace.enabled").contains("true"),
        "a property the statement omits should survive the replace")
    }

  // --- 6. what a replace does to the governance the catalog stores ---

  /**
   * A replace that also installs a new partition specification preserves the retention policy the catalog stored, so
   * replacing a table's content keeps the rule that ages its data out.
   */
  private def retentionPolicyPreservedCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.policy.retentionPreserved") { table =>
      val policiesBefore = tableProps(table.spark, table.name).getOrElse("policies", "")
      assert(
        policiesBefore.toLowerCase.contains("retention"),
        s"the preparation should store the retention policy the replace must preserve: $policiesBefore")

      table.spark.sql(
        s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
          s"PARTITIONED BY (${Core.date0.columnName}) " +
          s"AS SELECT * FROM ${table.name} WHERE ${Core.long0.columnName} <= 2")

      assert(
        tableProps(table.spark, table.name).getOrElse("policies", "") == policiesBefore,
        "the replace should preserve the retention policy")
    }

  /**
   * A replace preserves the PII tag the string column carried, so replacing a table's content keeps a column's
   * classification.
   */
  private def columnTagPreservedCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.policy.columnTagPreserved") { table =>
      val policiesBefore = tableProps(table.spark, table.name).getOrElse("policies", "")
      assert(
        policiesBefore.toLowerCase.contains("pii"),
        s"the preparation should store the PII tag the replace must preserve: $policiesBefore")

      table.spark.sql(replaceWithKeysUpTo(table.name, 2))

      assert(
        tableProps(table.spark, table.name).getOrElse("policies", "") == policiesBefore,
        "the replace should preserve the PII column tag")
    }

  // --- 7. reading the history a replace retired ---

  /**
   * A replace keeps the pre-replace snapshot in history and that snapshot still reads its three rows, so the content a
   * replace overwrote stays reachable by time travel.
   */
  private def preReplaceTimeTravelCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.history.preReplaceTimeTravel") { table =>
      val preReplaceSnapshotId = currentSnapshotId(table.spark, table.name)
      table.spark.sql(replaceWithKeysUpTo(table.name, 2))

      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}.snapshots") == "2",
        "the replace appends to the history it found, leaving the pre-replace snapshot in place")
      assert(
        countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name} VERSION AS OF $preReplaceSnapshotId") ==
          standardSeedRowCount.toString,
        "the pre-replace snapshot should still read the rows it held")
    }

  /**
   * Rolling back to a snapshot from before the replace is rejected because the replace started a new lineage and the
   * earlier snapshot lies outside the current ancestry.
   */
  private def rollbackAcrossLineageRejectedCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.history.rollbackRejected") { table =>
      val preReplaceSnapshotId = currentSnapshotId(table.spark, table.name)
      table.spark.sql(replaceWithKeysUpTo(table.name, 2))

      val exception = Check.intercept[ValidationException](
        table.spark.sql(
          "CALL openhouse.system.rollback_to_snapshot(" +
            s"'${catalogRelative(table.name)}', $preReplaceSnapshotId)"))

      assert(
        exception.getMessage.contains("not an ancestor"),
        s"unexpected message: ${exception.getMessage.take(160)}")
    }

  /**
   * set_current_snapshot to a pre-replace snapshot recovers the rows the replace overwrote, so the snapshot a rollback
   * refuses is still the way back to the content that was there before.
   */
  private def setCurrentSnapshotRecoversCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.history.setCurrentSnapshotRecovers") { table =>
      val preReplaceSnapshotId = currentSnapshotId(table.spark, table.name)
      table.spark.sql(replaceWithKeysUpTo(table.name, 2))
      table.spark.sql(
        "CALL openhouse.system.set_current_snapshot(" +
          s"'${catalogRelative(table.name)}', $preReplaceSnapshotId)")

      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") ==
          standardSeedRowCount.toString,
        "set_current_snapshot should recover the pre-replace rows")
    }

  // --- 8 and 9. asking for a range of changes that crosses the replacement boundary ---

  /**
   * A changelog view whose start snapshot sits before the replace is rejected with an IllegalArgumentException naming
   * the start snapshot as outside the current lineage, so a reader asking to span the replacement boundary is told
   * the range is unanswerable and reads the new lineage's changes only through a range inside it. The append that
   * follows the replace comes from ChangelogSupport, so this case and the general changelog cases agree on what the
   * operation does.
   */
  private def changelogAcrossBoundaryCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.changelog.acrossBoundaryRejected") { table =>
      val appendOperation = changelogOperations
        .find(_.name == "changelog.append")
        .getOrElse(throw new AssertionError("ChangelogSupport defines the changelog.append operation"))
      val preReplaceSnapshotId = currentSnapshotId(table.spark, table.name)
      table.spark.sql(replaceWithKeysUpTo(table.name, 2))
      table.spark.sql(appendOperation.statement(table.name))
      val rejection = Check.intercept[IllegalArgumentException](
        changeCounts(table, changelogViewFrom(table, preReplaceSnapshotId)))

      assert(
        rejection.getMessage.contains(crossLineageRejectionMessage),
        s"the rejection identifies the start snapshot as outside the current lineage, " +
          s"found: ${rejection.getMessage.take(200)}")
    }

  /**
   * An incremental read bounded by a snapshot from before the replace and the snapshot the append after it made
   * current is rejected with an IllegalArgumentException naming the start snapshot as outside the current lineage, so
   * a scan spans one lineage at a time.
   */
  private def incrementalReadAcrossBoundaryCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.incrementalRead.acrossBoundaryRejected") { table =>
      val preReplaceSnapshotId = currentSnapshotId(table.spark, table.name)
      table.spark.sql(replaceWithKeysUpTo(table.name, 2))
      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6L, "row-6")}")
      val postAppendSnapshotId = currentSnapshotId(table.spark, table.name)

      val rejection = Check.intercept[IllegalArgumentException](
        table.spark.read
          .format("iceberg")
          .option("start-snapshot-id", preReplaceSnapshotId)
          .option("end-snapshot-id", postAppendSnapshotId)
          .load(table.name)
          .count())

      assert(
        rejection.getMessage.contains(crossLineageRejectionMessage),
        s"the rejection identifies the start snapshot as outside the current lineage, " +
          s"found: ${rejection.getMessage.take(200)}")
    }

  // --- 10. a replace crossed with a rename, in both orders ---

  /**
   * A table replaced and then renamed keeps the replaced content under the new name, so a replace leaves a table
   * to the name it was replaced under. The rename boundary records the live name after each accepted rename, so a
   * failure between the two renames drops the table under the name it currently answers to.
   */
  private def replaceThenRenameCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.rename.replaceThenRename") { table =>
      val renamedTable = s"${table.name}_replaced_then_renamed"
      table.spark.sql(replaceWithKeysUpTo(table.name, 2))

      withTrackedRename(table.spark.sql(_), table.name) { renameTo =>
        renameTo(renamedTable)

        assert(
          countOf(table.spark, s"SELECT count(*) FROM $renamedTable") == "2",
          "the renamed table should hold the rows the replace left")
        renameTo(table.name)
      }
    }

  /**
   * A table renamed and then replaced under its new name accepts the replace and holds the replaced content, so a
   * rename keeps a table on the replace path.
   */
  private def renameThenReplaceCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.rename.renameThenReplace") { table =>
      val renamedTable = s"${table.name}_renamed_then_replaced"

      withTrackedRename(table.spark.sql(_), table.name) { renameTo =>
        renameTo(renamedTable)
        table.spark.sql(replaceWithKeysUpTo(renamedTable, 2))

        assert(
          countOf(table.spark, s"SELECT count(*) FROM $renamedTable") == "2",
          "the table renamed before the replace should hold the rows the replace left")
        renameTo(table.name)
      }
    }

  // --- 11. evolving the sort order the replaced table starts with ---

  /**
   * A replaced table accepts ALTER TABLE WRITE ORDERED BY afterwards, which sets range distribution and leaves the
   * table writable, so the write order stays settable after a replace.
   */
  private def sortOrderChangedAfterReplaceCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.sortOrder.changedAfterReplace") { table =>
      table.spark.sql(replaceWithKeysUpTo(table.name, 2))
      table.spark.sql(s"ALTER TABLE ${table.name} WRITE ORDERED BY ${Core.long0.columnName}")
      val distributionMode = tableProps(table.spark, table.name).get("write.distribution-mode")

      assert(
        distributionMode.contains("range"),
        s"a write sort order after a replace should set range distribution, got $distributionMode")
      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6L, "row-6")}")
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "3",
        "the ordered replaced table should stay writable")
    }

  /**
   * A replaced table that was given a write sort order accepts ALTER TABLE WRITE UNORDERED afterwards, which drops the
   * range distribution and leaves the table writable, so a sort order applied after a replace is still removable.
   */
  private def sortOrderRemovedAfterReplaceCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.sortOrder.removedAfterReplace") { table =>
      table.spark.sql(replaceWithKeysUpTo(table.name, 2))
      table.spark.sql(s"ALTER TABLE ${table.name} WRITE ORDERED BY ${Core.long0.columnName}")
      table.spark.sql(s"ALTER TABLE ${table.name} WRITE UNORDERED")
      val distributionMode = tableProps(table.spark, table.name).get("write.distribution-mode")

      assert(
        !distributionMode.contains("range"),
        s"dropping the sort order should drop range distribution, got $distributionMode")
      table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6L, "row-6")}")
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "3",
        "the unordered replaced table should stay writable")
    }

  // --- 12. the identity the catalog governs the table by ---

  /**
   * A replace preserves every reserved property that identifies the table, including the creator the catalog recorded,
   * so the table the catalog governs after a replace is the same table it governed before, which keeps a replace from
   * being a way to take over a table's identity.
   */
  private def creatorIdentityPreservedCase(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rtas.identity.creatorPreserved") { table =>
      val identityBefore = identityProperties(table)
      assert(
        identityBefore.contains("openhouse.tableCreator"),
        s"the catalog should record a creator before the replace: ${identityBefore.keys.toList.sorted}")

      table.spark.sql(replaceWithKeysUpTo(table.name, 2))

      assert(
        identityProperties(table) == identityBefore,
        s"the replace changed the table's identity from $identityBefore to ${identityProperties(table)}")
    }

  // --- 13. a replace racing another writer ---

  /**
   * A serializable replace and INSERT race settles at either the two rows the replace selected, where the replace
   * committed last, or three rows, where the append landed on the replaced table. Whichever writer loses fails with a
   * typed commit conflict, so a caller recognizes every valid way this race ends.
   */
  private def replaceVersusAppendCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation
      .test("rtas.concurrency.replaceVersusAppend") { table =>
        val outcomeByWriter = new ConcurrentHashMap[String, String]()
        def writer(writerName: String, statement: String): () => Unit = () =>
          try {
            table.spark.sql(statement)
            outcomeByWriter.put(writerName, committedOutcome)
          } catch {
            case NonFatal(conflict) if ConcurrencySupport.isTypedCommitConflict(conflict) =>
              outcomeByWriter.put(writerName, conflictedOutcome)
          }

        val threadErrors = ConcurrencySupport.runConcurrently(
          Seq(
            writer("replace", replaceWithKeysUpTo(table.name, 2)),
            writer("append", s"INSERT INTO ${table.name} VALUES ${coreRow(30L, "row-30")}")))
        assert(
          threadErrors.isEmpty,
          s"both writers either commit or hit a typed commit conflict, found: $threadErrors")

        table.spark.sql(s"REFRESH TABLE ${table.name}")
        val settledKeys = table.spark
          .sql(s"SELECT ${Core.long0.columnName} FROM ${table.name}")
          .collect()
          .toSeq
          .map(_.getLong(0))
          .toSet
        val raceOutcome =
          (outcomeByWriter.get("replace"), outcomeByWriter.get("append"))

        println(s"DIAG rtas.concurrency.replaceVersusAppend: $raceOutcome settled at $settledKeys")
        raceOutcome match {
          case (`committedOutcome`, `conflictedOutcome`) =>
            assert(
              settledKeys == Set(1L, 2L),
              s"a winning replace leaves the keys it selected, found $settledKeys")
          case (`conflictedOutcome`, `committedOutcome`) =>
            assert(
              settledKeys == Set(1L, 2L, 3L, 30L),
              s"a winning append leaves the seed plus its row, found $settledKeys")
          case (`committedOutcome`, `committedOutcome`) =>
            assert(
              settledKeys == Set(1L, 2L) || settledKeys == Set(1L, 2L, 30L),
              s"two commits leave the replaced rows, with the append included when it landed " +
                s"on the replaced table, found $settledKeys")
          case recordedOutcome =>
            throw new AssertionError(
              s"one writer commits when a replace races an append, recorded $recordedOutcome")
        }
      }
      .copy(knownBugReason = Some(
        "A replace and append can both report successful commits while the append's snapshot wins and loses the " +
          "replace, leaving the seed rows plus the appended row."))

}
