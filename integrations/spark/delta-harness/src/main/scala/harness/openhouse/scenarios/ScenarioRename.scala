package harness

import org.apache.spark.sql.{AnalysisException, Row}

import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException

/**
 * Table rename: ALTER TABLE RENAME TO moves a table to a new alias with its full identity intact, the moved table keeps
 * accepting writes, and the catalog refuses a rename onto a name that is already taken while leaving both tables
 * exactly as they were.
 *
 * Operations: RENAME TO a free alias, a read of the moved table's rows, schema, UUID, snapshots and metadata, a
 * follow-up insert, then a rename back; and RENAME TO the name of a table that already exists.
 *
 * Preparation axes: the standard seeded core table in each of the two columnar formats. The conflict family creates the
 * table it collides with under an owned lifecycle and drops only that table.
 *
 * Case families: two families contributing 4 cases.
 */
trait ScenarioRename extends CatalogDdlSupport {

  /** Every rename case, one file format at a time. */
  lazy val renameCases: List[TestCase] =
    fileFormats.flatMap { format =>
      List(
        renameTableCase(preparedStandardTable(format)),
        renameTableConflictCase(preparedStandardTable(format), format))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  // The full observable identity of a table: its rows in key order, its Iceberg schema, the OpenHouse table UUID, the
  // current snapshot and its ancestry, the partition spec and sort order, and every stored property. A rename keeps all
  // of it except the alias-derived and commit-bookkeeping properties; a rejected rename keeps all of it.
  private final case class TableIdentity(
    rows: Seq[Row],
    schema: String,
    tableUuid: String,
    currentSnapshotId: Option[Long],
    snapshotLineage: Seq[Long],
    partitionSpec: String,
    sortOrder: String,
    properties: Map[String, String])

  // The property keys a rename updates: OpenHouse rewrites the table id, URI and location to the new alias, and the
  // rename commit advances the last-modified time and the table version. Every other property, the table UUID
  // included, survives the move unchanged.
  private val renameMutatedPropertyKeys =
    Set(
      "openhouse.tableId",
      "openhouse.tableUri",
      "openhouse.tableLocation",
      "openhouse.lastModifiedTime",
      "openhouse.tableVersion")

  private def captureIdentity(spark: org.apache.spark.sql.SparkSession, table: String): TableIdentity = {
    val icebergTable = icebergTableOf(spark, table)
    val properties = tableProps(spark, table)
    val currentSnapshot = Option(icebergTable.currentSnapshot()).map(_.snapshotId())
    TableIdentity(
      spark.sql(s"SELECT $columnNameList FROM $table ORDER BY ${Core.long0.columnName}").collect().toSeq,
      icebergTable.schema().asStruct().toString,
      properties.getOrElse("openhouse.tableUUID", ""),
      currentSnapshot,
      if (currentSnapshot.isDefined) snapshotIds(spark, table) else Seq.empty,
      icebergTable.spec().toString,
      icebergTable.sortOrder().toString,
      properties)
  }

  private def shortName(table: String): String = table.substring(table.lastIndexOf('.') + 1)

  /**
   * ALTER TABLE RENAME TO moves the table to a new alias with its rows, Iceberg schema, UUID, current snapshot,
   * snapshot lineage, partition spec, sort order and every non-name property intact, while the table id and URI follow
   * the alias. A follow-up insert commits a new snapshot and lands the extra row, proving the moved table stays
   * writable. A second rename returns the table to its original name, which teardown drops. The rename boundary records
   * the live name after each accepted rename, so a failure between the renames drops the table under the name it
   * currently answers to.
   */
  private def renameTableCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("rename.table") { table =>
      val renamedTable = s"${table.name}_ren"
      val before = captureIdentity(table.spark, table.name)

      withTrackedRename(table.spark.sql(_), table.name) { renameTo =>
        renameTo(renamedTable)
        val afterRename = captureIdentity(table.spark, renamedTable)

        assert(afterRename.rows == before.rows, "the rename should preserve the exact rows")
        assert(afterRename.schema == before.schema, "the rename should preserve the Iceberg schema")
        assert(
          afterRename.tableUuid == before.tableUuid && afterRename.tableUuid.nonEmpty,
          "the rename should preserve the table UUID identity")
        assert(
          afterRename.currentSnapshotId == before.currentSnapshotId,
          "the rename should preserve the current snapshot")
        assert(
          afterRename.snapshotLineage == before.snapshotLineage,
          "the rename should preserve the snapshot lineage")
        assert(
          afterRename.partitionSpec == before.partitionSpec,
          "the rename should preserve the partition spec")
        assert(afterRename.sortOrder == before.sortOrder, "the rename should preserve the sort order")
        assert(
          before.properties -- renameMutatedPropertyKeys ==
            afterRename.properties -- renameMutatedPropertyKeys,
          "the rename should preserve every property except the alias, location and commit bookkeeping")
        assert(
          afterRename.properties.get("openhouse.tableId").contains(shortName(renamedTable)),
          "the rename should move the table id to the new alias")
        assert(
          afterRename.properties.get("openhouse.tableUri").exists(_.contains(shortName(renamedTable))),
          "the rename should move the table URI to the new alias")

        Check.intercept[AnalysisException](
          table.spark.sql(s"SELECT 1 FROM ${table.name} LIMIT 1"))

        table.spark.sql(s"INSERT INTO $renamedTable VALUES ${coreRow(4, "row-4")}")
        val afterInsert = captureIdentity(table.spark, renamedTable)

        assert(
          afterInsert.rows.dropRight(1) == before.rows,
          "the insert should keep the moved rows and append after them")
        assert(
          afterInsert.rows.last.getLong(0) == 4L && afterInsert.rows.last.getString(2) == "row-4",
          "the insert should land the new row at the new alias")
        assert(
          afterInsert.currentSnapshotId.isDefined &&
            afterInsert.currentSnapshotId != afterRename.currentSnapshotId,
          "the insert should commit a new snapshot")
        assert(
          afterInsert.snapshotLineage == afterRename.snapshotLineage :+ afterInsert.currentSnapshotId.get,
          "the insert should extend the lineage with exactly the new snapshot")

        renameTo(table.name)
      }
    }

  /**
   * ALTER TABLE RENAME TO a name that already exists is rejected with an error naming the conflict, and both tables
   * keep their full identity: rows, schema, UUID, snapshots, metadata and properties. The source is seeded by the
   * preparation and the target is created and seeded under an owned lifecycle whose name extends the source table's
   * generated unique suffix, so the case drops only the table it created and never pre-drops a name it does not own.
   */
  private def renameTableConflictCase(
      preparation: TablePreparation[CoreTable.type],
      format: String): TestCase =
    preparation.test("rename.table.conflict") { table =>
      val conflictingTable = s"${table.name}_other"

      withOwnedTable(table.spark.sql(_), conflictingTable)(
        table.spark.sql(coreCreate(conflictingTable, format))) {
        table.spark.sql(
          s"INSERT INTO $conflictingTable VALUES ${coreRow(7, "row-7")}, ${coreRow(8, "row-8")}")
        val sourceBefore = captureIdentity(table.spark, table.name)
        val targetBefore = captureIdentity(table.spark, conflictingTable)

        val exception = Check.intercept[WebClientResponseWithMessageException](
          table.spark.sql(s"ALTER TABLE ${table.name} RENAME TO $conflictingTable"))

        assert(
          exception.getMessage.contains("already exists"),
          s"unexpected message: ${exception.getMessage.take(160)}")
        assert(
          captureIdentity(table.spark, table.name) == sourceBefore,
          "the rejected rename should leave the source table exactly unchanged")
        assert(
          captureIdentity(table.spark, conflictingTable) == targetBefore,
          "the rejected rename should leave the pre-existing target table exactly unchanged")
      }
    }

}
