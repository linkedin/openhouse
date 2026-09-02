package harness

import java.nio.file.{Files, Paths}
import java.nio.file.attribute.FileTime

/**
 * Maintenance: the procedures that rewrite a table's files and metadata without changing the rows a reader sees.
 *
 * Operations: expire_snapshots down to the newest snapshot, rewrite_data_files over the table's data files,
 * remove_orphan_files over the table's directory, rewrite_manifests over a fragmented manifest list, remove_orphan_
 * files against a planted backdated stray file, and rewrite_data_files across an ADD COLUMN.
 *
 * Preparation axes: in each of the two columnar formats, the two-snapshot core table for the three procedures that run
 * over an existing history, an unseeded core table for the manifest family, which fragments the manifest list itself,
 * and the standard seeded core table for the planted-orphan and schema-evolution families.
 *
 * Case families: six families contributing 12 cases.
 */
trait ScenarioMaintenance extends ScenarioKit {

  /** Every maintenance case, one file format at a time. */
  lazy val maintenanceCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        expireSnapshotsCase(preparedTwoSnapshotTable(format)),
        rewriteDataFilesCase(preparedTwoSnapshotTable(format)),
        removeOrphanFilesCase(preparedTwoSnapshotTable(format)),
        rewriteManifestsCase(preparedEmptyStandardTable(format)),
        removeOrphanFilesPlantedCase(preparedStandardTable(format)),
        rewriteDataFilesAfterAddColumnCase(preparedStandardTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** expire_snapshots with retain_last=1 removes the seed snapshot and leaves all 5 current rows unchanged. */
  private def expireSnapshotsCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("maintenance.expireSnapshots") { table =>
      table.spark.sql(
        "CALL openhouse.system.expire_snapshots(" +
          s"table => '${catalogRelative(table.name)}', " +
          "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
          "retain_last => 1)")

      assert(
        table.rows.size == 5,
        "expire_snapshots changed the current data")
      assert(
        table.snapshotCount < table.preparedSnapshotCount,
        "expire_snapshots did not remove a snapshot: " +
          s"${table.preparedSnapshotCount} -> ${table.snapshotCount}")
    }

  /** rewrite_data_files compacts the data files and leaves all 5 rows unchanged. */
  private def rewriteDataFilesCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("maintenance.rewriteDataFiles") { table =>
      table.spark.sql(
        "CALL openhouse.system.rewrite_data_files(" +
          s"table => '${catalogRelative(table.name)}')")

      assert(table.rows.size == 5, "compaction changed rows")
    }

  /** remove_orphan_files over a table with no stray files leaves all 5 rows unchanged. */
  private def removeOrphanFilesCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("maintenance.removeOrphanFiles") { table =>
      table.spark.sql(
        "CALL openhouse.system.remove_orphan_files(" +
          s"table => '${catalogRelative(table.name)}', " +
          "older_than => TIMESTAMP '2020-01-01 00:00:00')")

      assert(table.rows.size == 5, "orphan removal changed rows")
    }

  /**
   * After 5 single-row inserts fragment the manifest list, rewrite_manifests compacts it to fewer manifests while
   * preserving all 5 rows.
   */
  private def rewriteManifestsCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("maintenance.rewriteManifests") { table =>
      (1 to 5).foreach(index =>
        table.spark.sql(
          s"INSERT INTO ${table.name} VALUES " +
            coreRow(index, s"r$index")))
      val manifestCountBefore = table.spark
        .sql(s"SELECT count(*) FROM ${table.name}.manifests")
        .collect()(0)
        .getLong(0)
      table.spark.sql(
        "CALL openhouse.system.rewrite_manifests(" +
          s"table => '${catalogRelative(table.name)}', " +
          "use_caching => false)")
      val manifestCountAfter = table.spark
        .sql(s"SELECT count(*) FROM ${table.name}.manifests")
        .collect()(0)
        .getLong(0)

      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "5",
        "rewrite_manifests should preserve the five rows")
      assert(
        manifestCountBefore >= 2 &&
          manifestCountAfter < manifestCountBefore,
        "rewrite_manifests should compact the manifest set: " +
          s"before=$manifestCountBefore after=$manifestCountAfter")
    }

  /**
   * remove_orphan_files deletes a planted, backdated stray file next to a real data file while the table's 3 live rows
   * remain intact.
   */
  private def removeOrphanFilesPlantedCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("maintenance.removeOrphanFiles.planted") { table =>
      val dataFile = table.spark
        .sql(s"SELECT file_path FROM ${table.name}.files LIMIT 1")
        .collect()(0)
        .getString(0)
        .stripPrefix("file:")
      val orphanFile = Paths
        .get(dataFile)
        .getParent
        .resolve(s"${table.name.split('.').last}_orphan.parquet")
      Files.write(orphanFile, "not-a-real-parquet".getBytes)
      Files.setLastModifiedTime(orphanFile, FileTime.fromMillis(1546300800000L))

      table.spark.sql(
        "CALL openhouse.system.remove_orphan_files(" +
          s"table => '${catalogRelative(table.name)}', " +
          "older_than => TIMESTAMP '2020-01-01 00:00:00')")
      assert(
        Files.notExists(orphanFile),
        "remove_orphan_files should delete the planted orphan")
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "3",
        "remove_orphan_files should preserve live data")
    }

  /**
   * Compacting a table after an ADD COLUMN and inserts into the new column preserves all rows, the new column's
   * non-null values, and null for rows written before the column was added.
   */
  private def rewriteDataFilesAfterAddColumnCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("maintenance.rewriteDataFiles.afterAddColumn") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColInsert9")
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColInsert10")
      table.spark.sql(
        "CALL openhouse.system.rewrite_data_files(" +
          s"table => '${catalogRelative(table.name)}')")

      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "5",
        "compaction should preserve 5 rows")
      assert(
        countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name} WHERE extra_col IN (42, 43)") == "2",
        "compaction should preserve both evolved values")
      assert(
        countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name} WHERE extra_col IS NULL") == "3",
        "pre-evolution rows should remain null")
    }

}
