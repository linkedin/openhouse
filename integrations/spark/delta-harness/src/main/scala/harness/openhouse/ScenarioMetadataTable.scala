package harness

/**
 * Metadata tables: the hidden metadata columns a scan exposes and the Iceberg metadata tables the catalog serves
 * alongside every table.
 *
 * Operations: select the hidden _file, _pos, _spec_id and _partition columns; query every metadata table the catalog
 * serves (entries, files, manifests, snapshots, history, refs, partitions, metadata_log_entries, data_files and the
 * all_* variants); and read the snapshot, history, files and manifests counts of a two-snapshot table.
 *
 * Preparation axes: in each of the two columnar formats, the standard seeded core table for the hidden-column family
 * and the two-snapshot core table for the two families that count metadata rows.
 *
 * Case families: three families contributing 6 cases.
 */
trait ScenarioMetadataTable extends ScenarioKit {

  /** Every metadata-table case, one file format at a time. */
  lazy val metadataTableCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        hiddenColumnsCase(preparedStandardTable(format)),
        tableSweepCase(preparedTwoSnapshotTable(format)),
        snapshotAndHistoryCase(preparedTwoSnapshotTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * Selecting the hidden metadata columns _file, _pos, _spec_id and _partition returns one row per seed row, each with
   * a populated file path and a non-negative position.
   */
  private def hiddenColumnsCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("metadata.hiddenColumns") { table =>
      val rows = table.spark
        .sql(s"SELECT _file, _pos, _spec_id, _partition FROM ${table.name}")
        .collect()
        .toSeq

      assert(
        rows.size == 3,
        s"hidden metadata columns should return 3 rows, got ${rows.size}")
      assert(
        rows.forall(row => Option(row.getString(0)).exists(_.nonEmpty)),
        "_file should be populated for every row")
      assert(
        rows.forall(_.getLong(1) >= 0),
        "_pos should be non-negative for every row")
    }

  /**
   * Every Iceberg metadata table is queryable without error, and the snapshots metadata table reports the table's 2
   * snapshots.
   */
  private def tableSweepCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("metadata.tableSweep") { table =>
      val metadataTables = Seq(
        "entries",
        "files",
        "manifests",
        "snapshots",
        "history",
        "refs",
        "partitions",
        "metadata_log_entries",
        "data_files",
        "all_data_files",
        "all_manifests",
        "all_entries",
        "all_files")
      metadataTables.foreach { metadataTable =>
        table.spark.sql(s"SELECT count(*) FROM ${table.name}.`$metadataTable`").collect()
      }
      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}.snapshots") == "2",
        "snapshot metadata should contain two snapshots")
    }

  /**
   * The snapshots and history metadata tables each report the table's 2 snapshots, and the files and manifests
   * metadata tables each report at least 1 row.
   */
  private def snapshotAndHistoryCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("metadata.snapshotAndHistory") { table =>
      def metadataRowCount(metadataTable: String): Long =
        table.spark
          .sql(s"SELECT count(*) FROM ${table.name}.$metadataTable")
          .collect()(0)
          .getLong(0)

      assert(metadataRowCount("snapshots") == 2)
      assert(metadataRowCount("history") == 2)
      assert(metadataRowCount("files") >= 1 && metadataRowCount("manifests") >= 1)
    }

}
