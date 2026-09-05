package harness

/**
 * Branch writes and the isolation they keep: which effects a write aimed at a branch keeps to that branch, and which
 * ones the table shares with every reader of `main`.
 *
 * Row data belongs to the snapshot a reference names, so a write aimed at a branch advances that branch alone.
 * Schema and table properties belong to the table, so a change made while a session is routed at a branch is part of
 * the table every reference reads through. Each case proves both sides: the exact rows and the exact snapshot each
 * reference names afterwards.
 *
 * Operations: an INSERT through the `branch_audit` identifier, a DataFrame `writeTo(...).append()` at the same
 * identifier, a branch DELETE followed by a branch UPDATE, and the INSERT, UPDATE, DELETE, INSERT OVERWRITE and
 * MERGE a session routed through `spark.wap.branch` performs. Alongside them, the ADD COLUMN and SET TBLPROPERTIES a
 * routed session issues, both of which the table shares.
 *
 * Preparation axes: file format, and whether the session routes through `spark.wap.branch`. Each family runs in both
 * columnar formats. The families that name the branch in the statement start from the branched table; the families
 * that route the session start from the routed branched table, which also carries `write.wap.enabled`, the property
 * the catalog requires before it accepts a routed write.
 *
 * Case families: 9 families over 2 formats, contributing 18 cases.
 */
trait ScenarioBranchWrite extends BranchTableFixtures {
  import Rows._

  /** Every branch-write case, in the order this file introduces the families. */
  lazy val branchWriteCases: List[TestCase] =
    preparedBranchedTables.map(insertThroughTheBranchIdentifier) ++
      preparedBranchedTables.map(dataFrameAppendToTheBranch) ++
      preparedBranchedTables.map(deleteAndUpdateOnTheBranch) ++
      preparedRoutedBranchTables.map(routedInsert) ++
      preparedRoutedBranchTables.map(routedUpdateAndDelete) ++
      preparedRoutedBranchTables.map(routedOverwrite) ++
      preparedRoutedBranchTables.map(routedMerge) ++
      preparedRoutedBranchTables.map(routedAddColumnIsTableGlobal) ++
      preparedRoutedBranchTables.map(routedTablePropertyIsTableGlobal)

  // --- the case bodies the surface above composes ---

  /**
   * An INSERT through the `branch_audit` identifier appends its row to the branch. The branch names a new snapshot
   * that descends from the one both references shared, and `main` still names that shared snapshot and reads the
   * rows it read.
   */
  private def insertThroughTheBranchIdentifier(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.write.insertThroughTheBranchIdentifier") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(6, "branch-insert")}")

      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          seededRows :+ expectedCoreRow(6L, "branch-insert"),
        s"$auditBranchName reads its seeded rows and the inserted row, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the branch insert leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == branchPointSnapshotId,
        s"$mainBranchName still names $branchPointSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")
      val branchHeadSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)

      assert(
        parentSnapshotId(table.spark, table.name, branchHeadSnapshotId)
          .contains(branchPointSnapshotId),
        s"the branch snapshot descends from $branchPointSnapshotId, found " +
          s"${parentSnapshotId(table.spark, table.name, branchHeadSnapshotId)}")
      assert(
        retainedSnapshotIds(table.spark, table.name).size == 2,
        s"the branch insert commits one snapshot beside the seed, found " +
          s"${retainedSnapshotIds(table.spark, table.name)}")
    }

  /**
   * A DataFrame `writeTo(...).append()` at the `branch_audit` identifier appends its row to the branch, so a job that
   * writes through the DataFrame writer reaches a branch the same way a SQL INSERT does. `main` keeps its binding and
   * its rows.
   */
  private def dataFrameAppendToTheBranch(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.write.dataFrameAppendToTheBranch") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      table.spark
        .sql(
          s"SELECT CAST(6 AS BIGINT) AS ${Core.long0.columnName}, " +
            s"6 AS ${Core.int0.columnName}, " +
            s"'dataframe-append' AS ${Core.string0.columnName}, " +
            s"6.5 AS ${Core.double0.columnName}, " +
            s"true AS ${Core.boolean0.columnName}, " +
            s"'2024-01-01-05' AS ${Core.date0.columnName}")
        .writeTo(s"${table.name}.branch_$auditBranchName")
        .append()

      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          seededRows :+ expectedCoreRow(6L, "dataframe-append"),
        s"$auditBranchName reads its seeded rows and the appended row, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the DataFrame append leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == branchPointSnapshotId,
        s"$mainBranchName still names $branchPointSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")
    }

  /**
   * A DELETE and then an UPDATE through the `branch_audit` identifier change the branch alone. The branch drops the
   * row the DELETE matched and reads the value the UPDATE wrote, `main` reads its seeded rows, and the branch has
   * committed exactly the two snapshots the two statements produced.
   */
  private def deleteAndUpdateOnTheBranch(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.write.deleteAndUpdateOnTheBranch") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      table.spark.sql(
        s"DELETE FROM ${table.name}.branch_$auditBranchName WHERE ${Core.long0.columnName} = 2")
      table.spark.sql(
        s"UPDATE ${table.name}.branch_$auditBranchName SET ${Core.string0.columnName} = 'audited' " +
          s"WHERE ${Core.long0.columnName} = 1")

      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          seededRows
            .filter(row => row.get(Core.long0) != 2L)
            .map(row =>
              if (row.get(Core.long0) == 1L) withColumnValue(row, Core.string0, "audited") else row),
        s"$auditBranchName reads the deleted and updated rows, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the branch mutations leave the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == branchPointSnapshotId,
        s"$mainBranchName still names $branchPointSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")
      assert(
        retainedSnapshotIds(table.spark, table.name).size == 3,
        s"the two branch mutations commit two snapshots beside the seed, found " +
          s"${retainedSnapshotIds(table.spark, table.name)}")
    }

  /**
   * A session routed through `spark.wap.branch` writes and reads at the branch it names: the INSERT lands on the
   * branch and the read that follows it inside the routed scope returns the branch rows. Outside the scope the same
   * read returns the rows `main` holds.
   */
  private def routedInsert(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.write.routedInsert") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)
      val routedRows = routedAt(table.spark, auditBranchName) {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "routed-insert")}")
        PreparedTable.currentRows(table.spark, table.name, Core)
      }
      val unroutedRows = PreparedTable.currentRows(table.spark, table.name, Core)

      assert(
        routedRows == seededRows :+ expectedCoreRow(6L, "routed-insert"),
        s"the routed read returns the rows the routed write produced, found $routedRows")
      assert(
        rowsOn(table.spark, table.name, auditBranchName) == routedRows,
        s"$auditBranchName reads what the routed session wrote, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        unroutedRows == seededRows,
        s"the unrouted read returns the rows $mainBranchName holds, found $unroutedRows")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == branchPointSnapshotId,
        s"$mainBranchName still names $branchPointSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")
    }

  /**
   * An unqualified UPDATE and an unqualified DELETE issued by a routed session are resolved by `spark.wap.branch`
   * alone, so each one lands on the branch the session names. The branch reads the value the UPDATE wrote and then
   * loses the row the DELETE matched, each statement advances the branch by one snapshot that descends from the one
   * before it, and `main` keeps the snapshot it named and the rows it read throughout. The routing setting is clear
   * once each scope ends, whether the scope returned or raised.
   */
  private def routedUpdateAndDelete(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.write.routedUpdateAndDelete") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      routedAt(table.spark, auditBranchName) {
        table.spark.sql(
          s"UPDATE ${table.name} SET ${Core.string0.columnName} = 'routed-update' " +
            s"WHERE ${Core.long0.columnName} = 1")
      }

      val updateSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)

      assert(
        table.spark.conf.getOption(writeAuditPublishBranchSetting).isEmpty,
        s"the routed scope leaves $writeAuditPublishBranchSetting clear, found " +
          s"${table.spark.conf.getOption(writeAuditPublishBranchSetting)}")
      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          seededRows.map(row =>
            if (row.get(Core.long0) == 1L) withColumnValue(row, Core.string0, "routed-update")
            else row),
        s"$auditBranchName reads the value the routed update wrote, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        parentSnapshotId(table.spark, table.name, updateSnapshotId)
          .contains(branchPointSnapshotId),
        s"the routed update advances $auditBranchName from $branchPointSnapshotId, found " +
          s"${parentSnapshotId(table.spark, table.name, updateSnapshotId)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows &&
          referenceSnapshotId(table.spark, table.name, mainBranchName) == branchPointSnapshotId,
        s"the routed update leaves $mainBranchName on $branchPointSnapshotId reading its rows, " +
          s"found ${rowsOn(table.spark, table.name, mainBranchName)} on " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")

      routedAt(table.spark, auditBranchName) {
        table.spark.sql(s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 2")
      }

      val deleteSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)

      assert(
        table.spark.conf.getOption(writeAuditPublishBranchSetting).isEmpty,
        s"the second routed scope leaves $writeAuditPublishBranchSetting clear, found " +
          s"${table.spark.conf.getOption(writeAuditPublishBranchSetting)}")
      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          seededRows
            .filter(row => row.get(Core.long0) != 2L)
            .map(row =>
              if (row.get(Core.long0) == 1L) withColumnValue(row, Core.string0, "routed-update")
              else row),
        s"$auditBranchName loses the row the routed delete matched, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        parentSnapshotId(table.spark, table.name, deleteSnapshotId).contains(updateSnapshotId),
        s"the routed delete advances $auditBranchName from $updateSnapshotId, found " +
          s"${parentSnapshotId(table.spark, table.name, deleteSnapshotId)}")
      assert(
        retainedSnapshotIds(table.spark, table.name).size == 3,
        s"the two routed statements commit two snapshots beside the seed, found " +
          s"${retainedSnapshotIds(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows &&
          referenceSnapshotId(table.spark, table.name, mainBranchName) == branchPointSnapshotId,
        s"the routed delete leaves $mainBranchName on $branchPointSnapshotId reading its rows, " +
          s"found ${rowsOn(table.spark, table.name, mainBranchName)} on " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")

      Check.intercept[IllegalStateException](
        routedAt(table.spark, auditBranchName)(throw new IllegalStateException("routed body fails")))

      assert(
        table.spark.conf.getOption(writeAuditPublishBranchSetting).isEmpty,
        s"a routed scope whose body raises leaves $writeAuditPublishBranchSetting clear, found " +
          s"${table.spark.conf.getOption(writeAuditPublishBranchSetting)}")
    }

  /**
   * An INSERT OVERWRITE issued by a routed session replaces the rows the branch reads and leaves the rows `main`
   * reads, so the widest row-replacing write still stops at the branch it was routed to.
   */
  private def routedOverwrite(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.write.routedOverwrite") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      routedAt(table.spark, auditBranchName) {
        table.spark.sql(s"INSERT OVERWRITE ${table.name} VALUES ${coreRow(6, "routed-overwrite")}")
      }

      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          Seq(expectedCoreRow(6L, "routed-overwrite")),
        s"$auditBranchName reads the overwritten row alone, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the routed overwrite leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == branchPointSnapshotId,
        s"$mainBranchName still names $branchPointSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")
    }

  /**
   * A MERGE issued by a routed session applies its matched update and its unmatched insert to the branch. `main`
   * keeps the rows it read and the snapshot it named.
   */
  private def routedMerge(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.write.routedMerge") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      routedAt(table.spark, auditBranchName) {
        table.spark.sql(
          s"MERGE INTO ${table.name} AS target USING " +
            "(SELECT CAST(1 AS BIGINT) AS key UNION ALL SELECT CAST(6 AS BIGINT) AS key) AS source " +
            s"ON target.${Core.long0.columnName} = source.key " +
            s"WHEN MATCHED THEN UPDATE SET target.${Core.string0.columnName} = 'routed-merge' " +
            s"WHEN NOT MATCHED THEN INSERT ($columnNameList) VALUES " +
            "(source.key, 6, 'routed-merge', 6.5, true, '2024-01-01-05')")
      }

      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          seededRows.map(row =>
            if (row.get(Core.long0) == 1L) withColumnValue(row, Core.string0, "routed-merge")
            else row) :+ expectedCoreRow(6L, "routed-merge"),
        s"$auditBranchName reads the merged rows, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the routed merge leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == branchPointSnapshotId,
        s"$mainBranchName still names $branchPointSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")
    }

  /**
   * ADD COLUMN issued by a routed session changes the schema the table presents, so `main` and the branch both read
   * the added column and the rows written before it read null for it. The branch then accepts a write that fills the
   * added column, and `main` keeps its rows.
   */
  private def routedAddColumnIsTableGlobal(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.write.routedAddColumnIsTableGlobal") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      routedAt(table.spark, auditBranchName) {
        table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMN audit_extra int")
      }

      assert(
        schemaColumnNames(table.spark, table.name) == Core.columnNames :+ "audit_extra",
        s"the routed ADD COLUMN extends the schema the table presents, found " +
          s"${schemaColumnNames(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the routed ADD COLUMN leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        table.spark
          .sql(s"SELECT count(*) FROM ${table.name} WHERE audit_extra IS NULL")
          .collect()(0)
          .getLong(0) == standardSeedRowCount.toLong,
        "every row written before the routed ADD COLUMN reads null for it")

      table.spark.sql(
        s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES " +
          "(CAST(6 AS BIGINT), 6, 'wider-write', 6.5, true, '2024-01-01-05', 42)")

      assert(
        rowsOn(table.spark, table.name, auditBranchName) ==
          seededRows :+ expectedCoreRow(6L, "wider-write"),
        s"$auditBranchName reads the row written at the added arity, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        table.spark
          .sql(
            s"SELECT audit_extra FROM ${table.name} VERSION AS OF '$auditBranchName' " +
              s"WHERE ${Core.long0.columnName} = 6")
          .collect()
          .toSeq
          .map(_.getInt(0)) == Seq(42),
        "the branch row carries the value written into the added column")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the widened branch write leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
    }

  /**
   * SET TBLPROPERTIES issued by a routed session changes the table's own properties, so the value is the one the
   * table reports through `main`. The change commits no snapshot and leaves both references reading the rows they
   * read.
   */
  private def routedTablePropertyIsTableGlobal(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("branch.write.routedTablePropertyIsTableGlobal") { table =>
      val referencesBefore = referenceEntries(table.spark, table.name)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      routedAt(table.spark, auditBranchName) {
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('user.audit.owner'='branch-writer')")
      }

      assert(
        persistedProperty(table.spark, table.name, "user.audit.owner").contains("branch-writer"),
        s"the routed property change is the value the table reports, found " +
          s"${persistedProperty(table.spark, table.name, "user.audit.owner")}")
      assert(
        referenceEntries(table.spark, table.name) == referencesBefore,
        s"the routed property change leaves every reference where it was, found " +
          s"${referenceEntries(table.spark, table.name)}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows &&
          rowsOn(table.spark, table.name, auditBranchName) == seededRows,
        "the routed property change leaves both references reading their rows")
    }

}
