package harness

import org.apache.iceberg.exceptions.ValidationException

/**
 * Write-audit-publish configuration: what the `write.wap.enabled` table property controls, what the two session
 * settings mean while it is on, and what the catalog does when the two settings are asked for at once.
 *
 * The property decides whether a write made under `spark.wap.id` commits a snapshot no reference names. With the
 * property off, the identifier carries no meaning and the write commits to `main`; with it on, the same write stays
 * off `main` until a publish. The property gates `spark.wap.branch` the same way, so a session routed at a branch on
 * a table that never set it writes to `main` under the branch's name. The two settings name different destinations
 * for one write, so asking for both is rejected rather than resolved.
 *
 * Operations: ALTER TABLE SET TBLPROPERTIES turning the property on and off, a write made under `spark.wap.id` on
 * each side of the property, a table that declared the property at CREATE, a branch that outlives the property being
 * turned off, a routed write on a table that never set the property, and the rejected session that asks for both
 * settings at once.
 *
 * Preparation axes: file format, and how the table came by its configuration. Each family runs in both columnar
 * formats. The enabling family starts from the standard seeded table; the families that read behavior while the
 * property is on start from the write-audit-publish table; the declared-at-CREATE family starts from the table that
 * set the property in its CREATE statement; and the two families that route a session start from the routed branched
 * table and the branched table, which differ by exactly the property under test.
 *
 * Case families: 7 families over 2 formats, contributing 14 cases.
 */
trait ScenarioWriteAuditPublishConfiguration extends BranchTableFixtures {

  /** Every write-audit-publish configuration case, in the order this file introduces the families. */
  lazy val writeAuditPublishConfigurationCases: List[TestCase] =
    preparedCoreFormats.map(enablingStagesTheNextWrite) ++
      preparedWriteAuditPublishAtCreateTables.map(declaredAtCreateStagesAWrite) ++
      preparedWriteAuditPublishTables.map(disablingKeepsTheStagedSnapshot) ++
      preparedWriteAuditPublishTables.map(disablingCommitsTheNextWriteToMain) ++
      preparedWriteAuditPublishTables.map(disablingKeepsTheBranch) ++
      preparedRoutedBranchTables.map(identifierAndBranchTogetherRejected) ++
      preparedBranchedTables.map(routingWithoutTheFlagWritesToMain)

  // --- the case bodies the surface above composes ---

  /**
   * Before the property is set the table does not carry it, and a write made under `spark.wap.id` commits to `main`.
   * Setting the property persists it as `true` and commits no snapshot, and the next write made under the same
   * identifier commits a snapshot no reference names while `main` reads the rows it already read.
   */
  private def enablingStagesTheNextWrite(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.configuration.enablingStagesTheNextWrite") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      assert(
        persistedProperty(table.spark, table.name, writeAuditPublishProperty).isEmpty,
        s"the seeded table carries no $writeAuditPublishProperty, found " +
          s"${persistedProperty(table.spark, table.name, writeAuditPublishProperty)}")

      stagingUnder(table.spark, "before-enable") {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "unstaged")}")
      }

      assert(
        stagedSnapshotIds(table.spark, table.name, "before-enable").isEmpty,
        s"the identifier stages nothing while the property is unset, found " +
          s"${stagedSnapshotIds(table.spark, table.name, "before-enable")}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows :+ expectedCoreRow(6L, "unstaged"),
        s"$mainBranchName reads the write made while the property is unset, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")

      val enabledRows = rowsOn(table.spark, table.name, mainBranchName)
      val enabledSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)

      table.spark.sql(
        s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('$writeAuditPublishProperty'='true')")

      assert(
        persistedProperty(table.spark, table.name, writeAuditPublishProperty).contains("true"),
        s"the table persists $writeAuditPublishProperty as true, found " +
          s"${persistedProperty(table.spark, table.name, writeAuditPublishProperty)}")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == enabledSnapshotId,
        s"setting the property commits no snapshot, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")

      stagingUnder(table.spark, "after-enable") {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(7, "staged")}")
      }

      assert(
        stagedSnapshotIds(table.spark, table.name, "after-enable").size == 1,
        s"the identifier stages one snapshot while the property is set, found " +
          s"${stagedSnapshotIds(table.spark, table.name, "after-enable")}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == enabledRows,
        s"the staged write leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == enabledSnapshotId,
        s"the staged write leaves $mainBranchName on $enabledSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")
    }

  /**
   * A table that declared the property in its CREATE statement reads it back as `true` and behaves as one that was
   * altered into it: a write made under `spark.wap.id` commits a snapshot no reference names, and `main` reads the
   * rows it already read.
   */
  private def declaredAtCreateStagesAWrite(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.configuration.declaredAtCreateStagesAWrite") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)
      val seededSnapshotId = referenceSnapshotId(table.spark, table.name, mainBranchName)

      assert(
        persistedProperty(table.spark, table.name, writeAuditPublishProperty).contains("true"),
        s"the table declared $writeAuditPublishProperty at CREATE, found " +
          s"${persistedProperty(table.spark, table.name, writeAuditPublishProperty)}")

      stagingUnder(table.spark, "declared") {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "declared")}")
      }

      assert(
        stagedSnapshotIds(table.spark, table.name, "declared").size == 1,
        s"the identifier stages one snapshot, found " +
          s"${stagedSnapshotIds(table.spark, table.name, "declared")}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"the staged write leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) == seededSnapshotId,
        s"the staged write leaves $mainBranchName on $seededSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, mainBranchName)}")
    }

  /**
   * Turning the property off leaves a snapshot that was already staged exactly where it is, so work in flight
   * survives the configuration change and stays publishable.
   */
  private def disablingKeepsTheStagedSnapshot(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.configuration.disablingKeepsTheStagedSnapshot") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      stagingUnder(table.spark, "in-flight") {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "in-flight")}")
      }

      val stagedBeforeDisable = stagedSnapshotIds(table.spark, table.name, "in-flight")

      assert(
        stagedBeforeDisable.size == 1,
        s"the identifier stages one snapshot, found $stagedBeforeDisable")

      table.spark.sql(
        s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('$writeAuditPublishProperty'='false')")

      assert(
        persistedProperty(table.spark, table.name, writeAuditPublishProperty).contains("false"),
        s"the table persists $writeAuditPublishProperty as false, found " +
          s"${persistedProperty(table.spark, table.name, writeAuditPublishProperty)}")
      assert(
        stagedSnapshotIds(table.spark, table.name, "in-flight") == stagedBeforeDisable,
        s"turning the property off keeps the staged snapshot, found " +
          s"${stagedSnapshotIds(table.spark, table.name, "in-flight")}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) == seededRows,
        s"turning the property off leaves the rows $mainBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")

      cherryPick(table.spark, table.name, stagedBeforeDisable.head)

      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows :+ expectedCoreRow(6L, "in-flight"),
        s"the snapshot staged before the change is still publishable, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
    }

  /**
   * With the property off the identifier carries no meaning, so the next write made under `spark.wap.id` commits to
   * `main` and stages nothing.
   */
  private def disablingCommitsTheNextWriteToMain(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.configuration.disablingCommitsTheNextWriteToMain") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      table.spark.sql(
        s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('$writeAuditPublishProperty'='false')")

      stagingUnder(table.spark, "after-disable") {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "after-disable")}")
      }

      assert(
        stagedSnapshotIds(table.spark, table.name, "after-disable").isEmpty,
        s"the identifier stages nothing while the property is off, found " +
          s"${stagedSnapshotIds(table.spark, table.name, "after-disable")}")
      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows :+ expectedCoreRow(6L, "after-disable"),
        s"$mainBranchName reads the write made while the property is off, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
    }

  /**
   * A branch is part of the table rather than part of the property, so turning the property off keeps the branch, its
   * binding and its rows, and the branch goes on accepting writes named through its identifier.
   */
  private def disablingKeepsTheBranch(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.configuration.disablingKeepsTheBranch") { table =>
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      withOwnedReference(table.spark.sql(_), table.name, branchReferenceType, auditBranchName)(
        table.spark.sql(s"ALTER TABLE ${table.name} CREATE BRANCH $auditBranchName")) {
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(6, "before-disable")}")

        val branchSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)

        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('$writeAuditPublishProperty'='false')")

        assert(
          referenceSnapshotId(table.spark, table.name, auditBranchName) == branchSnapshotId,
          s"turning the property off leaves $auditBranchName on $branchSnapshotId, found " +
            s"${referenceSnapshotId(table.spark, table.name, auditBranchName)}")
        assert(
          rowsOn(table.spark, table.name, auditBranchName) ==
            seededRows :+ expectedCoreRow(6L, "before-disable"),
          s"turning the property off keeps the rows $auditBranchName reads, found " +
            s"${rowsOn(table.spark, table.name, auditBranchName)}")

        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_$auditBranchName VALUES ${coreRow(7, "after-disable")}")

        assert(
          rowsOn(table.spark, table.name, auditBranchName) ==
            seededRows ++
              Seq(expectedCoreRow(6L, "before-disable"), expectedCoreRow(7L, "after-disable")),
          s"$auditBranchName accepts a write after the property is off, found " +
            s"${rowsOn(table.spark, table.name, auditBranchName)}")
        assert(
          rowsOn(table.spark, table.name, mainBranchName) == seededRows,
          s"the branch writes leave the rows $mainBranchName reads, found " +
            s"${rowsOn(table.spark, table.name, mainBranchName)}")
      }
    }

  /**
   * The two settings name different destinations for one write, so a session that sets both is rejected as a
   * validation failure naming the identifier and the branch it was given. Nothing is staged, and both references keep
   * their rows.
   */
  private def identifierAndBranchTogetherRejected(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.configuration.identifierAndBranchTogether.rejected") {
      table =>
        val referencesBefore = referenceEntries(table.spark, table.name)
        val seededRows = rowsOn(table.spark, table.name, mainBranchName)
        val rejection = stagingUnder(table.spark, "conflicting") {
          routedAt(table.spark, auditBranchName) {
            Check.intercept[ValidationException](
              table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "conflicting")}"))
          }
        }

        assert(
          rejection.getMessage.contains("Cannot set both WAP ID and branch"),
          s"the rejection names the conflict between the two settings, found " +
            s"${rejection.getMessage.take(160)}")
        assert(
          stagedSnapshotIds(table.spark, table.name, "conflicting").isEmpty,
          s"the rejected write stages nothing, found " +
            s"${stagedSnapshotIds(table.spark, table.name, "conflicting")}")
        assert(
          referenceEntries(table.spark, table.name) == referencesBefore,
          s"the rejected write leaves every reference where it was, found " +
            s"${referenceEntries(table.spark, table.name)}")
        assert(
          rowsOn(table.spark, table.name, mainBranchName) == seededRows &&
            rowsOn(table.spark, table.name, auditBranchName) == seededRows,
          "the rejected write leaves both references reading their rows")
    }

  /**
   * `spark.wap.branch` is read through the same property that governs staging, so a session routed at a branch on a
   * table that never set `write.wap.enabled` writes to `main` instead of the branch it named. The branch keeps its
   * binding and its rows, and `main` reads the row the routed session wrote, which is what a caller has to account
   * for before it relies on routing.
   */
  private def routingWithoutTheFlagWritesToMain(
      preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("writeAuditPublish.configuration.routingWithoutTheFlagWritesToMain") { table =>
      val branchPointSnapshotId = referenceSnapshotId(table.spark, table.name, auditBranchName)
      val seededRows = rowsOn(table.spark, table.name, mainBranchName)

      assert(
        persistedProperty(table.spark, table.name, writeAuditPublishProperty).isEmpty,
        s"the branched table carries no $writeAuditPublishProperty, found " +
          s"${persistedProperty(table.spark, table.name, writeAuditPublishProperty)}")

      routedAt(table.spark, auditBranchName) {
        table.spark.sql(s"INSERT INTO ${table.name} VALUES ${coreRow(6, "unflagged-routing")}")
      }

      assert(
        rowsOn(table.spark, table.name, mainBranchName) ==
          seededRows :+ expectedCoreRow(6L, "unflagged-routing"),
        s"$mainBranchName reads the routed write, found " +
          s"${rowsOn(table.spark, table.name, mainBranchName)}")
      assert(
        rowsOn(table.spark, table.name, auditBranchName) == seededRows,
        s"the routed write leaves the rows $auditBranchName reads, found " +
          s"${rowsOn(table.spark, table.name, auditBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, auditBranchName) == branchPointSnapshotId,
        s"$auditBranchName still names $branchPointSnapshotId, found " +
          s"${referenceSnapshotId(table.spark, table.name, auditBranchName)}")
      assert(
        referenceSnapshotId(table.spark, table.name, mainBranchName) != branchPointSnapshotId,
        s"$mainBranchName moves off $branchPointSnapshotId to carry the routed write")
    }

}
