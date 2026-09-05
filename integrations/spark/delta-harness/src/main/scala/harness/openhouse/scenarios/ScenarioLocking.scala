package harness

import org.apache.spark.sql.SparkSession

/**
 * Table locking: while a table carries a REST lock, the catalog rejects the commits that would change it, and
 * deleting the lock lets them through again.
 *
 * Operations: POST the lock endpoint, then run an UPDATE and an expire_snapshots call against the locked table, then
 * DELETE the lock and run each of them again. The lock endpoint has no SQL surface, so both cases drive the generated
 * Tables client against the embedded server, which runs the same TablesController and TablesServiceImpl as
 * production. Both cases hold the lock through the shared lock boundary, which checks every lock and release
 * response and releases the lock once, whichever way the case ends.
 *
 * Preparation axes: each case builds its own parquet core table and seeds it directly, because the Tables API
 * addresses the table by its database and table name.
 *
 * Case families: two families contributing 2 cases.
 */
trait ScenarioLocking extends CompatibilityTableFixtures {

  /** The lock cases, each driven over HTTP against the embedded server. */
  lazy val lockingCases: List[TestCase] =
    List(
      TestCase("lock.enforcement @ embedded", lockEnforcement),
      TestCase("lock.starvesMaintenance @ embedded", lockStarvesMaintenance))

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * POSTing a table lock causes a following Spark UPDATE to be rejected server-side with the typed Iceberg
   * BadRequestException the LOCKED_TABLE_OPERATION 400 surfaces as, and while the lock holds the table keeps its exact
   * seed rows, holds no locked-write value, and points main at the same snapshot. DELETEing the lock lets a later
   * UPDATE commit one new snapshot, leaving the seed with row 1 carrying the unlocked value.
   */
  private def lockEnforcement(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = TableTest.nextQualifiedTableName(ctx.namespace)
    val Array(database, tableName) = table.stripPrefix("openhouse.").split("\\.", 2)

    withOwnedTable(spark.sql(_), table)(spark.sql(coreCreateStatement(table, "parquet"))) {
      spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(Core, standardSeedRowCount)}")
      val rowsBeforeLock = PreparedTable.currentRows(spark, table, Core)
      val snapshotCountBeforeLock = PreparedTable.snapshotCount(spark, table)
      val snapshotBeforeLock = activeSnapshotId(spark, table)

      withTableLock(lockRequest(ctx, database, tableName), unlockRequest(ctx, database, tableName)) {
        releaseLock =>
          val lockedFailure = Check.intercept[Exception](
            spark.sql(
              s"UPDATE $table SET ${Core.string0.columnName} = 'locked-write' " +
                s"WHERE ${Core.long0.columnName} = 1"))
          val lockedCauses = Exceptions.causeChain(lockedFailure)
          assert(
            lockedCauses.exists(cause =>
              cause.getClass.getName.contains("BadRequestException")),
            "the locked write surfaces the typed Iceberg BadRequest (400) rejection, found: " +
              s"${lockedCauses.map(_.getClass.getName)}")
          assert(
            lockedCauses.exists(cause =>
              Option(cause.getMessage).exists(_.toLowerCase.contains("locked"))),
            s"the rejection names the locked state, found: ${lockedFailure.getMessage.take(200)}")

          spark.sql(s"REFRESH TABLE $table")
          assert(
            PreparedTable.currentRows(spark, table, Core) == rowsBeforeLock,
            "a locked table keeps its exact rows")
          assert(
            queryCount(
              spark,
              s"SELECT count(*) FROM $table WHERE ${Core.string0.columnName} = 'locked-write'") == "0",
            "the rejected write leaves no locked-write value behind")
          assert(
            activeSnapshotId(spark, table) == snapshotBeforeLock,
            "a locked table points main at the same snapshot")
          assert(
            PreparedTable.snapshotCount(spark, table) == snapshotCountBeforeLock,
            "a locked table commits no new snapshot")

          releaseLock()
          spark.sql(
            s"UPDATE $table SET ${Core.string0.columnName} = 'unlocked-write' " +
              s"WHERE ${Core.long0.columnName} = 1")
          spark.sql(s"REFRESH TABLE $table")
          assert(
            PreparedTable.snapshotCount(spark, table) == snapshotCountBeforeLock + 1,
            "the unlocked write commits exactly one new snapshot")
          assert(
            activeSnapshotId(spark, table) != snapshotBeforeLock,
            "the unlocked write moves main to a new snapshot")
          val expectedRows = inKeyOrder(rowsBeforeLock.map { row =>
            if (Rows.TypedRow(row).get(Core.long0) == 1L) {
              withColumnValue(row, Core.string0, "unlocked-write")
            } else {
              row
            }
          })
          assert(
            PreparedTable.currentRows(spark, table, Core) == expectedRows,
            "the unlocked write leaves the seed with row 1 carrying the unlocked value")
      }
    }
  }

  /**
   * While a table is REST-locked, an expire_snapshots call is rejected and snapshots keep accumulating. After the lock
   * is deleted, expire_snapshots succeeds and the snapshot count drops, so the lock holds off every maintenance commit
   * for as long as it is held.
   */
  private def lockStarvesMaintenance(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = TableTest.nextQualifiedTableName(ctx.namespace)
    val Array(database, tableName) = table.stripPrefix("openhouse.").split("\\.", 2)
    val expireSnapshots =
      s"CALL openhouse.system.expire_snapshots(table => '${catalogRelativeTableName(table)}', " +
        "older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)"

    withOwnedTable(spark.sql(_), table)(spark.sql(coreCreateStatement(table, "parquet"))) {
      spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(Core, standardSeedRowCount)}")
      spark.sql(
        s"INSERT INTO $table VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      withTableLock(lockRequest(ctx, database, tableName), unlockRequest(ctx, database, tableName)) {
        releaseLock =>
          val snapshotsBefore = queryCount(spark, s"SELECT count(*) FROM $table.snapshots")

          val lockedFailure = Check.intercept[Exception](spark.sql(expireSnapshots))
          assert(
            Exceptions.causeChain(lockedFailure).exists(cause =>
              Option(cause.getMessage).exists(_.toLowerCase.contains("locked"))),
            "expected a LOCKED rejection for the maintenance commit: " +
              s"${lockedFailure.getClass.getName} " +
              Option(lockedFailure.getMessage).getOrElse("").take(180))
          spark.sql(s"REFRESH TABLE $table")
          assert(
            queryCount(spark, s"SELECT count(*) FROM $table.snapshots") == snapshotsBefore,
            "a locked table keeps every snapshot it holds")

          releaseLock()
          spark.sql(expireSnapshots)
          spark.sql(s"REFRESH TABLE $table")
          assert(
            queryCount(spark, s"SELECT count(*) FROM $table.snapshots").toLong < snapshotsBefore.toLong,
            "maintenance must proceed after unlock")
      }
    }
  }

  /**
   * Runs `use` while the case holds a table lock. The boundary validates acquisition and release, releases exactly
   * once, and preserves a body failure as the primary failure when release also fails.
   */
  private def withTableLock(
      lock: () => TableLockResponse,
      unlock: () => TableLockResponse)(use: (() => Unit) => Unit): Unit = {
    val lockResponse = lock()
    assert(
      lockResponse.statusCode >= 200 && lockResponse.statusCode < 300,
      s"lock request failed: ${lockResponse.statusCode} ${lockResponse.diagnosticText}")

    var lockHeld = true
    def releaseLock(): Unit = {
      val unlockResponse = unlock()
      lockHeld = false
      assert(
        unlockResponse.statusCode >= 200 && unlockResponse.statusCode < 300,
        s"unlock request failed: ${unlockResponse.statusCode} ${unlockResponse.diagnosticText}")
    }

    OwnedTableLifecycle.withCleanup(if (lockHeld) releaseLock())(use(() => releaseLock()))
  }

  /** The snapshot the table's main branch currently points at, read from the refs metadata table. */
  private def activeSnapshotId(spark: SparkSession, table: String): Long =
    spark.sql(s"SELECT snapshot_id FROM $table.refs WHERE name = 'main'").collect()(0).getLong(0)

  /** The generated Tables client call that takes the lock on the named table. */
  private def lockRequest(
      ctx: Ctx,
      database: String,
      tableName: String): () => TableLockResponse =
    () => ctx.tableLockClient.createLock(database, tableName)

  /** The generated Tables client call that releases the lock on the named table. */
  private def unlockRequest(
      ctx: Ctx,
      database: String,
      tableName: String): () => TableLockResponse =
    () => ctx.tableLockClient.deleteLock(database, tableName)

}
