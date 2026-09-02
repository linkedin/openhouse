package harness

/**
 * Table locking: while a table carries a REST lock, the catalog rejects the commits that would change it, and
 * deleting the lock lets them through again.
 *
 * Operations: POST the lock endpoint, then run an UPDATE and an expire_snapshots call against the locked table, then
 * DELETE the lock and run each of them again. The lock endpoint has no SQL surface, so both cases drive it over HTTP
 * against the embedded server, which runs the same TablesController and TablesServiceImpl as production. Both cases
 * hold the lock through the shared lock boundary, which checks every lock and release response and releases the lock
 * once, whichever way the case ends.
 *
 * Preparation axes: each case builds its own parquet core table and seeds it directly, because the REST path
 * addresses the table by its database and table name.
 *
 * Case families: two families contributing 2 cases.
 */
trait ScenarioLocking extends ScenarioKit {

  /** The lock cases, each driven over HTTP against the embedded server. */
  lazy val lockingCases: List[Plan.Case] =
    List(
      Plan.Case("lock.enforcement @ embedded", lockEnforcement),
      Plan.Case("lock.starvesMaintenance @ embedded", lockStarvesMaintenance))

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * POSTing a table lock causes a following Spark UPDATE to be rejected server-side with LOCKED_TABLE_OPERATION, and
   * DELETEing the lock lets a later UPDATE apply.
   */
  private def lockEnforcement(ctx: Ctx): Unit = {
    val spark = ctx.spark
    val table = TableTest.nextQualifiedTableName(ctx.namespace)
    val Array(database, tableName) = table.stripPrefix("openhouse.").split("\\.", 2)

    withOwnedTable(spark.sql(_), table)(spark.sql(coreCreate(table, "parquet"))) {
      spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(Core, standardSeedRowCount)}")
      withTableLock(lockRequest(ctx, database, tableName), unlockRequest(ctx, database, tableName)) {
        releaseLock =>
          val lockedFailure = Check.intercept[Exception](
            spark.sql(
              s"UPDATE $table SET ${Core.string0.columnName} = 'locked-write' " +
                s"WHERE ${Core.long0.columnName} = 1"))
          assert(
            Exceptions.causeChain(lockedFailure).exists(cause =>
              Option(cause.getMessage).exists(_.toLowerCase.contains("locked"))),
            s"expected a locked-table rejection, got: ${lockedFailure.getMessage.take(200)}")

          releaseLock()
          spark.sql(
            s"UPDATE $table SET ${Core.string0.columnName} = 'unlocked-write' " +
              s"WHERE ${Core.long0.columnName} = 1")
          assert(
            countOf(
              spark,
              s"SELECT count(*) FROM $table WHERE ${Core.string0.columnName} = 'unlocked-write'") == "1",
            "post-unlock update did not apply")
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
      s"CALL openhouse.system.expire_snapshots(table => '${catalogRelative(table)}', " +
        "older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)"

    withOwnedTable(spark.sql(_), table)(spark.sql(coreCreate(table, "parquet"))) {
      spark.sql(s"INSERT INTO $table ${RowGenerator.valuesClause(Core, standardSeedRowCount)}")
      spark.sql(
        s"INSERT INTO $table VALUES (CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      withTableLock(lockRequest(ctx, database, tableName), unlockRequest(ctx, database, tableName)) {
        releaseLock =>
          val snapshotsBefore = countOf(spark, s"SELECT count(*) FROM $table.snapshots")

          val lockedFailure = Check.intercept[Exception](spark.sql(expireSnapshots))
          assert(
            Exceptions.causeChain(lockedFailure).exists(cause =>
              Option(cause.getMessage).exists(_.toLowerCase.contains("locked"))),
            "expected a LOCKED rejection for the maintenance commit: " +
              s"${lockedFailure.getClass.getName} " +
              Option(lockedFailure.getMessage).getOrElse("").take(180))
          spark.sql(s"REFRESH TABLE $table")
          assert(
            countOf(spark, s"SELECT count(*) FROM $table.snapshots") == snapshotsBefore,
            "a locked table keeps every snapshot it holds")

          releaseLock()
          spark.sql(expireSnapshots)
          spark.sql(s"REFRESH TABLE $table")
          assert(
            countOf(spark, s"SELECT count(*) FROM $table.snapshots").toLong < snapshotsBefore.toLong,
            "maintenance must proceed after unlock")
      }
    }
  }

  /** The POST that takes the REST lock on the named table. */
  private def lockRequest(ctx: Ctx, database: String, tableName: String): () => (Int, String) =
    () => Rest.post(ctx, s"/v1/databases/$database/tables/$tableName/lock", """{"locked":true}""")

  /** The DELETE that releases the REST lock on the named table. */
  private def unlockRequest(ctx: Ctx, database: String, tableName: String): () => (Int, String) =
    () => Rest.delete(ctx, s"/v1/databases/$database/tables/$tableName/lock")

}
