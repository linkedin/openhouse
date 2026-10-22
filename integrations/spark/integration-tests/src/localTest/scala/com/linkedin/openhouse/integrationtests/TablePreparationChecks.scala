package com.linkedin.openhouse.integrationtests

/** Exercises preparation ownership and teardown against the embedded OpenHouse catalog. */
object TablePreparationChecks {
  def main(arguments: Array[String]): Unit = {
    val (server, spark) = OpenHouseEnv.start()
    OwnedTableLifecycle.withCleanup(
      OwnedTableLifecycle.withCleanup(server.stop())(spark.stop())) {
      spark.sparkContext.setLogLevel("ERROR")
      List("success", "create-validation", "preparation", "body").foreach { phase =>
        var createdTable = Option.empty[String]
        val expectedFailure = new AssertionError(s"injected $phase failure")
        val preparation = TableTest(CoreTable)
          .sql("create") { table =>
            createdTable = Some(table)
            s"CREATE TABLE $table (" +
              CoreTable.tableColumns.map(column => s"${column.columnName} ${column.sqlType}").mkString(", ") +
              ") USING iceberg"
          } { _ =>
            if (phase == "create-validation") throw expectedFailure
          }
          .step("prepare") { (_, _) =>
            if (phase == "preparation") throw expectedFailure
          }()

        val runPreparation = () =>
          preparation.prepare(TestContext(spark, "openhouse.dbMatrix")) { table =>
            assert(spark.catalog.tableExists(table.name))
            if (phase == "body") throw expectedFailure
          }

        if (phase == "success") {
          runPreparation()
        } else {
          assert(Check.intercept[AssertionError](runPreparation()) eq expectedFailure)
        }
        assert(createdTable.isDefined, s"$phase must execute CREATE")
        assert(
          !spark.catalog.tableExists(createdTable.get),
          s"$phase must drop its created table")
        println(s"PASS preparation teardown: $phase")
      }
    }
  }
}
