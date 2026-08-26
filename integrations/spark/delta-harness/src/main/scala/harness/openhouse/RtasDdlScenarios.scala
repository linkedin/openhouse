package harness

import org.apache.iceberg.exceptions.BadRequestException

// The CTAS and RTAS DDL family. CREATE TABLE AS SELECT copies a seeded table into a new one, and
// CREATE OR REPLACE TABLE AS SELECT replaces a table's content in place. The replace path is gated
// on the replace.enabled table property and is rejected outright while a replication policy is set,
// so both the enabled and the rejected outcomes are pinned here.
trait RtasDdlScenarios extends RtasScenarioKit {

  lazy val ddlCtasRtasCases: List[Plan.Case] = preparedCoreFormats.flatMap { preparation =>
    List(
      preparation.test(
        "ddl.ctas",
        "CREATE TABLE AS SELECT from the seeded table produces a new table with the same 3 " +
          "rows.") { table =>
        val targetTable = s"${table.name}_ctas"

        table.spark.sql(s"DROP TABLE IF EXISTS $targetTable")
        table.spark.sql(
          s"CREATE TABLE $targetTable USING $dataSource AS SELECT * FROM ${table.name}")

        assert(
          table.spark.sql(s"SELECT count(*) FROM $targetTable").collect()(0).getLong(0) == 3,
          "CTAS lost rows")

        table.spark.sql(s"DROP TABLE IF EXISTS $targetTable")
      },
      preparation.test(
        "ddl.rtas.enabled",
        "With replace.enabled=true, CREATE OR REPLACE TABLE AS SELECT replaces the table's " +
          "content, leaving only the 2 rows selected by the replacement query.") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES ('replace.enabled'='true')")
        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            s"AS SELECT * FROM ${table.name} WHERE ${Core.long0.columnName} <= 2")

        assert(
          table.spark.sql(s"SELECT count(*) FROM ${table.name}").collect()(0).getLong(0) == 2,
          "RTAS did not replace")
      },
      preparation.test(
        "ddl.rtas.disabled",
        "Without replace.enabled set, CREATE OR REPLACE TABLE AS SELECT is rejected with a " +
          "BadRequestException stating RTAS is not enabled.") { table =>
        val exception = Check.intercept[BadRequestException](
          table.spark.sql(
            s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
              s"AS SELECT * FROM ${table.name}"))

        assert(
          exception.getMessage.contains("REPLACE TABLE AS SELECT is not enabled"),
          s"msg: ${exception.getMessage.take(160)}")
      },
      preparation.test(
        "ddl.rtas.replicationConflict",
        "With replace.enabled=true but a replication policy also set, CREATE OR REPLACE TABLE " +
          "AS SELECT is rejected with a BadRequestException about replication being enabled.") {
        table =>
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
}
