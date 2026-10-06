package com.linkedin.openhouse.spark.statementtest;

import com.google.common.collect.Lists;
import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import java.nio.file.Files;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SetSharingPolicyStatementTest {

  private static SparkSession spark = null;

  @Test
  public void testPolicySuccess() {
    assert isPlanValid(
        "ALTER TABLE openhouse.db.table SET POLICY (SHARING=TRUE)", "db.table", "TRUE");
    assert isPlanValid(
        "ALTER TABLE openhouse.db.table SET POLICY (SHARING=FALSE)", "db.table", "FALSE");
  }

  @Test
  public void testPolicyLowerCase() {
    assert isPlanValid(
        "ALTER TABLE openhouse.db.table SET policy (SHARING=TRUE)", "db.table", "TRUE");
  }

  @Test
  public void testPolicyAfterUseCatalog() {
    spark.sql("use openhouse").show();
    assert isPlanValid("ALTER TABLE db.table SET policy (SHARING=TRUE)", "db.table", "TRUE");
  }

  @Test
  public void testPolicyAfterUseCatalogAndDatabase() {
    spark.sql("use openhouse.db").show();
    assert isPlanValid("ALTER TABLE table SET policy (SHARING=TRUE)", "db.table", "TRUE");
  }

  @Test
  public void testPolicyWithQuotedTableIdentifier() {
    assert isPlanValid(
        "ALTER TABLE openhouse.`db`.`table` SET policy (SHARING=TRUE)", "db.table", "TRUE");
  }

  @Test
  public void testPolicyIdentifierWithLeadingDigits() {
    assert isPlanValid("ALTER TABLE openhouse.0_.0_ SET policy (SHARING=TRUE)", "0_.0_", "TRUE");
  }

  @Test
  public void testSharingWithMultiLineComments() {
    List<String> statementsWithComments =
        Lists.newArrayList(
            "/* bracketed comment */  ALTER TABLE openhouse.`db`.`table` SET policy (SHARING=TRUE)",
            "/**/  ALTER TABLE openhouse.`db`.`table` SET policy (SHARING=TRUE)",
            "-- single line comment \n ALTER TABLE openhouse.`db`.`table` SET policy (SHARING=TRUE)",
            "-- multiple \n-- single line \n-- comments \n ALTER TABLE openhouse.`db`.`table` SET policy (SHARING=TRUE)",
            "/* select * from multiline_comment \n where x like '%sql%'; */ ALTER TABLE openhouse.`db`.`table` SET policy (SHARING=TRUE)",
            "/* {\"app\": \"dbt\", \"dbt_version\": \"1.0.1\", \"profile_name\": \"profile1\", \"target_name\": \"dev\", "
                + "\"node_id\": \"model.profile1.stg_users\"} \n*/ ALTER TABLE openhouse.`db`.`table` SET policy (SHARING=TRUE)",
            "/* Some multi-line comment \n"
                + "*/ ALTER /* inline comment */ TABLE openhouse.`db`.`table` SET policy (SHARING=TRUE) -- ending comment",
            "ALTER -- a line ending comment\n"
                + "TABLE openhouse.`db`.`table` SET policy (SHARING=TRUE)");
    for (String statement : statementsWithComments) {
      assert isPlanValid(statement, "db.table", "TRUE");
    }
  }

  @Test
  public void testPolicyWithoutProperSyntax() {
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.db.table SET policy (SHARING=TRU)").show());
  }

  @SneakyThrows
  @BeforeAll
  public void setupSpark() {
    Path unittest = new Path(Files.createTempDirectory("unittest").toString());
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config(
                "spark.sql.extensions",
                ("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    + "com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions"))
            .config("spark.sql.catalog.openhouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.openhouse.type", "hadoop")
            .config("spark.sql.catalog.openhouse.warehouse", unittest.toString())
            .getOrCreate();
  }

  @BeforeEach
  public void setup() {
    spark
        .sql(
            "CREATE TABLE openhouse.db.table (id bigint, data string, `openhouse.tableId` string) USING iceberg")
        .show();
    spark
        .sql(
            "CREATE TABLE openhouse.0_.0_ (id bigint, 0_ string, `openhouse.tableId` string) USING iceberg")
        .show();
    spark
        .sql("ALTER TABLE openhouse.db.table SET TBLPROPERTIES ('openhouse.tableId' = 'tableid')")
        .show();
    spark
        .sql("ALTER TABLE openhouse.0_.0_ SET TBLPROPERTIES ('openhouse.tableId' = 'tableid')")
        .show();
  }

  @AfterEach
  public void tearDown() {
    spark.sql("DROP TABLE openhouse.db.table").show();
    spark.sql("DROP TABLE openhouse.0_.0_").show();
  }

  @AfterAll
  public void tearDownSpark() {
    spark.close();
  }

  @SneakyThrows
  private boolean isPlanValid(String statement, String dbTable, String sharingEnabled) {
    String queryStr = StatementTestUtils.planWithoutExecuting(spark, statement);
    return queryStr.contains(sharingEnabled) && queryStr.contains(dbTable);
  }
}
