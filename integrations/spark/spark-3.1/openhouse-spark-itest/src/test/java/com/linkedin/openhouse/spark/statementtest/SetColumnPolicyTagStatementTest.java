package com.linkedin.openhouse.spark.statementtest;

import com.google.common.collect.Lists;
import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
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
public class SetColumnPolicyTagStatementTest {

  private static SparkSession spark = null;

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
            "CREATE TABLE openhouse.db.table (id bigint, col1 string, col2 string, `openhouse.tableId` string) USING iceberg")
        .show();
    spark
        .sql(
            "CREATE TABLE openhouse.0_.0_ (id bigint, 0_ string, col2 string, `openhouse.tableId` string) USING iceberg")
        .show();
    spark
        .sql("ALTER TABLE openhouse.db.table SET TBLPROPERTIES ('openhouse.tableId' = 'tableid')")
        .show();
    spark
        .sql("ALTER TABLE openhouse.0_.0_ SET TBLPROPERTIES ('openhouse.tableId' = 'tableid')")
        .show();
  }

  private static final List<String> tagPII = Collections.singletonList("PII");
  private static final List<String> tagHC = Collections.singletonList("HC");
  private static final List<String> tagALL = Arrays.asList("PII", "HC");

  @Test
  public void testPolicySuccess() {
    assert isPlanValid(
        "ALTER TABLE openhouse.db.table MODIFY COLUMN col1 SET TAG = (PII)",
        "openhouse",
        "db.table",
        "col1",
        tagPII);
    assert isPlanValid(
        "ALTER TABLE openhouse.db.table MODIFY COLUMN col1 SET TAG = (PII, HC)",
        "openhouse",
        "db.table",
        "col1",
        tagALL);
    assert isPlanValid(
        "ALTER TABLE openhouse.db.table MODIFY COLUMN col1 SET TAG = (NONE)",
        "openhouse",
        "db.table",
        "col1",
        Collections.emptyList());
    assert isPlanValid(
        "ALTER TABLE openhouse.db.table MODIFY COLUMN col1 SET TAG = (HC)",
        "openhouse",
        "db.table",
        "col1",
        tagHC);
  }

  @Test
  public void testPolicyLowerCase() {
    assert isPlanValid(
        "ALTER TABLE openhouse.db.table modify column col1 SET TAG = (PII)",
        "openhouse",
        "db.table",
        "col1",
        tagPII);
    assert isPlanValid(
        "ALTER TABLE openhouse.db.table MODIFY COLUMN col1 set tag = (PII, HC)",
        "openhouse",
        "db.table",
        "col1",
        tagALL);
    assert isPlanValid(
        "alter table openhouse.db.table modify column col1 set tag = (NONE)",
        "openhouse",
        "db.table",
        "col1",
        Collections.emptyList());
  }

  @Test
  public void testPolicyAfterUseCatalog() {
    spark.sql("use openhouse").show();
    assert isPlanValid(
        "ALTER TABLE openhouse.db.table MODIFY COLUMN col1 SET TAG = (PII)",
        "openhouse",
        "db.table",
        "col1",
        tagPII);

    spark.sql("use openhouse").show();
    assert isPlanValid(
        "ALTER TABLE openhouse.db.table MODIFY COLUMN col1 SET TAG = (PII, HC)",
        "openhouse",
        "db.table",
        "col1",
        tagALL);
  }

  @Test
  public void testPolicyAfterUseCatalogAndDatabase() {
    spark.sql("use openhouse.db").show();
    assert isPlanValid(
        "ALTER TABLE openhouse.db.table MODIFY COLUMN col1 SET TAG = (PII)",
        "openhouse",
        "db.table",
        "col1",
        tagPII);

    spark.sql("use openhouse.db").show();
    assert isPlanValid(
        "ALTER TABLE openhouse.db.table MODIFY COLUMN col1 SET TAG = (PII, HC)",
        "openhouse",
        "db.table",
        "col1",
        tagALL);
  }

  @Test
  public void testPolicyWithQuotedTableIdentifier() {
    assert isPlanValid(
        "ALTER TABLE openhouse.`db`.`table` MODIFY COLUMN col1 SET TAG = (PII)",
        "openhouse",
        "db.table",
        "col1",
        tagPII);
    assert isPlanValid(
        "ALTER TABLE openhouse.`db`.`table` MODIFY COLUMN col1 SET TAG = (PII, HC)",
        "openhouse",
        "db.table",
        "col1",
        tagALL);
    assert isPlanValid(
        "alter table openhouse.`db`.`table` modify column col1 set tag = (NONE)",
        "openhouse",
        "db.table",
        "col1",
        Collections.emptyList());
  }

  @Test
  public void testPolicyIdentifierWithLeadingDigits() {
    assert isPlanValid(
        "ALTER TABLE openhouse.0_.0_ MODIFY COLUMN 0_ SET TAG = (PII)",
        "openhouse",
        "0_.0_",
        "0_",
        tagPII);
  }

  @Test
  public void testSharingWithMultiLineComments() {
    List<String> statementsWithComments =
        Lists.newArrayList(
            "/* bracketed comment */  ALTER TABLE openhouse.`db`.`table` MODIFY COLUMN col1 SET TAG = (PII, HC)",
            "/**/  ALTER TABLE openhouse.`db`.`table` MODIFY COLUMN col1 SET TAG = (PII, HC)",
            "-- single line comment \n ALTER TABLE openhouse.`db`.`table` MODIFY COLUMN col1 SET TAG = (PII, HC)",
            "-- multiple \n-- single line \n-- comments \n ALTER TABLE openhouse.`db`.`table` MODIFY COLUMN col1 SET TAG = (PII, HC)",
            "/* select * from multiline_comment \n where x like '%sql%'; */ ALTER TABLE openhouse.`db`.`table` MODIFY COLUMN col1 SET TAG = (PII, HC)",
            "/* {\"app\": \"dbt\", \"dbt_version\": \"1.0.1\", \"profile_name\": \"profile1\", \"target_name\": \"dev\", "
                + "\"node_id\": \"model.profile1.stg_users\"} \n*/ ALTER TABLE openhouse.`db`.`table` MODIFY COLUMN col1 SET TAG = (PII, HC)",
            "/* Some multi-line comment \n"
                + "*/ ALTER /* inline comment */ TABLE openhouse.`db`.`table` MODIFY COLUMN col1 SET TAG = (PII, HC) -- ending comment",
            "ALTER -- a line ending comment\n"
                + "TABLE openhouse.`db`.`table` MODIFY COLUMN col1 SET TAG = (PII, HC)");
    for (String statement : statementsWithComments) {
      assert isPlanValid(statement, "openhouse", "db.table", "col1", tagALL);
    }
  }

  @Test
  public void testPolicyWithoutProperSyntax() {
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql("ALTER TABLE openhouse.db.table MODIFY COLUMN col1 SET POLICY = (PII)")
                .show());

    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.db.table MODIFY COLUMN col1 SET TAG = PII").show());

    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql("ALTER TABLE openhouse.db.table MODIFY COLUMN (col1) SET TAG = (PII)")
                .show());

    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql("ALTER TABLE openhouse.db.table MODIFY COLUMN col1 SET TAG = PII, HC")
                .show());

    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql("ALTER TABLE openhouse.db.table MODIFY COLUMN col1 SET TAG = (PII, NONE)")
                .show());

    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.db.table MODIFY COLUMN col1 SET TAG = (PI)").show());
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
  private boolean isPlanValid(
      String statement,
      String catalogName,
      String dbTable,
      String colName,
      List<String> policyTags) {
    String queryStr = StatementTestUtils.planWithoutExecuting(spark, statement);
    boolean containsCol = true;
    for (String tag : policyTags) {
      if (!queryStr.contains(tag)) {
        containsCol = false;
        break;
      }
    }
    return queryStr.contains(colName) && queryStr.contains(dbTable) && containsCol;
  }
}
