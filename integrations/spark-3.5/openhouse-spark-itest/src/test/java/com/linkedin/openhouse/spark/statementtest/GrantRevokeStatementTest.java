package com.linkedin.openhouse.spark.statementtest;

import static org.assertj.core.api.Assertions.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.linkedin.openhouse.javaclient.api.SupportsGrantRevoke;
import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GrantRevokeStatementTest {

  private static SparkSession spark = null;

  @Test
  public void testSimpleGrantTable() {
    for (String privilege : ImmutableList.of("SELECT", "ALTER", "DESCRIBE", "MANAGE GRANTS")) {
      spark.sql(String.format("GRANT %s ON TABLE openhouse.db.table TO sraikar", privilege));
      assertPlanValid(true, "TABLE", "db.table", privilege, "sraikar");
    }
  }

  @Test
  public void testSimpleRevokeTable() {
    for (String privilege : ImmutableList.of("SELECT", "ALTER", "DESCRIBE", "MANAGE GRANTS")) {
      spark.sql(String.format("REVOKE %s ON TABLE openhouse.db.table FROM sraikar", privilege));
      assertPlanValid(false, "TABLE", "db.table", privilege, "sraikar");
    }
  }

  @Test
  public void testSimpleGrantLowerCase() {
    spark.sql("grant select on table openhouse.db.table to sraikar");
    assertPlanValid(true, "TABLE", "db.table", "SELECT", "sraikar");
  }

  @Test
  public void testSimpleGrantIdentifierWithLeadingDigits() {
    spark.sql("grant select on table openhouse.0_.0_ to sraikar");
    assertPlanValid(true, "TABLE", "0_.0_", "SELECT", "sraikar");
  }

  @Test
  public void testSimpleRevokeLowerCase() {
    spark.sql("revoke select on table openhouse.db.table from sraikar");
    assertPlanValid(false, "TABLE", "db.table", "SELECT", "sraikar");
  }

  @Test
  public void testSimpleRevokeIdentifierWithLeadingDigits() {
    spark.sql("revoke select on table openhouse.0_.0_ from sraikar");
    assertPlanValid(false, "TABLE", "0_.0_", "SELECT", "sraikar");
  }

  @Test
  public void testSimpleGrantAfterUseCatalog() {
    spark.sql("use openhouse").show();
    spark.sql("GRANT SELECT ON TABLE db.table TO sraikar");
    assertPlanValid(true, "TABLE", "db.table", "SELECT", "sraikar");
  }

  @Test
  public void testSimpleRevokeAfterUseCatalog() {
    spark.sql("use openhouse").show();
    spark.sql("REVOKE SELECT ON TABLE db.table FROM sraikar");
    assertPlanValid(false, "TABLE", "db.table", "SELECT", "sraikar");
  }

  @Test
  public void testSimpleGrantAfterUseCatalogAndDatabase() {
    spark.sql("use openhouse.db").show();
    spark.sql("GRANT SELECT ON TABLE table TO sraikar");
    assertPlanValid(true, "TABLE", "db.table", "SELECT", "sraikar");
  }

  @Test
  public void testSimpleRevokeAfterUseCatalogAndDatabase() {
    spark.sql("use openhouse.db").show();
    spark.sql("REVOKE SELECT ON TABLE table FROM sraikar");
    assertPlanValid(false, "TABLE", "db.table", "SELECT", "sraikar");
  }

  @Test
  public void testGrantWithQuotedTableIdentifier() {
    spark.sql("GRANT SELECT ON TABLE openhouse.`db`.`table` TO sraikar");
    assertPlanValid(true, "TABLE", "db.table", "SELECT", "sraikar");
  }

  @Test
  public void testRevokeWithQuotedTableIdentifier() {
    spark.sql("REVOKE SELECT ON TABLE openhouse.`db`.`table` FROM sraikar");
    assertPlanValid(false, "TABLE", "db.table", "SELECT", "sraikar");
  }

  @Test
  public void testGrantWithQuotedPrincipal() {
    spark.sql("GRANT SELECT ON TABLE openhouse.db.table TO `sraikar`");
    assertPlanValid(true, "TABLE", "db.table", "SELECT", "sraikar");
  }

  @Test
  public void testRevokeWithQuotedPrincipal() {
    spark.sql("REVOKE SELECT ON TABLE openhouse.db.table FROM `sraikar`");
    assertPlanValid(false, "TABLE", "db.table", "SELECT", "sraikar");
  }

  @Test
  public void testSimpleGrantDatabase() {
    for (String privilege : ImmutableList.of("CREATE TABLE")) {
      spark.sql(String.format("GRANT %s ON DATABASE openhouse.db TO sraikar", privilege));
      assertPlanValid(true, "DATABASE", "db", privilege, "sraikar");
    }
  }

  @Test
  public void testSimpleRevokeDatabase() {
    for (String privilege : ImmutableList.of("CREATE TABLE")) {
      spark.sql(String.format("REVOKE %s ON DATABASE openhouse.db FROM sraikar", privilege));
      assertPlanValid(false, "DATABASE", "db", privilege, "sraikar");
    }
  }

  @Test
  public void testSimpleGrantLowerCaseDatabase() {
    spark.sql("GRANT create table on database openhouse.db to sraikar");
    assertPlanValid(true, "DATABASE", "db", "CREATE TABLE", "sraikar");
  }

  @Test
  public void testSimpleRevokeLowerCaseDatabase() {
    spark.sql("revoke create table on database openhouse.db from sraikar");
    assertPlanValid(false, "DATABASE", "db", "CREATE TABLE", "sraikar");
  }

  @Test
  public void testMultiNamespace() {
    spark.sql("GRANT CREATE TABLE ON DATABASE openhouse.db1.db2 to sraikar");
    assertPlanValid(true, "DATABASE", "db1.db2", "CREATE TABLE", "sraikar");
  }

  @Test
  public void testGrantWithMultiLineComments() {
    List<String> statementsWithComments =
        Lists.newArrayList(
            "/* bracketed comment */  GRANT SELECT ON TABLE openhouse.db.table TO sraikar",
            "/**/  GRANT SELECT ON TABLE openhouse.db.table TO sraikar",
            "-- single line comment \n GRANT SELECT ON TABLE openhouse.db.table TO sraikar",
            "-- multiple \n-- single line \n-- comments \n GRANT SELECT ON TABLE openhouse.db.table TO sraikar",
            "/* select * from multiline_comment \n where x like '%sql%'; */ GRANT SELECT ON TABLE openhouse.db.table TO sraikar",
            "/* {\"app\": \"dbt\", \"dbt_version\": \"1.0.1\", \"profile_name\": \"profile1\", \"target_name\": \"dev\", "
                + "\"node_id\": \"model.profile1.stg_users\"} \n*/ GRANT SELECT ON TABLE openhouse.db.table TO sraikar",
            "/* Some multi-line comment \n"
                + "*/ GRANT /* inline comment */ SELECT ON TABLE openhouse.db.table TO sraikar -- ending comment",
            "GRANT -- a line ending comment\n" + "SELECT ON TABLE openhouse.db.table TO sraikar");
    for (String statement : statementsWithComments) {
      spark.sql(statement);
      assertPlanValid(true, "TABLE", "db.table", "SELECT", "sraikar");
    }
  }

  @Test
  public void testGrantWithoutProperSyntax() {
    Assertions.assertThrows(
        ParseException.class,
        () -> spark.sql("GRAN SELECT ON TABLE openhouse.db.table TO sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("GRANT SELECT openhouse.db.table sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("GRANT ON SELECT openhouse.db.table sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("GRANT SELECT ON TABLE openhouse.db.table sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("GRANT SELECT ON openhouse.db.table TO sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("GRANT `SELECT` ON TABLE openhouse.db.table TO sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("GRANT GRANT//REVOKE ON TABLE openhouse.db.table TO sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("GRANT GRANT_REVOKE ON TABLE openhouse.db.table TO sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("GRANT CREATETABLE ON TABLE openhouse.db.table TO sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("GRANT CREATE TABLE ON DATABAS openhouse.db TO sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("GRANT CREATE TABLE DATABASE openhouse.db TO sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("GRANT CREATE TABLE DATABASE openhouse.db sraikar").show());
    Assertions.assertThrows(OpenhouseParseException.class, () -> spark.sql("GRANT").show());
  }

  @Test
  public void testRevokeWithoutProperSyntax() {
    Assertions.assertThrows(
        ParseException.class,
        () -> spark.sql("REVOK SELECT ON TABLE openhouse.db.table from sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("REVOKE SELECT openhouse.db.table sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("REVOKE ON SELECT openhouse.db.table sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("REVOKE SELECT ON TABLE openhouse.db.table sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("REVOKE SELECT ON openhouse.db.table from sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("REVOKE `SELECT` ON TABLE openhouse.db.table FROM sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("REVOKE GRANT//REVOKE ON TABLE openhouse.db.table FROM sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("REVOKE GRANT_REVOKE ON TABLE openhouse.db.table FROM sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("REVOKE CREATE TABLE ON DATABAS openhouse.db FROM sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("REVOKE CREATE TABLE DATABASE openhouse.db FROM sraikar").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("REVOKE CREATE TABLE DATABASE openhouse.db sraikar").show());
    Assertions.assertThrows(OpenhouseParseException.class, () -> spark.sql("REVOKE").show());
  }

  @Test
  public void testNotSupportGrantRevokeCatalog() {
    spark.sql("CREATE DATABASE spark_catalog.db").show();
    spark.sql("CREATE TABLE spark_catalog.db.tabl (id bigint, data string) USING iceberg").show();
    Exception exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> spark.sql("GRANT SELECT ON TABLE spark_catalog.db.tabl to sraikar").show());
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Catalog 'spark_catalog' does not support Grant Revoke Statements"));
  }

  @Test
  public void testSingleShowGrantsForTable() {
    GrantRevokeHadoopCatalog.granteeList =
        ImmutableList.of(new SupportsGrantRevoke.AclPolicyDto("SELECT", "sraikar"));
    List<SupportsGrantRevoke.AclPolicyDto> granteeList =
        toAclPolicies(spark.sql("SHOW GRANTS ON TABLE openhouse.db.table"));
    org.assertj.core.api.Assertions.assertThat(granteeList)
        .containsExactlyInAnyOrderElementsOf(GrantRevokeHadoopCatalog.granteeList);
  }

  @Test
  public void testMultipleShowGrantsForTable() {
    GrantRevokeHadoopCatalog.granteeList =
        ImmutableList.of(
            new SupportsGrantRevoke.AclPolicyDto("ALTER", "sraikar"),
            new SupportsGrantRevoke.AclPolicyDto("SELECT", "lesun"),
            new SupportsGrantRevoke.AclPolicyDto("MANAGE GRANTS", "sraikar"),
            new SupportsGrantRevoke.AclPolicyDto("DESCRIBE", "lejiang"));
    List<SupportsGrantRevoke.AclPolicyDto> granteeList =
        toAclPolicies(spark.sql("SHOW GRANTS ON TABLE openhouse.db.table"));
    org.assertj.core.api.Assertions.assertThat(granteeList)
        .containsExactlyInAnyOrderElementsOf(GrantRevokeHadoopCatalog.granteeList);
  }

  @Test
  public void testMultipleShowGrantsUseOpenhouseForTable() {
    spark.sql("use openhouse");
    GrantRevokeHadoopCatalog.granteeList =
        ImmutableList.of(
            new SupportsGrantRevoke.AclPolicyDto("ALTER", "sraikar"),
            new SupportsGrantRevoke.AclPolicyDto("SELECT", "lesun"),
            new SupportsGrantRevoke.AclPolicyDto("MANAGE GRANTS", "sraikar"),
            new SupportsGrantRevoke.AclPolicyDto("DESCRIBE", "lejiang"));
    List<SupportsGrantRevoke.AclPolicyDto> granteeList =
        toAclPolicies(spark.sql("SHOW GRANTS ON TABLE db.table"));
    org.assertj.core.api.Assertions.assertThat(granteeList)
        .containsExactlyInAnyOrderElementsOf(GrantRevokeHadoopCatalog.granteeList);
  }

  @Test
  public void testNoShowGrantsForTable() {
    GrantRevokeHadoopCatalog.granteeList = ImmutableList.of();
    List<SupportsGrantRevoke.AclPolicyDto> granteeList =
        toAclPolicies(spark.sql("SHOW GRANTS ON TABLE openhouse.db.table"));
    org.assertj.core.api.Assertions.assertThat(granteeList)
        .containsExactlyInAnyOrderElementsOf(GrantRevokeHadoopCatalog.granteeList);
  }

  @Test
  public void testSingleShowGrantsForDatabase() {
    GrantRevokeHadoopCatalog.granteeList =
        ImmutableList.of(new SupportsGrantRevoke.AclPolicyDto("CREATE TABLE", "sraikar"));
    List<SupportsGrantRevoke.AclPolicyDto> granteeList =
        toAclPolicies(spark.sql("SHOW GRANTS ON DATABASE openhouse.db"));
    org.assertj.core.api.Assertions.assertThat(granteeList)
        .containsExactlyInAnyOrderElementsOf(GrantRevokeHadoopCatalog.granteeList);
  }

  @Test
  public void testMultipleShowGrantsForDatabase() {
    GrantRevokeHadoopCatalog.granteeList =
        ImmutableList.of(
            new SupportsGrantRevoke.AclPolicyDto("CREATE TABLE", "sraikar"),
            new SupportsGrantRevoke.AclPolicyDto("CREATE TABLE", "lesun"));
    List<SupportsGrantRevoke.AclPolicyDto> granteeList =
        toAclPolicies(spark.sql("SHOW GRANTS ON DATABASE openhouse.db"));
    org.assertj.core.api.Assertions.assertThat(granteeList)
        .containsExactlyInAnyOrderElementsOf(GrantRevokeHadoopCatalog.granteeList);
  }

  @Test
  public void testMultipleShowGrantsUseOpenhouseForDatabase() {
    spark.sql("use openhouse");
    GrantRevokeHadoopCatalog.granteeList =
        ImmutableList.of(
            new SupportsGrantRevoke.AclPolicyDto("CREATE TABLE", "sraikar"),
            new SupportsGrantRevoke.AclPolicyDto("CREATE TABLE", "lesun"));
    List<SupportsGrantRevoke.AclPolicyDto> granteeList =
        toAclPolicies(spark.sql("SHOW GRANTS ON DATABASE db"));
    org.assertj.core.api.Assertions.assertThat(granteeList)
        .containsExactlyInAnyOrderElementsOf(GrantRevokeHadoopCatalog.granteeList);
  }

  @Test
  public void testNoShowGrantsForDatabase() {
    GrantRevokeHadoopCatalog.granteeList = ImmutableList.of();
    List<SupportsGrantRevoke.AclPolicyDto> granteeList =
        toAclPolicies(spark.sql("SHOW GRANTS ON DATABASE openhouse.db"));
    org.assertj.core.api.Assertions.assertThat(granteeList)
        .containsExactlyInAnyOrderElementsOf(GrantRevokeHadoopCatalog.granteeList);
  }

  @Test
  public void testShowGrantsSyntaxError() {
    Assertions.assertThrows(
        ParseException.class, () -> spark.sql("SHOW GRANT TABLE openhouse.db.tbl").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("SHOW GRANTS TABLE openhouse.db.tbl").show());
    Assertions.assertThrows(
        OpenhouseParseException.class, () -> spark.sql("SHOW GRANTS DATABASE openhouse.db").show());
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("SHOW GRANTS FOR DATABASE openhouse.db").show());
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
            .config(
                "spark.sql.catalog.openhouse.catalog-impl",
                "com.linkedin.openhouse.spark.statementtest.GrantRevokeStatementTest$GrantRevokeHadoopCatalog")
            .config("spark.sql.catalog.openhouse.warehouse", unittest.toString())
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", unittest.toString())
            .getOrCreate();
  }

  @BeforeEach
  public void setup() {
    GrantRevokeHadoopCatalog.resourceType = null;
    GrantRevokeHadoopCatalog.resourceName = null;
    GrantRevokeHadoopCatalog.isGrant = null;
    GrantRevokeHadoopCatalog.privilege = null;
    GrantRevokeHadoopCatalog.principal = null;
    GrantRevokeHadoopCatalog.granteeList = null;
    spark.sql("CREATE TABLE openhouse.db.table (id bigint, data string) USING iceberg").show();
  }

  @AfterEach
  public void tearDown() {
    spark.sql("DROP TABLE openhouse.db.table").show();
  }

  @AfterAll
  public void tearDownSpark() {
    spark.close();
  }

  private void assertPlanValid(
      Boolean isGrant,
      String resourceType,
      String resourceName,
      String privilege,
      String principal) {
    Assertions.assertEquals(isGrant, GrantRevokeHadoopCatalog.isGrant, "Grant type mismatch");
    Assertions.assertEquals(privilege, GrantRevokeHadoopCatalog.privilege, "Privilege mismatch");
    Assertions.assertEquals(principal, GrantRevokeHadoopCatalog.principal, "Principal mismatch");
    Assertions.assertEquals(
        resourceType, GrantRevokeHadoopCatalog.resourceType, "ResourceType mismatch");
    Assertions.assertEquals(
        resourceName, GrantRevokeHadoopCatalog.resourceName, "ResourceName mismatch");
  }

  private List<SupportsGrantRevoke.AclPolicyDto> toAclPolicies(Dataset<Row> ac) {
    return ac.collectAsList().stream()
        .map(x -> new SupportsGrantRevoke.AclPolicyDto(x.getString(0), x.getString(1)))
        .collect(Collectors.toList());
  }

  public static class GrantRevokeHadoopCatalog extends HadoopCatalog
      implements SupportsGrantRevoke {
    public static String resourceType;
    public static String resourceName;
    public static Boolean isGrant;
    public static String privilege;
    public static String principal;

    public static List<AclPolicyDto> granteeList;

    @Override
    public void updateTableAclPolicies(TableIdentifier t1, boolean ig, String pvl, String prp) {
      resourceName = t1.toString();
      isGrant = ig;
      privilege = pvl;
      principal = prp;
      resourceType = "TABLE";
    }

    @Override
    public List<AclPolicyDto> getTableAclPolicies(TableIdentifier tableIdentifier) {
      return granteeList;
    }

    @Override
    public void updateDatabaseAclPolicies(Namespace n, boolean ig, String pvl, String prp) {
      resourceName = n.toString();
      isGrant = ig;
      privilege = pvl;
      principal = prp;
      resourceType = "DATABASE";
    }

    @Override
    public List<AclPolicyDto> getDatabaseAclPolicies(Namespace namespace) {
      return granteeList;
    }
  }
}
