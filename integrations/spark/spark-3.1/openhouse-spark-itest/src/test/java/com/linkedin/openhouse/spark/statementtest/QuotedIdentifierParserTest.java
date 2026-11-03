package com.linkedin.openhouse.spark.statementtest;

import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseSparkSqlExtensionsParser;
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.GrantRevokeStatement;
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.SetColumnPolicyTag;
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.SetHistoryPolicy;
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.SetReplicationPolicy;
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.SetRetentionPolicy;
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.SetSharingPolicy;
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.ShowGrantsStatement;
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.UnSetReplicationPolicy;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.Seq;

class QuotedIdentifierParserTest {
  private static final String TABLE = "`open``house`.`da.ta`.`ta````ble`";
  private final ParserInterface sparkParser = CatalystSqlParser$.MODULE$;
  private final ParserInterface parser = new OpenhouseSparkSqlExtensionsParser(sparkParser);

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      quoteCharacter = '~',
      value = {
        "ordinary|ordinary",
        "`ordinary`|ordinary",
        "`MiXeD`|MiXeD",
        "`ta``ble`|ta`ble",
        "`ta````ble`|ta``ble",
        "```edge```|`edge`",
        "````|`",
        "`db.table`|db.table",
        "`with space`|with space",
        "`/*x*/--y`|/*x*/--y",
        "``|~~"
      })
  void matchesSparkIdentifierDecoding(String identifier, String expected) throws ParseException {
    Seq<String> sparkName = sparkParser.parseMultipartIdentifier(identifier);
    Assertions.assertEquals(1, sparkName.size());
    Assertions.assertEquals(expected, sparkName.apply(0), "Spark native identifier");
    GrantRevokeStatement plan =
        (GrantRevokeStatement) parser.parsePlan("GRANT SELECT ON TABLE " + identifier + " TO role");
    Assertions.assertEquals(sparkName, plan.resourceName(), "OpenHouse resource identifier");
  }

  @ParameterizedTest
  @CsvSource({"GRANT,TO", "REVOKE,FROM"})
  void preservesGrantRevokeResourceAndPrincipal(String command, String preposition)
      throws ParseException {
    GrantRevokeStatement plan =
        (GrantRevokeStatement)
            parser.parsePlan(
                command + " SELECT ON TABLE " + TABLE + " " + preposition + " `us``er`");
    assertTableName(plan.resourceName());
    Assertions.assertEquals("us`er", plan.principal());
  }

  @ParameterizedTest
  @ValueSource(strings = {"TABLE", "DATABASE"})
  void preservesShowGrantsResource(String resourceType) throws ParseException {
    ShowGrantsStatement plan =
        (ShowGrantsStatement) parser.parsePlan("SHOW GRANTS ON " + resourceType + " " + TABLE);
    assertTableName(plan.resourceName());
  }

  @Test
  void preservesRetentionIdentifiersAndPatternLiteral() throws ParseException {
    SetRetentionPolicy plan =
        (SetRetentionPolicy)
            parser.parsePlan(
                "ALTER TABLE "
                    + TABLE
                    + " SET POLICY (RETENTION=30d ON COLUMN `ts``col` WHERE PATTERN='yyyy``MM')");
    assertTableName(plan.tableName());
    Assertions.assertEquals("ts`col", plan.colName().get());
    Assertions.assertEquals("'yyyy``MM'", plan.colPattern().get());
  }

  @Test
  void preservesReplicationIdentifierAndDestinationLiteral() throws ParseException {
    SetReplicationPolicy plan =
        (SetReplicationPolicy)
            parser.parsePlan(
                "ALTER TABLE " + TABLE + " SET POLICY (REPLICATION=({DESTINATION:'cl``uster'}))");
    assertTableName(plan.tableName());
    Assertions.assertEquals("'cl``uster'", plan.replicationPolicies().apply(0)._1());
  }

  @Test
  void preservesUnsetReplicationIdentifier() throws ParseException {
    UnSetReplicationPolicy plan =
        (UnSetReplicationPolicy)
            parser.parsePlan("ALTER TABLE " + TABLE + " UNSET POLICY (REPLICATION)");
    assertTableName(plan.tableName());
  }

  @Test
  void preservesSharingIdentifier() throws ParseException {
    SetSharingPolicy plan =
        (SetSharingPolicy) parser.parsePlan("ALTER TABLE " + TABLE + " SET POLICY (SHARING=true)");
    assertTableName(plan.tableName());
  }

  @Test
  void preservesHistoryIdentifier() throws ParseException {
    SetHistoryPolicy plan =
        (SetHistoryPolicy)
            parser.parsePlan(
                "ALTER TABLE " + TABLE + " SET POLICY (HISTORY MAX_AGE=3d VERSIONS=2)");
    assertTableName(plan.tableName());
  }

  @Test
  void decodesModifyColumnIdentifierOnce() throws ParseException {
    SetColumnPolicyTag plan =
        (SetColumnPolicyTag)
            parser.parsePlan("ALTER TABLE " + TABLE + " MODIFY COLUMN `co````l` SET TAG=(PII)");
    assertTableName(plan.tableName());
    Assertions.assertEquals("co``l", plan.colName());
  }

  @ParameterizedTest
  @ValueSource(strings = {"`unfinished", "`ta`ble`"})
  void rejectsMalformedQuotedIdentifiers(String identifier) {
    Assertions.assertThrows(
        ParseException.class, () -> sparkParser.parseMultipartIdentifier(identifier));
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> parser.parsePlan("SHOW GRANTS ON TABLE " + identifier));
  }

  private void assertTableName(Seq<String> actual) throws ParseException {
    Assertions.assertEquals(sparkParser.parseMultipartIdentifier(TABLE), actual);
    Assertions.assertEquals("open`house", actual.apply(0));
    Assertions.assertEquals("da.ta", actual.apply(1));
    Assertions.assertEquals("ta``ble", actual.apply(2));
  }
}
