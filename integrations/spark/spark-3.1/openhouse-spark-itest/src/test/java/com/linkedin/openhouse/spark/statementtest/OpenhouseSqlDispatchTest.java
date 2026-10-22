package com.linkedin.openhouse.spark.statementtest;

import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseSparkSqlExtensionsParser;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class OpenhouseSqlDispatchTest {
  private final ParserInterface delegate = CatalystSqlParser$.MODULE$;
  private final ParserInterface parser = new OpenhouseSparkSqlExtensionsParser(delegate);

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      quoteCharacter = '~',
      value = {
        "ALTER TABLE db.t SET /* outer /* inner */ tail */ POLICY (RETENTION=30d)|SetRetentionPolicy",
        "ALTER TABLE db.t UNSET /* outer /* inner */ tail */ POLICY (REPLICATION)|UnSetReplicationPolicy",
        "ALTER TABLE db.t MODIFY /* outer /* inner */ tail */ COLUMN c SET TAG=(PII)|SetColumnPolicyTag",
        "-- comment\rALTER /* outer /* inner */ tail */ TABLE db.t UNLOCK|UnlockTable",
        "alter table db.`ta``ble` set policy (sharing=true)|SetSharingPolicy",
        "ALTER TABLE db.t SET POLICY (HISTORY MAX_AGE=3d VERSIONS=2)|SetHistoryPolicy",
        "ALTER TABLE db.t SET POLICY (REPLICATION=({DESTINATION:'cluster'}))|SetReplicationPolicy"
      })
  void routesExtensionActionsByTokens(String sql, String planName) throws ParseException {
    Assertions.assertEquals(planName, parser.parsePlan(sql).getClass().getSimpleName());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ALTER TABLE db.t SET TBLPROPERTIES ('note'='set policy')",
        "ALTER TABLE db.t SET TBLPROPERTIES ('note'='unset policy')",
        "ALTER TABLE db.t SET TBLPROPERTIES ('note'='modify column and set tag')",
        "ALTER TABLE db.`set policy` RENAME TO next_table",
        "ALTER TABLE db.t ADD COLUMNS (`unset policy` STRING)",
        "ALTER TABLE db.t RENAME TO unlock",
        "ALTER TABLE db.`ta``ble` SET TBLPROPERTIES ('note'='UNLOCK')",
        "ALTER VIEW db.t SET TBLPROPERTIES ('note'='unset policy')",
        "SELECT 'unset policy'"
      })
  void delegatesOrdinaryStatementsUnchanged(String sql) throws ParseException {
    LogicalPlan expected = delegate.parsePlan(sql);
    ParserInterface sparkParser = Mockito.mock(ParserInterface.class);
    Mockito.when(sparkParser.parsePlan(sql)).thenReturn(expected);
    Assertions.assertSame(
        expected, new OpenhouseSparkSqlExtensionsParser(sparkParser).parsePlan(sql));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ALTER TABLE db.t SET POLICY",
        "ALTER TABLE db.t UNSET POLICY",
        "ALTER TABLE db.t MODIFY COLUMN",
        "ALTER TABLE db.t MODIFY COLUMN c SET POLICY=(PII)",
        "ALTER TABLE db.t UNLOCK REASON"
      })
  void malformedExtensionActionsKeepExtensionErrors(String sql) {
    Assertions.assertThrows(OpenhouseParseException.class, () -> parser.parsePlan(sql));
  }
}
