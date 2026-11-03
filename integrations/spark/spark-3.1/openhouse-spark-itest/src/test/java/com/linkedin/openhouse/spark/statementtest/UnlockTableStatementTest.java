package com.linkedin.openhouse.spark.statementtest;

import com.linkedin.openhouse.gen.tables.client.model.CreateUpdateLockRequestBody.ReasonEnum;
import com.linkedin.openhouse.javaclient.api.SupportsUnlock;
import com.linkedin.openhouse.javaclient.api.UnlockTableTarget;
import com.linkedin.openhouse.javaclient.exception.UnlockValidationException;
import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.ResolvedUnlockTable;
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.UnlockTable;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Locale;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class UnlockTableStatementTest {

  private static final String UNLOCK_ENABLED = "spark.sql.openhouse.unlock.enabled";
  private static SparkSession spark = null;

  @Test
  public void testOmittedReasonIsTypedLegacy() throws ParseException {
    UnlockTable plan =
        (UnlockTable)
            spark.sessionState().sqlParser().parsePlan("ALTER TABLE openhouse.db.table UNLOCK");
    Assertions.assertEquals(ReasonEnum.LEGACY, plan.reason());
  }

  @Test
  public void testExplicitReasonsAreTyped() throws ParseException {
    for (ReasonEnum reason : ReasonEnum.values()) {
      UnlockTable plan =
          (UnlockTable)
              spark
                  .sessionState()
                  .sqlParser()
                  .parsePlan(
                      "ALTER TABLE openhouse.db.table UNLOCK REASON `"
                          + reason.getValue().toLowerCase(Locale.ROOT)
                          + "`");
      Assertions.assertEquals(reason, plan.reason());
    }
  }

  @Test
  public void testUnlockDisabledByDefault() {
    spark.conf().unset(UNLOCK_ENABLED);
    assertDisabled();
  }

  @Test
  public void testUnlockExplicitlyDisabled() {
    spark.conf().set(UNLOCK_ENABLED, false);
    assertDisabled();
  }

  private void assertDisabled() {
    for (String suffix : new String[] {"", " REASON SYSTEM_ONLY"}) {
      UnsupportedOperationException failure =
          Assertions.assertThrows(
              UnsupportedOperationException.class,
              () -> spark.sql("ALTER TABLE openhouse.db.table UNLOCK" + suffix));
      Assertions.assertTrue(failure.getMessage().contains(UNLOCK_ENABLED), failure.getMessage());
    }
    Assertions.assertNull(UnlockHadoopCatalog.unlockedTarget);
  }

  @Test
  public void testAnalysisResolvesTargetWithoutUnlocking() throws ParseException {
    LogicalPlan parsed =
        spark
            .sessionState()
            .sqlParser()
            .parsePlan("ALTER TABLE openhouse.db.table UNLOCK REASON SYSTEM_ONLY");
    LogicalPlan analyzed = spark.sessionState().analyzer().execute(parsed);
    spark.sessionState().analyzer().checkAnalysis(analyzed);
    Assertions.assertEquals("db.table", ((ResolvedUnlockTable) analyzed).target().toString());
    Assertions.assertEquals(ReasonEnum.SYSTEM_ONLY, ((ResolvedUnlockTable) analyzed).reason());
    Assertions.assertNull(UnlockHadoopCatalog.unlockedTarget);
  }

  @Test
  public void testTargetDoesNotRetainMutableNamespace() throws UnlockValidationException {
    String[] namespace = {"my db"};
    UnlockTableTarget target = UnlockTableTarget.of(namespace, "ta`ble");
    namespace[0] = "..";
    Assertions.assertEquals("my db", target.getDatabaseId());
    Assertions.assertEquals("ta`ble", target.getTableId());
  }

  @Test
  public void testTargetFactoryRejectsInvalidComponents() {
    String[][] invalidNamespaces = {null, {}, {"db", "extra"}, {null}, {""}, {"."}, {".."}};
    for (String[] namespace : invalidNamespaces) {
      Assertions.assertThrows(
          UnlockValidationException.class, () -> UnlockTableTarget.of(namespace, "table"));
    }
    for (String table : new String[] {null, "", ".", ".."}) {
      Assertions.assertThrows(
          UnlockValidationException.class, () -> UnlockTableTarget.of(new String[] {"db"}, table));
    }
  }

  @Test
  public void testPlanningKeepsTheAnalyzedNamespace() throws ParseException {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS openhouse.original");
    spark.sql("CREATE NAMESPACE IF NOT EXISTS openhouse.changed");
    spark.sql("USE openhouse.original");
    try {
      LogicalPlan analyzed =
          spark
              .sessionState()
              .analyzer()
              .execute(spark.sessionState().sqlParser().parsePlan("ALTER TABLE target UNLOCK"));
      spark.sessionState().analyzer().checkAnalysis(analyzed);
      Assertions.assertNull(UnlockHadoopCatalog.unlockedTarget);
      spark.sql("USE openhouse.changed");
      spark.sessionState().planner().plan(analyzed).next().executeCollect();
      assertUnlocked("original.target", ReasonEnum.LEGACY);
      Assertions.assertSame(
          ((ResolvedUnlockTable) analyzed).target(), UnlockHadoopCatalog.unlockedTarget);
    } finally {
      spark.sql("USE spark_catalog");
    }
  }

  @Test
  public void testAuthorizationFailurePreventsUnlock() {
    UnlockHadoopCatalog.authorized = false;
    Assertions.assertThrows(
        SecurityException.class,
        () -> spark.sql("ALTER TABLE openhouse.db.table UNLOCK REASON SYSTEM_ONLY"));
    Assertions.assertNull(UnlockHadoopCatalog.unlockedTarget);
  }

  @Test
  public void testUnlockLegacyLock() {
    spark.sql("ALTER TABLE openhouse.db.table UNLOCK");
    assertUnlocked("db.table", ReasonEnum.LEGACY);
  }

  @Test
  public void testUnlockWithReason() {
    spark.sql("ALTER TABLE openhouse.db.table UNLOCK REASON SYSTEM_ONLY");
    assertUnlocked("db.table", ReasonEnum.SYSTEM_ONLY);
  }

  @Test
  public void testUnlockLowerCase() {
    spark.sql("alter table openhouse.db.table unlock reason system_only");
    assertUnlocked("db.table", ReasonEnum.SYSTEM_ONLY);
  }

  @Test
  public void testUnlockReasonIgnoresDefaultLocale() {
    Locale defaultLocale = Locale.getDefault();
    Locale displayLocale = Locale.getDefault(Locale.Category.DISPLAY);
    Locale formatLocale = Locale.getDefault(Locale.Category.FORMAT);
    Locale.setDefault(new Locale("tr", "TR"));
    try {
      for (ReasonEnum reason : ReasonEnum.values()) {
        spark.sql(
            "ALTER TABLE openhouse.db.table UNLOCK REASON "
                + reason.getValue().toLowerCase(Locale.ROOT));
        assertUnlocked("db.table", reason);
      }
    } finally {
      Locale.setDefault(defaultLocale);
      Locale.setDefault(Locale.Category.DISPLAY, displayLocale);
      Locale.setDefault(Locale.Category.FORMAT, formatLocale);
    }
  }

  @Test
  public void testUnlockAfterUseCatalog() {
    spark.sql("USE openhouse");
    try {
      spark.sql("ALTER TABLE db.table UNLOCK");
      assertUnlocked("db.table", ReasonEnum.LEGACY);
    } finally {
      spark.sql("USE spark_catalog");
    }
  }

  @Test
  public void testUnlockQuotedAndKeywordIdentifiers() {
    spark.sql("ALTER TABLE openhouse.`my db`.`unlock` UNLOCK REASON `system_only`");
    assertUnlocked("my db.unlock", ReasonEnum.SYSTEM_ONLY);
    spark.sql("ALTER TABLE openhouse.reason.unlock /* comment */ UNLOCK");
    assertUnlocked("reason.unlock", ReasonEnum.LEGACY);
  }

  @Test
  public void testUnlockSpacedAndCommentedIdentifiers() {
    spark.sql("ALTER TABLE openhouse . db . table UNLOCK");
    assertUnlocked("db.table", ReasonEnum.LEGACY);
    setup();
    spark.sql("ALTER TABLE openhouse./* comment */db.table UNLOCK--comment");
    assertUnlocked("db.table", ReasonEnum.LEGACY);
    setup();
    spark.sql("ALTER /* outer /* inner */ comment */ TABLE openhouse.db.table UNLOCK");
    assertUnlocked("db.table", ReasonEnum.LEGACY);
    setup();
    spark.sql("--comment\rALTER --comment\rTABLE openhouse.db.table UNLOCK");
    assertUnlocked("db.table", ReasonEnum.LEGACY);
    setup();
    spark.sql("ALTER TABLE openhouse.db.`a/*` UNLOCK /* comment */");
    assertUnlocked("db.a/*", ReasonEnum.LEGACY);
  }

  @Test
  public void testUnlockKeepsEscapedBackticks() {
    spark.sql("ALTER TABLE openhouse.db.`ta``ble` UNLOCK REASON `system_only`");
    assertUnlocked("db.ta`ble", ReasonEnum.SYSTEM_ONLY);
  }

  @Test
  public void testLongQuotedIdentifiers() throws ParseException {
    String name = String.join("", Collections.nCopies(10000, "a``"));
    Assertions.assertFalse(
        spark
                .sessionState()
                .sqlParser()
                .parsePlan("ALTER TABLE openhouse.db.`" + name + "` SET TBLPROPERTIES ('k' = 'v')")
            instanceof UnlockTable);
    spark.sql("ALTER TABLE openhouse.db.`" + name + "` UNLOCK");
    assertUnlocked("db." + name.replace("``", "`"), ReasonEnum.LEGACY);
  }

  @Test
  public void testUnlockSyntaxErrors() {
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.db.table UNLOCK REASON"));
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.db.table UNLOCK SYSTEM_ONLY"));
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.db.table UNLOCK REASON SYSTEM_ONLY NOW"));
    Assertions.assertThrows(
        ParseException.class, () -> spark.sql("ALTER TABLE openhouse.db.table UNLOCKED"));
    Assertions.assertNull(UnlockHadoopCatalog.unlockedTarget);
  }

  @Test
  public void testOtherAlterTableStatementsAreNotUnlock() throws ParseException {
    for (String sql :
        new String[] {
          "ALTER TABLE openhouse.db.table RENAME TO unlock",
          "ALTER TABLE openhouse.db.table SET TBLPROPERTIES ('note' = 'unlock')",
          "ALTER TABLE openhouse.db.table ADD COLUMNS (unlock string)"
        }) {
      Assertions.assertFalse(
          spark.sessionState().sqlParser().parsePlan(sql) instanceof UnlockTable, sql);
    }
  }

  @Test
  public void testUnsupportedCatalog() throws ParseException {
    LogicalPlan parsed =
        spark.sessionState().sqlParser().parsePlan("ALTER TABLE spark_catalog.db.table UNLOCK");
    Exception exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                spark
                    .sessionState()
                    .analyzer()
                    .checkAnalysis(spark.sessionState().analyzer().execute(parsed)));
    Assertions.assertTrue(
        exception.getMessage().contains("Catalog 'spark_catalog' does not support UNLOCK"));
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
                "com.linkedin.openhouse.spark.statementtest.UnlockTableStatementTest$UnlockHadoopCatalog")
            .config("spark.sql.catalog.openhouse.warehouse", unittest.toString())
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", unittest.toString())
            .getOrCreate();
  }

  @BeforeEach
  public void setup() {
    spark.conf().set(UNLOCK_ENABLED, true);
    UnlockHadoopCatalog.unlockedTarget = null;
    UnlockHadoopCatalog.authorized = true;
    UnlockHadoopCatalog.reason = null;
  }

  @AfterAll
  public void tearDownSpark() {
    spark.close();
  }

  private void assertUnlocked(String tableName, ReasonEnum reason) {
    Assertions.assertNotNull(UnlockHadoopCatalog.unlockedTarget, "Unlock missing");
    Assertions.assertEquals(
        tableName, UnlockHadoopCatalog.unlockedTarget.toString(), "Table mismatch");
    Assertions.assertEquals(reason, UnlockHadoopCatalog.reason, "Reason mismatch");
  }

  public static class UnlockHadoopCatalog extends HadoopCatalog implements SupportsUnlock {
    public static UnlockTableTarget unlockedTarget;
    public static boolean authorized;
    public static ReasonEnum reason;

    @Override
    public void unlockTable(UnlockTableTarget target, ReasonEnum lockReason) {
      if (!authorized) {
        throw new SecurityException("Not authorized to unlock " + target);
      }
      unlockedTarget = target;
      reason = lockReason;
    }
  }
}
