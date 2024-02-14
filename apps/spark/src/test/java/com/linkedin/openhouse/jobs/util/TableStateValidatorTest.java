package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.jobs.exception.TableValidationException;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableStateValidatorTest extends OpenHouseSparkITest {
  private static final String CATALOG_NAME = "openhouse";

  @Test
  void testShouldValidateTableInDB() throws Exception {
    final String tableName = "db.test_validation_sql";
    final String numberTableName = "db.test3validation_sql";
    final String tableNotPresent = "db.not_present";
    List<String> tableList = new ArrayList<>();
    tableList.add(tableName);
    tableList.add(numberTableName);
    try (SparkSession sparkSession = getSparkSession()) {
      sparkSession.sql(String.format("USE %s", CATALOG_NAME)).show();
      for (String fqtn : tableList) {
        String quotedFqtn = SparkJobUtil.getQuotedFqtn(fqtn);
        sparkSession.sql(String.format("DROP TABLE IF EXISTS %s", quotedFqtn)).show();
        sparkSession.sql(
            String.format("CREATE TABLE %s (data string, ts timestamp) USING iceberg", quotedFqtn));
        Assertions.assertDoesNotThrow(() -> TableStateValidator.run(sparkSession, tableName));
      }
      Assertions.assertThrows(
          TableValidationException.class,
          () ->
              TableStateValidator.validateTableInDB(
                  sparkSession, TableIdentifier.parse(tableNotPresent)));
      Assertions.assertThrows(
          TableValidationException.class,
          () -> TableStateValidator.run(sparkSession, tableNotPresent));
    }
  }
}
