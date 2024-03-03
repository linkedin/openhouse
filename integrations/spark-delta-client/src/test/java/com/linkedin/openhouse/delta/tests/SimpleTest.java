package com.linkedin.openhouse.delta.tests;

import com.linkedin.openhouse.delta.BaseLiOHTest;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class SimpleTest extends BaseLiOHTest {

  @Test
  public void basicSQLCatalog() throws Exception {
    SparkSession sparkSession = getSparkSession();
    sparkSession.sql("CREATE TABLE openhouse.db.t3 (col1 string, col2 string, col3 string)").show();
    sparkSession.sql("INSERT INTO openhouse.db.t3 VALUES ('a', 'b', 'c')").show();
    sparkSession.sql("INSERT INTO openhouse.db.t3 VALUES ('c', 'd', 'd')").show();
    sparkSession.sql("SELECT * FROM openhouse.db.t3").show();
    sparkSession.sql("DROP TABLE openhouse.db.t3").show();
  }
}
