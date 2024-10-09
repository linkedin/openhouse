package com.linkedin.openhouse.spark.e2e.ddl;

import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.spark.SparkTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class UseCatalogTest {

  @Test
  public void testUseCatalog() {
    Assertions.assertDoesNotThrow(() -> spark.sql("USE openhouse").show());
  }
}
