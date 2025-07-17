package com.linkedin.openhouse.housetables.config;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@TestPropertySource(properties = {"spring.datasource.hikari.maximum-pool-size=20"})
public class HikariCpConfigTest {

  @Autowired private DataSource dataSource;

  @Test
  public void testHikariCpMaximumPoolSize() {
    Assertions.assertTrue(dataSource instanceof HikariDataSource);
    HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
    Assertions.assertEquals(20, hikariDataSource.getMaximumPoolSize());
  }
}
