package com.linkedin.openhouse.housetables.mock;

import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import com.linkedin.openhouse.housetables.services.UserTablesService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SpringBootApplication
@ComponentScan(
    basePackages = {"com.linkedin.openhouse"},
    excludeFilters =
        @ComponentScan.Filter(
            type = FilterType.REGEX,
            pattern = {
              "com.linkedin.openhouse.housetables.e2e.*",
              "com.linkedin.openhouse.internal.catalog.*"
            }))
public class MockHouseTablesApplication {
  public static void main(String[] args) {
    SpringApplication.run(MockHouseTablesApplication.class, args);
  }

  @MockBean HtsRepository<UserTableRow, UserTableRowPrimaryKey> htsRepository;

  @MockBean UserTablesService userTablesService;
}
