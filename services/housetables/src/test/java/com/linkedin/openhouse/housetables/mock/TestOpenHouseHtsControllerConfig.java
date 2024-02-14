package com.linkedin.openhouse.housetables.mock;

import com.linkedin.openhouse.housetables.api.handler.UserTableHtsApiHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class TestOpenHouseHtsControllerConfig {

  @Bean
  @Primary
  public UserTableHtsApiHandler createTestHouseTableApiHandler() {
    return new MockUserTableHtsApiHandler();
  }
}
