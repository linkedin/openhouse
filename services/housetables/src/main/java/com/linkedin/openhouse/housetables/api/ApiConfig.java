package com.linkedin.openhouse.housetables.api;

import com.linkedin.openhouse.housetables.api.handler.OpenHouseUserTableHtsApiHandler;
import com.linkedin.openhouse.housetables.api.handler.UserTableHtsApiHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApiConfig {
  @Bean
  public UserTableHtsApiHandler tableHtsApiHandler() {
    return new OpenHouseUserTableHtsApiHandler();
  }
}
