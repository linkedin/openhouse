package com.linkedin.openhouse.internal.catalog.mapper;

import com.linkedin.openhouse.housetables.client.api.ToggleStatusApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepositoryImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@SpringBootTest
public class HouseTableMapperTest {

  /**
   * Tests that doesn't care on HTS server should import this test configuration as
   *
   * @import(classes = MockConfiguration.class)
   */
  @TestConfiguration
  public static class MockConfiguration {
    @Bean
    public UserTableApi provideMockHtsApiInstance() {
      // Routing the client to access port from Mock server so that Mock server can respond with
      // stub response.
      ApiClient apiClient = new ApiClient();
      return new UserTableApi(apiClient);
    }

    @Bean
    public ToggleStatusApi provideMockHtsApiInstanceForToggle() {
      // Routing the client to access port from Mock server so that Mock server can respond with
      // stub response.
      ApiClient apiClient = new ApiClient();
      return new ToggleStatusApi(apiClient);
    }

    @Bean
    public HouseTableRepository provideRealHtsRepository() {
      return new HouseTableRepositoryImpl();
    }
  }

  @Autowired protected HouseTableMapper houseTableMapper;
}
