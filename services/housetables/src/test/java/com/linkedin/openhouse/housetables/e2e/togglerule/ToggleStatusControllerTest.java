package com.linkedin.openhouse.housetables.e2e.togglerule;

import static com.linkedin.openhouse.housetables.e2e.togglerule.ToggleStatusesTestConstants.*;
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.ToggleStatusHtsJdbcRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@AutoConfigureMockMvc
public class ToggleStatusControllerTest {
  @Autowired MockMvc mvc;

  @Autowired ToggleStatusHtsJdbcRepository htsRepository;

  @Test
  public void testGetTableToggleStatus() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/togglestatuses")
                .param("databaseId", TEST_DB_NAME)
                .param("tableId", TEST_TABLE_NAME)
                .param("featureId", TEST_FEATURE_NAME)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.entity.status", is(equalTo("ACTIVE"))));

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/togglestatuses")
                /* Knowing these are the exact Id, instead of patterns with wildcard */
                .param("databaseId", TEST_RULE_1.getDatabasePattern())
                .param("tableId", TEST_RULE_1.getTablePattern())
                .param("featureId", TEST_RULE_1.getFeature())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.entity.status", is(equalTo("ACTIVE"))));

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/togglestatuses")
                /* Knowing these are the exact Id, instead of patterns with wildcard */
                .param("databaseId", TEST_RULE_2.getDatabasePattern())
                .param("tableId", TEST_RULE_2.getTablePattern())
                .param("featureId", TEST_RULE_2.getFeature())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.entity.status", is(equalTo("ACTIVE"))));

    mvc.perform(
            MockMvcRequestBuilders.get("/hts/togglestatuses")
                .param("databaseId", TEST_DB_NAME)
                .param("tableId", TEST_TABLE_NAME)
                .param(
                    "featureId",
                    TEST_FEATURE_NAME + "postfix") /* something that are not activated*/
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.entity.status", is(equalTo("INACTIVE"))));
  }
}
