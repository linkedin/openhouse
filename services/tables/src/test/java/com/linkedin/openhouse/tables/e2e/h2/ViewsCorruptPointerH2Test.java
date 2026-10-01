package com.linkedin.openhouse.tables.e2e.h2;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.tables.exception.ViewExceptionHandler;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.services.ViewsFeatureGate;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

/**
 * A stored VIEW row missing its required pointer facts is corrupt persisted state. A typed GET must
 * report a sanitized server fault rather than return the incomplete pointer or treat it as absent.
 * Rows are seeded directly into the real house-table store, not produced by a stubbed failure.
 */
@SpringBootTest(classes = {SpringH2Application.class, ViewsCorruptPointerH2Test.Config.class})
@AutoConfigureMockMvc
@ContextConfiguration(
    initializers = {
      PropertyOverrideContextInitializer.class,
      AuthorizationPropertiesInitializer.class
    })
public class ViewsCorruptPointerH2Test {

  private static final String DATABASE_ID = "corrupt_view_db";
  private static final String VIEW_ID = "corrupt_view";
  private static final String STORED_LOCATION =
      "file:/tmp/corrupt-view-root/00001-stored-pointer-marker.metadata.json";

  @Autowired private MockMvc mvc;
  @Autowired private HouseTableRepository houseTableRepository;
  @MockBean private ViewsFeatureGate viewsFeatureGate;

  private String jwtAccessToken;

  @BeforeEach
  public void setup() throws Exception {
    jwtAccessToken =
        new DummyTokenInterceptor.DummySecurityJWT("DUMMY_ANONYMOUS_USER").buildNoopJWT();
    Mockito.when(viewsFeatureGate.isEnabled(ArgumentMatchers.anyString())).thenReturn(true);
  }

  @AfterEach
  public void removeSeededRow() {
    houseTableRepository.deleteViewById(
        HouseTablePrimaryKey.builder().databaseId(DATABASE_ID).tableId(VIEW_ID).build());
  }

  @ParameterizedTest
  @ValueSource(strings = {"blankTableLocation", "blankStorageType"})
  public void incompleteStoredPointerIsASanitizedServerFault(String corruption) throws Exception {
    boolean blankLocation = "blankTableLocation".equals(corruption);
    houseTableRepository.saveView(
        HouseTable.builder()
            .databaseId(DATABASE_ID)
            .tableId(VIEW_ID)
            .tableUUID(UUID.randomUUID().toString())
            .tableLocation(blankLocation ? "" : STORED_LOCATION)
            .storageType(blankLocation ? "local" : "")
            .tableCreator("DUMMY_ANONYMOUS_USER")
            .creationTime(1L)
            .lastModifiedTime(1L)
            .build());

    MvcResult result =
        mvc.perform(
                MockMvcRequestBuilders.get("/v1/databases/" + DATABASE_ID + "/views/" + VIEW_ID)
                    .accept(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer " + jwtAccessToken))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.cause").doesNotExist())
            .andExpect(jsonPath("$.stacktrace").doesNotExist())
            .andExpect(jsonPath("$.metadataLocation").doesNotExist())
            .andReturn();

    String body = result.getResponse().getContentAsString();
    assertFalse(body.contains(STORED_LOCATION), body);
    assertFalse(body.contains("stored-pointer-marker"), body);
  }

  @TestConfiguration
  static class Config {
    @Bean
    ViewExceptionHandler viewExceptionHandler() {
      return new ViewExceptionHandler();
    }
  }
}
