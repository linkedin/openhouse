package com.linkedin.openhouse.tables.mock.controller;

import static com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.common.audit.CachingRequestBodyFilter;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.tables.controller.IcebergSnapshotsController;
import com.linkedin.openhouse.tables.mock.RequestConstants;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import java.text.ParseException;
import org.codehaus.jettison.json.JSONException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@SpringBootTest
@ContextConfiguration(initializers = AuthorizationPropertiesInitializer.class)
public class IcebergSnapshotsControllerTest {
  private MockMvc mvc;

  @Autowired private IcebergSnapshotsController icebergSnapshotsController;

  @Autowired private OpenHouseExceptionHandler openHouseExceptionHandler;

  private String jwtAccessToken;

  @BeforeEach
  public void setup() throws JSONException, ParseException {
    mvc =
        MockMvcBuilders.standaloneSetup(icebergSnapshotsController)
            .setControllerAdvice(openHouseExceptionHandler)
            .addInterceptors(new DummyTokenInterceptor())
            .addFilter(new CachingRequestBodyFilter())
            .build();
    DummyTokenInterceptor.DummySecurityJWT dummySecurityJWT =
        new DummyTokenInterceptor.DummySecurityJWT("DUMMY_ANONYMOUS_USER");
    jwtAccessToken = dummySecurityJWT.buildNoopJWT();
  }

  @Test
  public void testCreated() throws Exception {
    performPut("d201")
        .andExpect(status().isCreated())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(RequestConstants.TEST_GET_TABLE_RESPONSE_BODY.toJson()));
  }

  @Test
  public void testUpdated() throws Exception {
    performPut("d200")
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(RequestConstants.TEST_GET_TABLE_RESPONSE_BODY.toJson()));
  }

  @Test
  public void testClientError() throws Exception {
    performPut("d400").andExpect(status().is4xxClientError());
  }

  private ResultActions performPut(String databaseId) throws Exception {
    return mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/t1/iceberg/v2/snapshots",
                    databaseId))
            .accept(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer " + jwtAccessToken)
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_ICEBERG_SNAPSHOTS_REQUEST_BODY.toJson()));
  }
}
