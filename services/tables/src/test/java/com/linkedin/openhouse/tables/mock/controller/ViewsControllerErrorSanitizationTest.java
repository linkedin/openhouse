package com.linkedin.openhouse.tables.mock.controller;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.common.audit.CachingRequestBodyFilter;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseViewsApiHandler;
import com.linkedin.openhouse.tables.api.validator.ViewsApiValidator;
import com.linkedin.openhouse.tables.controller.ViewsController;
import com.linkedin.openhouse.tables.dto.mapper.ViewsMapper;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.exception.ViewExceptionHandler;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.services.ViewsService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@SpringBootTest
@ContextConfiguration(initializers = AuthorizationPropertiesInitializer.class)
public class ViewsControllerErrorSanitizationTest {

  private static final String ACTING_PRINCIPAL = "DUMMY_ANONYMOUS_USER";
  private static final String VIEWS_PATH =
      "/v1/databases/" + ViewModelConstants.DATABASE_ID + "/views/" + ViewModelConstants.VIEW_ID;

  @Autowired private ViewsApiValidator viewsApiValidator;
  @Autowired private ViewsMapper viewsMapper;
  @Autowired private OpenHouseExceptionHandler openHouseExceptionHandler;

  private ViewsService viewsService;
  private MockMvc mvc;
  private String jwtAccessToken;

  @BeforeEach
  public void setup() throws Exception {
    viewsService = Mockito.mock(ViewsService.class);
    mvc = standaloneMvcBackedBy(viewsService);
    jwtAccessToken = new DummyTokenInterceptor.DummySecurityJWT(ACTING_PRINCIPAL).buildNoopJWT();
  }

  @Test
  public void viewAdviceKeepsCauseInternalAndOffTheWire() throws Exception {
    RuntimeException sensitiveCause =
        new RuntimeException(
            "SELECT secret_sql FROM x; schema={secret}; base=file:/metadata-token;"
                + " pageToken=page-secret");
    when(viewsService.getView(
            ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL))
        .thenThrow(
            new ViewApiException(
                ViewErrorCode.VIEW_SERVICE_UNAVAILABLE,
                "View service unavailable",
                sensitiveCause));

    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isServiceUnavailable())
        .andExpect(content().string(containsString("View service unavailable")))
        .andExpect(content().string(not(containsString("secret_sql"))))
        .andExpect(content().string(not(containsString("schema={secret}"))))
        .andExpect(content().string(not(containsString("metadata-token"))))
        .andExpect(content().string(not(containsString("page-secret"))))
        .andExpect(content().string(not(containsString("stacktrace"))))
        .andExpect(content().string(not(containsString("RuntimeException"))));

    verify(viewsService)
        .getView(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL);
  }

  @Test
  public void unwrappedRuntimeFailureStillMapsToSanitized500ThroughResidualHandler()
      throws Exception {
    when(viewsService.getView(
            ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL))
        .thenThrow(new IllegalStateException("SELECT unwrapped_secret_sql FROM x"));

    mvc.perform(
            MockMvcRequestBuilders.get(VIEWS_PATH)
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.cause").doesNotExist())
        .andExpect(jsonPath("$.stacktrace").doesNotExist())
        .andExpect(content().string(not(containsString("unwrapped_secret_sql"))))
        .andExpect(content().string(not(containsString("IllegalStateException"))));
  }

  private MockMvc standaloneMvcBackedBy(ViewsService service) {
    OpenHouseViewsApiHandler handler = new OpenHouseViewsApiHandler();
    ReflectionTestUtils.setField(handler, "viewsApiValidator", viewsApiValidator);
    ReflectionTestUtils.setField(handler, "viewsService", service);
    ReflectionTestUtils.setField(handler, "viewsMapper", viewsMapper);

    ViewsController controller = new ViewsController();
    ReflectionTestUtils.setField(controller, "viewsApiHandler", handler);

    return MockMvcBuilders.standaloneSetup(controller)
        .setControllerAdvice(new ViewExceptionHandler(), openHouseExceptionHandler)
        .addInterceptors(new DummyTokenInterceptor())
        .addFilter(new CachingRequestBodyFilter())
        .build();
  }
}
