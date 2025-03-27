package com.linkedin.openhouse.tables.mock.controller;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX;
import static com.linkedin.openhouse.tables.model.ServiceAuditModelConstants.EXCLUDE_FIELDS;
import static com.linkedin.openhouse.tables.model.ServiceAuditModelConstants.SERVICE_AUDIT_EVENT_CREATE_TABLE_FAILED;
import static com.linkedin.openhouse.tables.model.ServiceAuditModelConstants.SERVICE_AUDIT_EVENT_CREATE_TABLE_SUCCESS;
import static com.linkedin.openhouse.tables.model.ServiceAuditModelConstants.SERVICE_AUDIT_EVENT_RUNTIME_EXCEPTION;
import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildCreateUpdateTableRequestBody;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.audit.CachingRequestBodyFilter;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.controller.TablesController;
import com.linkedin.openhouse.tables.e2e.h2.ValidationUtilities;
import com.linkedin.openhouse.tables.mock.RequestConstants;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import javax.servlet.Filter;
import javax.servlet.http.HttpServletRequest;
import org.codehaus.jettison.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.web.FilterChainProxy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@SpringBootTest
@ContextConfiguration(initializers = AuthorizationPropertiesInitializer.class)
public class TablesControllerTest {

  private MockMvc mvc;

  private MockMvc mvcUnauthenticated;

  private String jwtAccessToken;

  private String invalidAccessToken;

  @Autowired private TablesController tablesController;

  @Autowired private OpenHouseExceptionHandler openHouseExceptionHandler;

  @Captor private ArgumentCaptor<ServiceAuditEvent> argCaptor;

  @MockBean private AuditHandler<ServiceAuditEvent> serviceAuditHandler;

  @BeforeEach
  public void setup() throws IOException, JSONException, ParseException {
    mvc =
        MockMvcBuilders.standaloneSetup(tablesController)
            .setControllerAdvice(openHouseExceptionHandler)
            .addInterceptors(new DummyTokenInterceptor())
            .addFilter(new CachingRequestBodyFilter())
            .build();

    mvcUnauthenticated =
        MockMvcBuilders.standaloneSetup(tablesController)
            .setControllerAdvice(openHouseExceptionHandler)
            .addFilter(new CachingRequestBodyFilter())
            .apply(
                springSecurity(
                    new FilterChainProxy(
                        new SecurityFilterChain() {
                          @Override
                          public boolean matches(HttpServletRequest request) {
                            return true;
                          }

                          @Override
                          public List<Filter> getFilters() {
                            return null;
                          }
                        })))
            .build();

    DummyTokenInterceptor.DummySecurityJWT dummySecurityJWT =
        new DummyTokenInterceptor.DummySecurityJWT("DUMMY_ANONYMOUS_USER");
    jwtAccessToken = dummySecurityJWT.buildNoopJWT();
    invalidAccessToken = jwtAccessToken.substring(5, 10);
  }

  @Test
  public void findTableById2xx() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/tables/t1")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(RequestConstants.TEST_GET_TABLE_RESPONSE_BODY.toJson()));
  }

  @Test
  public void findTableById4xx() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d404/tables/t1")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isNotFound());
  }

  @Test
  public void findTableById401() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d404/tables/t1")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isUnauthorized());
    mvc.perform(
            MockMvcRequestBuilders.get(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d404/tables/t1")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + invalidAccessToken))
        .andExpect(status().isUnauthorized());
  }

  @Test
  @MockUnauthenticatedUser
  public void findTableById403() throws Exception {
    mvcUnauthenticated
        .perform(
            MockMvcRequestBuilders.get(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/tables/t1")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }

  @Test
  public void createTable2xx() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.post(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/tables")
                .contentType(MediaType.APPLICATION_JSON)
                .content(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY.toJson())
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isCreated())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(RequestConstants.TEST_GET_TABLE_RESPONSE_BODY.toJson()));
  }

  @Test
  public void createTable4xx() throws Exception {
    for (String db : Arrays.asList("d400", "d404", "d409")) {
      mvc.perform(
              MockMvcRequestBuilders.post(
                      String.format(CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables", db))
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY.toJson())
                  .accept(MediaType.APPLICATION_JSON)
                  .header("Authorization", "Bearer " + jwtAccessToken))
          .andExpect(status().is(Integer.parseInt(db.substring(1))));
    }
  }

  @Test
  public void updateTable2xx() throws Exception {
    for (String db : Arrays.asList("d200", "d201")) {
      mvc.perform(
              MockMvcRequestBuilders.put(
                      String.format(CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/tb1", db))
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY.toJson())
                  .accept(MediaType.APPLICATION_JSON)
                  .header("Authorization", "Bearer " + jwtAccessToken))
          .andExpect(status().is(Integer.parseInt(db.substring(1))))
          .andExpect(content().contentType(MediaType.APPLICATION_JSON))
          .andExpect(content().json(RequestConstants.TEST_GET_TABLE_RESPONSE_BODY.toJson()));
    }
  }

  @Test
  public void updateTable4xx() throws Exception {
    for (String db : Arrays.asList("d400", "d404")) {
      mvc.perform(
              MockMvcRequestBuilders.put(
                      String.format(CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/tb1", db))
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY.toJson())
                  .accept(MediaType.APPLICATION_JSON)
                  .header("Authorization", "Bearer " + jwtAccessToken))
          .andExpect(status().is(Integer.parseInt(db.substring(1))));
    }
  }

  @Test
  public void dropTable2xx() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.delete(
                    CURRENT_MAJOR_VERSION_PREFIX + "/databases/d204/tables/t1")
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isNoContent())
        .andExpect(content().string(""));
  }

  @Test
  public void dropTable4xx() throws Exception {
    for (String db : Arrays.asList("d400", "d404")) {
      mvc.perform(
              MockMvcRequestBuilders.delete(
                      String.format(CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/tb1", db))
                  .header("Authorization", "Bearer " + jwtAccessToken))
          .andExpect(status().is(Integer.parseInt(db.substring(1))));
    }
  }

  /**
   * Specifically for
   * com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody#toJson() method
   * which handles null value that is needed for unset feature.
   *
   * <p>The requirement is, we need to ensure that the map whose keys are associated null value will
   * still survive after a serde process.
   */
  @Test
  public void testNullableSerialize() {
    Map<String, String> valueNullMap = new HashMap<>();
    valueNullMap.put("key", null);

    CreateUpdateTableRequestBody createUpdateTableRequestBody =
        CreateUpdateTableRequestBody.builder()
            .databaseId("db1")
            .tableId("tb1")
            .clusterId("cl1")
            .tableProperties(valueNullMap)
            .build();

    JsonObject deserialized =
        new Gson().fromJson(createUpdateTableRequestBody.toJson(), JsonObject.class);
    Assertions.assertFalse(deserialized.get("tableProperties").isJsonNull());
  }

  @Test
  public void testUpdateAclPoliciesOnTable() throws Exception {
    for (String db : Arrays.asList("d204", "d400", "d404", "d503", "d422")) {
      mvc.perform(
              MockMvcRequestBuilders.patch(
                      String.format(
                          CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/tb1/aclPolicies",
                          db))
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(RequestConstants.TEST_UPDATE_ACL_POLICIES_REQUEST_BODY.toJson())
                  .accept(MediaType.APPLICATION_JSON)
                  .header("Authorization", "Bearer " + jwtAccessToken))
          .andExpect(status().is(Integer.parseInt(db.substring(1))));
    }
  }

  @Test
  public void testGetAclPoliciesOnTable() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(
                    String.format(
                        CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/tables/tb1/aclPolicies"))
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(RequestConstants.TEST_GET_ACL_POLICIES_RESPONSE_BODY.toJson()));

    mvc.perform(
            MockMvcRequestBuilders.get(
                    CURRENT_MAJOR_VERSION_PREFIX + "/databases/d404/tables/tb1/aclPolicies")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isNotFound());
    mvc.perform(
            MockMvcRequestBuilders.get(
                    CURRENT_MAJOR_VERSION_PREFIX + "/databases/d503/tables/tb1/aclPolicies")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().is5xxServerError());
  }

  @Test
  public void testGetAclPoliciesForPrincipalOnTable() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get(
                    String.format(
                        CURRENT_MAJOR_VERSION_PREFIX
                            + "/databases/d200/tables/tb1/aclPolicies/testUserPrincipal"))
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(RequestConstants.TEST_GET_ACL_POLICIES_RESPONSE_BODY.toJson()));

    mvc.perform(
            MockMvcRequestBuilders.get(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d404/tables/tb1/aclPolicies/testUserPrincipal")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().isNotFound());
    mvc.perform(
            MockMvcRequestBuilders.get(
                    CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/d503/tables/tb1/aclPolicies/testUserPrincipal")
                .accept(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + jwtAccessToken))
        .andExpect(status().is5xxServerError());
  }

  @Test
  public void testServiceAuditSuccessfulPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.post(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d200/tables")
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer " + jwtAccessToken));
    Mockito.verify(serviceAuditHandler, Mockito.atLeastOnce()).audit(argCaptor.capture());
    ServiceAuditEvent actualEvent = argCaptor.getValue();
    Assertions.assertTrue(
        new ReflectionEquals(SERVICE_AUDIT_EVENT_CREATE_TABLE_SUCCESS, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testServiceAuditFailedPath() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.post(
                String.format(CURRENT_MAJOR_VERSION_PREFIX + "/databases/d404/tables"))
            .contentType(MediaType.APPLICATION_JSON)
            .content(RequestConstants.TEST_CREATE_TABLE_REQUEST_BODY.toJson())
            .accept(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer " + jwtAccessToken));
    Mockito.verify(serviceAuditHandler, Mockito.atLeastOnce()).audit(argCaptor.capture());
    ServiceAuditEvent actualEvent = argCaptor.getValue();
    Assertions.assertTrue(
        new ReflectionEquals(SERVICE_AUDIT_EVENT_CREATE_TABLE_FAILED, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testServiceAuditFailedPathRuntimeException() throws Exception {
    mvc.perform(
        MockMvcRequestBuilders.get(
                String.format(CURRENT_MAJOR_VERSION_PREFIX + "/databases/dnullpointer/tables/t1"))
            .accept(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer " + jwtAccessToken));
    Mockito.verify(serviceAuditHandler, Mockito.atLeastOnce()).audit(argCaptor.capture());
    ServiceAuditEvent actualEvent = argCaptor.getValue();
    Assertions.assertTrue(
        new ReflectionEquals(SERVICE_AUDIT_EVENT_RUNTIME_EXCEPTION, EXCLUDE_FIELDS)
            .matches(actualEvent));
  }

  @Test
  public void testCreateUpdateResponseCodeForVariousExceptions() throws Exception {
    List<AbstractMap.SimpleEntry<String, Integer>> list =
        Arrays.asList(
            new AbstractMap.SimpleEntry<>(
                "entityconcurrentmodificationexception", HttpStatus.CONFLICT.value()),
            new AbstractMap.SimpleEntry<>(
                "openhousecommitstateunknownexception", HttpStatus.SERVICE_UNAVAILABLE.value()),
            new AbstractMap.SimpleEntry<>(
                "requestvalidationfailureexception", HttpStatus.BAD_REQUEST.value()),
            new AbstractMap.SimpleEntry<>(
                "unprocessableentityexception", HttpStatus.UNPROCESSABLE_ENTITY.value()),
            new AbstractMap.SimpleEntry<>("alreadyexistsexception", HttpStatus.CONFLICT.value()),
            new AbstractMap.SimpleEntry<>(
                "orgapacheicebergexceptionsalreadyexistsexception", HttpStatus.CONFLICT.value()),
            new AbstractMap.SimpleEntry<>(
                "invalidschemaevolutionexception", HttpStatus.BAD_REQUEST.value()),
            new AbstractMap.SimpleEntry<>(
                "unsupportedclientoperationexception", HttpStatus.BAD_REQUEST.value()),
            new AbstractMap.SimpleEntry<>("accessdeniedexception", HttpStatus.FORBIDDEN.value()),
            new AbstractMap.SimpleEntry<>(
                "illegalstateexception", HttpStatus.INTERNAL_SERVER_ERROR.value()),
            new AbstractMap.SimpleEntry<>(
                "authorizationserviceexception", HttpStatus.SERVICE_UNAVAILABLE.value()),
            new AbstractMap.SimpleEntry<>("exception", HttpStatus.INTERNAL_SERVER_ERROR.value()));

    for (AbstractMap.SimpleEntry<String, Integer> pair : list) {
      String error = pair.getKey();
      int status = pair.getValue();
      GetTableResponseBody responseBodyForException =
          GET_TABLE_RESPONSE_BODY.toBuilder().databaseId("dException").tableId(error).build();
      mvcUnauthenticated
          .perform(
              MockMvcRequestBuilders.put(
                      String.format(
                          ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                              + "/databases/%s/tables/%s",
                          responseBodyForException.getDatabaseId(),
                          responseBodyForException.getTableId()))
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(
                      buildCreateUpdateTableRequestBody(responseBodyForException)
                          .toBuilder()
                          .baseTableVersion(INITIAL_TABLE_VERSION)
                          .build()
                          .toJson())
                  .accept(MediaType.APPLICATION_JSON))
          .andExpect(status().is(status))
          .andReturn();
    }
  }

  @Test
  public void testCreateLockPolicyOnTable() throws Exception {
    for (String db : Arrays.asList("d201", "d400", "d404", "d503", "d422")) {
      mvc.perform(
              MockMvcRequestBuilders.post(
                      String.format(
                          CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/tb1/lock", db))
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(RequestConstants.TEST_UPDATE_LOCK_POLICIES_REQUEST_BODY.toJson())
                  .accept(MediaType.APPLICATION_JSON)
                  .header("Authorization", "Bearer " + jwtAccessToken))
          .andExpect(status().is(Integer.parseInt(db.substring(1))));
    }
  }
}
