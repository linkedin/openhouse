package com.linkedin.openhouse.housetables.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.handler.JobTableHtsApiHandler;
import com.linkedin.openhouse.housetables.api.handler.SoftDeletedUserTableHtsApiHandler;
import com.linkedin.openhouse.housetables.api.handler.ToggleStatusesApiHandler;
import com.linkedin.openhouse.housetables.api.handler.UserTableHtsApiHandler;
import com.linkedin.openhouse.housetables.controller.JobTablesController;
import com.linkedin.openhouse.housetables.controller.ToggleStatusesController;
import com.linkedin.openhouse.housetables.controller.UserHouseTablesController;
import com.linkedin.openhouse.housetables.dto.mapper.JobMapper;
import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mapstruct.factory.Mappers;
import org.mockito.invocation.Invocation;
import org.mockito.stubbing.Answer;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** No Spring context/database: selective keys must survive controller binding and dispatch. */
class HtsEndpointIndexRoutingTest {
  static Stream<HtsIndexScenarios> endpoints() {
    return HtsIndexScenarios.all().stream();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("endpoints")
  void preservesKeysAndDispatchesToTheCorrectPersistenceHandler(HtsIndexScenarios scenario)
      throws Exception {
    Answer<Object> response =
        invocation -> ApiResponse.builder().httpStatus(HttpStatus.valueOf(scenario.status)).build();
    UserTableHtsApiHandler tables = mock(UserTableHtsApiHandler.class, response);
    SoftDeletedUserTableHtsApiHandler deleted =
        mock(SoftDeletedUserTableHtsApiHandler.class, response);
    JobTableHtsApiHandler jobs = mock(JobTableHtsApiHandler.class, response);
    ToggleStatusesApiHandler toggles = mock(ToggleStatusesApiHandler.class, response);
    UserHouseTablesController tableController = new UserHouseTablesController();
    ReflectionTestUtils.setField(tableController, "tableHtsApiHandler", tables);
    ReflectionTestUtils.setField(tableController, "softDeletedTablesHtsApiHandler", deleted);
    ReflectionTestUtils.setField(
        tableController, "userTablesMapper", Mappers.getMapper(UserTablesMapper.class));
    java.lang.reflect.Field entityValidator =
        org.springframework.util.ReflectionUtils.findField(
            UserHouseTablesController.class, "htsEntityTypeValidator");
    if (entityValidator != null) {
      ReflectionTestUtils.setField(
          tableController,
          entityValidator.getName(),
          entityValidator.getType().getDeclaredConstructor().newInstance());
    }
    JobTablesController jobController = new JobTablesController();
    ReflectionTestUtils.setField(jobController, "jobTableHtsApiHandler", jobs);
    ReflectionTestUtils.setField(jobController, "jobMapper", Mappers.getMapper(JobMapper.class));
    ToggleStatusesController toggleController = new ToggleStatusesController();
    ReflectionTestUtils.setField(toggleController, "toggleStatuesApiHandler", toggles);
    MockMvcBuilders.standaloneSetup(tableController, jobController, toggleController)
        .setMessageConverters(
            new MappingJackson2HttpMessageConverter(
                new ObjectMapper().registerModule(new ParameterNamesModule())))
        .build()
        .perform(scenario.request())
        .andExpect(status().is(scenario.status));

    Object expectedHandler =
        scenario.path.contains("SoftDeleted")
                || scenario.path.endsWith("/restore")
                || scenario.path.endsWith("/purge")
            ? deleted
            : scenario.path.contains("/jobs")
                ? jobs
                : scenario.path.contains("/togglestatuses") ? toggles : tables;
    List<Invocation> calls = new ArrayList<>();
    for (Object handler : Arrays.asList(tables, deleted, jobs, toggles)) {
      if (handler != expectedHandler) {
        assertThat(mockingDetails(handler).getInvocations()).isEmpty();
      }
      calls.addAll(mockingDetails(handler).getInvocations());
    }
    assertThat(calls).hasSize(1);
    Invocation call = calls.get(0);
    String operation =
        scenario.path.endsWith("/rename")
            ? "renameEntity"
            : scenario.path.endsWith("/restore")
                ? "restoreEntity"
                : scenario.path.endsWith("/purge")
                    ? "deleteEntities"
                    : scenario.method.equals("DELETE")
                        ? "deleteEntity"
                        : scenario.method.equals("PUT")
                            ? "putEntity"
                            : scenario.path.endsWith("/query")
                                    || scenario.path.endsWith("/querySoftDeleted")
                                ? "getEntities"
                                : "getEntity";
    if (scenario.path.equals("/hts/entities")) {
      operation = "getNeutralEntity";
    } else if (scenario.path.contains("/views")) {
      operation =
          scenario.method.equals("PUT")
              ? "putView"
              : scenario.method.equals("DELETE")
                  ? "deleteView"
                  : scenario.path.endsWith("/query") ? "getViewEntities" : "getViewEntity";
    }
    assertThat(call.getMethod().getName()).isEqualTo(operation);
    Object[] arguments = call.getArguments();
    ObjectMapper json = new ObjectMapper();
    if (operation.equals("renameEntity")) {
      JsonNode from = json.valueToTree(arguments[0]);
      JsonNode to = json.valueToTree(arguments[1]);
      assertThat(from.path("databaseId").asText())
          .isEqualTo(scenario.parameters.get("fromDatabaseId"));
      assertThat(from.path("tableId").asText()).isEqualTo(scenario.parameters.get("fromTableId"));
      assertThat(to.path("databaseId").asText()).isEqualTo(scenario.parameters.get("toDatabaseId"));
      assertThat(to.path("tableId").asText()).isEqualTo(scenario.parameters.get("toTableId"));
    } else if (operation.equals("deleteEntities")) {
      assertThat(arguments[0]).isEqualTo(scenario.parameters.get("databaseId"));
      assertThat(arguments[1]).isEqualTo(scenario.parameters.get("tableId"));
      assertThat(arguments[2])
          .isEqualTo(
              scenario.parameters.containsKey("purgeAfterMs")
                  ? Long.valueOf(scenario.parameters.get("purgeAfterMs"))
                  : null);
    } else {
      JsonNode bound = json.valueToTree(arguments[0]);
      if (scenario.body != null) {
        JsonNode body = json.readTree(scenario.body).get("entity");
        for (String key : Arrays.asList("databaseId", "tableId", "jobId")) {
          if (body.has(key)) {
            assertThat(bound.get(key)).isEqualTo(body.get(key));
          }
        }
      } else {
        for (Map.Entry<String, String> parameter : scenario.parameters.entrySet()) {
          if (Arrays.asList(
                  "databaseId",
                  "tableId",
                  "jobId",
                  "featureId",
                  "deletedAtMs",
                  "purgeAfterMs",
                  "state")
              .contains(parameter.getKey())) {
            assertThat(bound.path(parameter.getKey()).asText()).isEqualTo(parameter.getValue());
          }
        }
      }
      if ((operation.equals("getEntities") || operation.equals("getViewEntities"))
          && arguments.length == 4) {
        assertThat(arguments[1])
            .isEqualTo(Integer.valueOf(scenario.parameters.getOrDefault("page", "0")));
        assertThat(arguments[2])
            .isEqualTo(Integer.valueOf(scenario.parameters.getOrDefault("size", "50")));
      }
      if (operation.equals("deleteEntity") && expectedHandler == tables) {
        assertThat(arguments[1])
            .isEqualTo(Boolean.valueOf(scenario.parameters.getOrDefault("isSoftDelete", "false")));
      }
    }
  }
}
