package com.linkedin.openhouse.housetables.e2e.database;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.DatabaseHtsJdbcRepository;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
class DatabasesControllerTest {
  @Autowired private MockMvc mockMvc;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private DatabaseHtsJdbcRepository repository;

  @AfterEach
  void cleanUp() {
    repository.deleteAll();
  }

  @Test
  void testDatabaseLifecycleAndErrorContract() throws Exception {
    String created =
        mockMvc
            .perform(
                post("/hts/databases")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        "{\"databaseName\":\"Database_Foo\",\"properties\":{\"foo\":\"one\"}}"))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.databaseName").value("Database_Foo"))
            .andExpect(jsonPath("$.properties.foo").value("one"))
            .andReturn()
            .getResponse()
            .getContentAsString();
    String databaseUuid = objectMapper.readTree(created).get("databaseUuid").asText();
    assertThat(UUID.fromString(databaseUuid).toString()).isEqualTo(databaseUuid);
    mockMvc
        .perform(get("/hts/databases/DATABASE_FOO"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.databaseUuid").value(databaseUuid));
    mockMvc
        .perform(
            post("/hts/databases")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"databaseName\":\"database_foo\"}"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.status").value("CONFLICT"));
    mockMvc
        .perform(
            post("/hts/databases/database_foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"updates\":{\"foo\":\"two\"},\"removals\":[\"foo\"]}"))
        .andExpect(status().isUnprocessableEntity());
    mockMvc
        .perform(
            post("/hts/databases/database_foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"updates\":{\"bar\":\"three\"},\"removals\":[\"foo\",\"baz\"]}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.updated[0]").value("bar"))
        .andExpect(jsonPath("$.removed[0]").value("foo"))
        .andExpect(jsonPath("$.missing[0]").value("baz"));
    mockMvc
        .perform(get("/hts/databases/database_foo"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.databaseUuid").value(databaseUuid))
        .andExpect(
            jsonPath("$.properties").value(org.hamcrest.Matchers.equalTo(Map.of("bar", "three"))));
    mockMvc.perform(delete("/hts/databases/DATABASE_FOO")).andExpect(status().isNoContent());
    mockMvc
        .perform(get("/hts/databases/database_foo"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.status").value("NOT_FOUND"));
    mockMvc.perform(delete("/hts/databases/database_foo")).andExpect(status().isNotFound());
    mockMvc
        .perform(
            post("/hts/databases/database_foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().isNotFound());
  }

  @Test
  void testOmittedMapsAndServerOwnedIdentity() throws Exception {
    mockMvc
        .perform(
            post("/hts/databases")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"databaseName\":\"foo\",\"databaseUuid\":\"bar\",\"version\":99}"))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.databaseUuid").value(org.hamcrest.Matchers.not("bar")))
        .andExpect(jsonPath("$.properties").isEmpty());
    mockMvc
        .perform(
            post("/hts/databases/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.updated").isEmpty())
        .andExpect(jsonPath("$.removed").isEmpty())
        .andExpect(jsonPath("$.missing").isEmpty());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{}",
        "{\"databaseName\":\"\"}",
        "{\"databaseName\":\"foo.bar\"}",
        "{\"databaseName\":\"foo\",\"properties\":null}",
        "{\"databaseName\":\"foo\",\"properties\":{\"bar\":null}}"
      })
  void testInvalidCreateRequestIsRejected(String request) throws Exception {
    mockMvc
        .perform(post("/hts/databases").contentType(MediaType.APPLICATION_JSON).content(request))
        .andExpect(status().isBadRequest());
    assertThat(repository.count()).isZero();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"updates\":null}",
        "{\"removals\":null}",
        "{\"updates\":{\"bar\":null}}",
        "{\"removals\":[null]}"
      })
  void testInvalidUpdateRequestIsRejected(String request) throws Exception {
    mockMvc
        .perform(
            post("/hts/databases/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content(request))
        .andExpect(status().isBadRequest());
  }

  @Test
  void testGeneratedOpenApiHasStableDatabaseContract() throws Exception {
    JsonNode document =
        objectMapper.readTree(
            mockMvc
                .perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString());
    JsonNode paths = document.get("paths");
    assertThat(paths.at("/~1hts~1databases/post/operationId").asText()).isEqualTo("createDatabase");
    assertThat(paths.at("/~1hts~1databases/post/tags/0").asText()).isEqualTo("Database");
    assertThat(paths.at("/~1hts~1databases~1{databaseName}/get/operationId").asText())
        .isEqualTo("getDatabase");
    assertThat(paths.at("/~1hts~1databases~1{databaseName}/delete/operationId").asText())
        .isEqualTo("deleteDatabase");
    assertThat(paths.at("/~1hts~1databases~1{databaseName}~1properties/post/operationId").asText())
        .isEqualTo("updateDatabaseProperties");
    assertThat(
            paths
                .at("/~1hts~1databases/post/responses/201/content/application~1json/schema/$ref")
                .asText())
        .isEqualTo("#/components/schemas/DatabaseMetadata");
    assertThat(
            paths
                .at(
                    "/~1hts~1databases~1{databaseName}~1properties/post/responses/200/content/application~1json/schema/$ref")
                .asText())
        .isEqualTo("#/components/schemas/UpdateDatabasePropertiesResponseBody");
    assertThat(
            paths
                .at("/~1hts~1databases/post/responses/409/content/application~1json/schema/$ref")
                .asText())
        .isEqualTo("#/components/schemas/ErrorResponseBody");
  }
}
