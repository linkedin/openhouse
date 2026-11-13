package com.linkedin.openhouse.housetables.e2e.namespace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.NamespaceHtsJdbcRepository;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
class NamespacesControllerTest {
  @Autowired private MockMvc mockMvc;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private NamespaceHtsJdbcRepository repository;

  @AfterEach
  void cleanUp() {
    repository.deleteAll();
  }

  @Test
  void testNamespaceLifecycleAndErrorContract() throws Exception {
    String created =
        mockMvc
            .perform(
                post("/hts/namespaces")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        "{\"namespaceName\":\"Namespace_Foo\",\"properties\":{\"foo\":\"one\"}}"))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.namespaceName").value("Namespace_Foo"))
            .andExpect(jsonPath("$.databaseName").doesNotExist())
            .andExpect(jsonPath("$.databaseUuid").doesNotExist())
            .andExpect(jsonPath("$.properties.foo").value("one"))
            .andReturn()
            .getResponse()
            .getContentAsString();
    String namespaceUuid = objectMapper.readTree(created).get("namespaceUuid").asText();
    assertThat(UUID.fromString(namespaceUuid).toString()).isEqualTo(namespaceUuid);
    mockMvc
        .perform(get("/hts/namespaces/NAMESPACE_FOO"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.namespaceName").value("Namespace_Foo"))
        .andExpect(jsonPath("$.namespaceUuid").value(namespaceUuid));
    mockMvc
        .perform(
            post("/hts/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespaceName\":\"namespace_foo\"}"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.status").value("CONFLICT"));
    mockMvc
        .perform(
            post("/hts/namespaces/namespace_foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"updates\":{\"bar\":\"three\"},\"removals\":[\"foo\",\"baz\"]}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.updated[0]").value("bar"))
        .andExpect(jsonPath("$.removed[0]").value("foo"))
        .andExpect(jsonPath("$.missing[0]").value("baz"));
    mockMvc
        .perform(get("/hts/namespaces/namespace_foo"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.namespaceUuid").value(namespaceUuid))
        .andExpect(jsonPath("$.properties").value(equalTo(Map.of("bar", "three"))));
    mockMvc.perform(delete("/hts/namespaces/NAMESPACE_FOO")).andExpect(status().isNoContent());
    mockMvc
        .perform(get("/hts/namespaces/namespace_foo"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.status").value("NOT_FOUND"));
    mockMvc.perform(delete("/hts/namespaces/namespace_foo")).andExpect(status().isNotFound());
    mockMvc
        .perform(
            post("/hts/namespaces/namespace_foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().isNotFound());
  }

  @Test
  void testOmittedMapsAndServerOwnedIdentity() throws Exception {
    mockMvc
        .perform(
            post("/hts/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespaceName\":\"foo\",\"namespaceUuid\":\"bar\",\"version\":99}"))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.namespaceUuid").value(not("bar")))
        .andExpect(jsonPath("$.properties").isEmpty());
    mockMvc
        .perform(
            post("/hts/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.updated").isEmpty())
        .andExpect(jsonPath("$.removed").isEmpty())
        .andExpect(jsonPath("$.missing").isEmpty());
  }

  @Test
  void testGeneratedOpenApiHasStableNamespaceContract() throws Exception {
    JsonNode document =
        objectMapper.readTree(
            mockMvc
                .perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString());
    JsonNode paths = document.get("paths");
    assertThat(paths.has("/hts/databases")).isFalse();
    assertThat(paths.at("/~1hts~1namespaces/post/operationId").asText())
        .isEqualTo("createNamespace");
    assertThat(paths.at("/~1hts~1namespaces/post/tags/0").asText()).isEqualTo("Namespace");
    assertThat(paths.at("/~1hts~1namespaces~1{namespaceName}/get/operationId").asText())
        .isEqualTo("getNamespace");
    assertThat(paths.at("/~1hts~1namespaces~1{namespaceName}/delete/operationId").asText())
        .isEqualTo("deleteNamespace");
    assertThat(
            paths.at("/~1hts~1namespaces~1{namespaceName}~1properties/post/operationId").asText())
        .isEqualTo("updateNamespaceProperties");
    assertThat(
            paths
                .at("/~1hts~1namespaces/post/responses/201/content/application~1json/schema/$ref")
                .asText())
        .isEqualTo("#/components/schemas/NamespaceMetadataResponse");
    assertThat(
            paths
                .at(
                    "/~1hts~1namespaces~1{namespaceName}~1properties/post/responses/200/content/application~1json/schema/$ref")
                .asText())
        .isEqualTo("#/components/schemas/UpdateNamespacePropertiesResponseBody");
    assertThat(
            paths
                .at("/~1hts~1namespaces/post/responses/409/content/application~1json/schema/$ref")
                .asText())
        .isEqualTo("#/components/schemas/NamespaceErrorResponse");
    assertThat(
            document.at("/components/schemas/NamespaceErrorResponse/properties/type/type").asText())
        .isEqualTo("string");
  }

  @Test
  void testSupplementaryUnicodeKeysRoundTrip() throws Exception {
    mockMvc
        .perform(
            post("/hts/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespaceName\":\"foo\",\"properties\":{\"\\ud83d\\ude00\":\"one\"}}"))
        .andExpect(status().isCreated());
    String loaded =
        mockMvc
            .perform(get("/hts/namespaces/foo"))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    assertThat(objectMapper.readTree(loaded).get("properties").get("😀").asText()).isEqualTo("one");
    mockMvc
        .perform(
            post("/hts/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"updates\":{\"\\ud83d\\ude00\":\"two\"}}"))
        .andExpect(status().isOk());
    mockMvc
        .perform(
            post("/hts/namespaces/foo/properties")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"removals\":[\"\\ud83d\\ude00\"]}"))
        .andExpect(status().isOk());
  }
}
