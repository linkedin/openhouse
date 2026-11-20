package com.linkedin.openhouse.housetables.e2e.namespace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.e2e.HtsDatabaseTestBase;
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
class NamespacesControllerTest extends HtsDatabaseTestBase {
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
    String revision = objectMapper.readTree(created).get("revision").asText();
    assertThat(UUID.fromString(namespaceUuid).toString()).isEqualTo(namespaceUuid);
    mockMvc
        .perform(get("/hts/namespaces/NAMESPACE_FOO"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.namespaceName").value("Namespace_Foo"))
        .andExpect(jsonPath("$.namespaceUuid").value(namespaceUuid))
        .andExpect(jsonPath("$.properties.foo").value("one"));
    mockMvc
        .perform(
            put("/hts/namespaces/namespace_foo")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    "{\"namespaceUuid\":\""
                        + namespaceUuid
                        + "\",\"namespaceName\":\"wrong\",\"revision\":\""
                        + revision
                        + "\",\"properties\":{}}"))
        .andExpect(status().isBadRequest());
    mockMvc
        .perform(
            put("/hts/namespaces/namespace_foo")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().isBadRequest());
    mockMvc
        .perform(
            post("/hts/namespaces")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"namespaceName\":\"namespace_foo\"}"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.status").value("CONFLICT"));
    mockMvc
        .perform(
            put("/hts/namespaces/namespace_foo")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    "{\"namespaceUuid\":\""
                        + namespaceUuid
                        + "\",\"namespaceName\":\"Namespace_Foo\",\"revision\":\""
                        + revision
                        + "\",\"properties\":{\"bar\":\"three\"}}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.properties.bar").value("three"));
    mockMvc
        .perform(
            put("/hts/namespaces/namespace_foo")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    "{\"namespaceUuid\":\""
                        + namespaceUuid
                        + "\",\"namespaceName\":\"Namespace_Foo\",\"revision\":\""
                        + revision
                        + "\",\"properties\":{}}"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.type").value("CommitFailedException"));
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
            put("/hts/namespaces/namespace_foo")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    "{\"namespaceUuid\":\""
                        + namespaceUuid
                        + "\",\"namespaceName\":\"Namespace_Foo\",\"revision\":\""
                        + revision
                        + "\",\"properties\":{}}"))
        .andExpect(status().isNotFound());
  }

  @Test
  void testOmittedMapsAndServerOwnedIdentity() throws Exception {
    String created =
        mockMvc
            .perform(
                post("/hts/namespaces")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        "{\"namespaceName\":\"foo\",\"namespaceUuid\":\"bar\",\"version\":99}"))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.namespaceUuid").value(not("bar")))
            .andExpect(jsonPath("$.properties").exists())
            .andExpect(jsonPath("$.properties").isEmpty())
            .andReturn()
            .getResponse()
            .getContentAsString();
    mockMvc
        .perform(get("/hts/namespaces/foo"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.revision").exists())
        .andExpect(jsonPath("$.properties").exists())
        .andExpect(jsonPath("$.properties").isEmpty());
    mockMvc
        .perform(
            put("/hts/namespaces/foo").contentType(MediaType.APPLICATION_JSON).content(created))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.properties").exists())
        .andExpect(jsonPath("$.properties").isEmpty());
  }

  @Test
  void testNullReplacementValueDoesNotChangeStoredMetadata() throws Exception {
    String created =
        mockMvc
            .perform(
                post("/hts/namespaces")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content("{\"namespaceName\":\"foo\",\"properties\":{\"keep\":\"value\"}}"))
            .andExpect(status().isCreated())
            .andReturn()
            .getResponse()
            .getContentAsString();
    JsonNode original = objectMapper.readTree(created);
    ObjectNode replacement = original.deepCopy();
    ((ObjectNode) replacement.get("properties")).putNull("invalid");

    mockMvc
        .perform(
            put("/hts/namespaces/foo")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(replacement)))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.type").value("InvalidRequestException"));
    String loaded =
        mockMvc
            .perform(get("/hts/namespaces/foo"))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    assertThat(objectMapper.readTree(loaded)).isEqualTo(original);

    ((ObjectNode) replacement.get("properties")).put("invalid", "valid");
    mockMvc
        .perform(
            put("/hts/namespaces/foo")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(replacement)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.properties.keep").value("value"))
        .andExpect(jsonPath("$.properties.invalid").value("valid"));
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
    assertThat(paths.at("/~1hts~1namespaces~1{namespaceName}/put/operationId").asText())
        .isEqualTo("updateNamespace");
    assertThat(
            paths
                .at("/~1hts~1namespaces/post/responses/201/content/application~1json/schema/$ref")
                .asText())
        .isEqualTo("#/components/schemas/NamespaceMetadataResponse");
    assertThat(
            paths
                .at(
                    "/~1hts~1namespaces~1{namespaceName}/put/responses/200/content/application~1json/schema/$ref")
                .asText())
        .isEqualTo("#/components/schemas/NamespaceMetadataResponse");
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
    JsonNode current = objectMapper.readTree(loaded);
    String uuid = current.get("namespaceUuid").asText();
    String revision = current.get("revision").asText();
    mockMvc
        .perform(
            put("/hts/namespaces/foo")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    "{\"namespaceUuid\":\""
                        + uuid
                        + "\",\"namespaceName\":\"foo\",\"revision\":\""
                        + revision
                        + "\",\"properties\":{\"\\ud83d\\ude00\":\"two\"}}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.properties.😀").value("two"));
  }
}
