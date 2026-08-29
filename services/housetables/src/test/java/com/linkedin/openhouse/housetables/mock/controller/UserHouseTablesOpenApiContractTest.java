package com.linkedin.openhouse.housetables.mock.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

/**
 * The generated client is produced from the OpenAPI document springdoc actually emits, not from the
 * annotations in isolation: an advice that is not {@code @Hidden}, or a springdoc convention that
 * differs from the raw annotation, would change the contract without changing any annotation. This
 * asserts the emitted document.
 *
 * <p>{@code UserHouseTablesControllerApiContractTest} pins the same surface at the annotation
 * level; if the two ever disagree, the difference is springdoc's behaviour and is exactly what this
 * test exists to catch.
 */
@SpringBootTest
@AutoConfigureMockMvc
public class UserHouseTablesOpenApiContractTest {

  @Autowired private MockMvc mvc;

  private static final Map<String, Set<String>> EXPECTED_RESPONSE_CODES = frozenContract();

  private static Map<String, Set<String>> frozenContract() {
    Map<String, Set<String>> expected = new TreeMap<>();
    expected.put("get /hts/tables", codes("200", "404"));
    expected.put("get /hts/tables/query", codes("200"));
    expected.put("get /v1/hts/tables/query", codes("200"));
    expected.put("get /hts/tables/querySoftDeleted", codes("200", "400", "404"));
    expected.put("delete /hts/tables", codes("204", "400", "404"));
    expected.put("delete /v1/hts/tables", codes("204", "400", "404"));
    expected.put("put /hts/tables", codes("200", "201", "400", "404", "409"));
    expected.put("patch /hts/tables/rename", codes("204", "400", "404", "409"));
    expected.put("put /hts/tables/restore", codes("204", "400", "404", "409"));
    expected.put("delete /hts/tables/purge", codes("204", "400", "404"));
    expected.put("get /hts/entities", codes("200", "400", "404", "500"));
    expected.put("get /hts/views", codes("200", "400", "404", "500"));
    expected.put("get /hts/views/query", codes("200", "400", "500"));
    expected.put("get /v1/hts/views/query", codes("200", "400", "500"));
    expected.put("put /hts/views", codes("200", "201", "400", "409"));
    expected.put("delete /hts/views", codes("204", "400", "404"));
    return expected;
  }

  private static Set<String> codes(String... values) {
    return new LinkedHashSet<>(Arrays.asList(values));
  }

  private Map<String, Set<String>> generatedUserTableOperations() throws Exception {
    String document =
        mvc.perform(MockMvcRequestBuilders.get("/v3/api-docs"))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    JsonObject paths = JsonParser.parseString(document).getAsJsonObject().getAsJsonObject("paths");
    Map<String, Set<String>> operations = new TreeMap<>();
    for (String path : paths.keySet()) {
      JsonObject methods = paths.getAsJsonObject(path);
      for (String method : methods.keySet()) {
        JsonObject operation = methods.getAsJsonObject(method);
        if (!isUserTableOperation(operation)) {
          continue;
        }
        operations.put(
            method + " " + path,
            new LinkedHashSet<>(operation.getAsJsonObject("responses").keySet()));
      }
    }
    return operations;
  }

  private static boolean isUserTableOperation(JsonObject operation) {
    if (!operation.has("tags")) {
      return false;
    }
    for (com.google.gson.JsonElement tag : operation.getAsJsonArray("tags")) {
      if ("UserTable".equals(tag.getAsString())) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testGeneratedDocumentDeclaresExactlyTheFrozenResponseCodes() throws Exception {
    assertThat(generatedUserTableOperations())
        .containsExactlyInAnyOrderEntriesOf(EXPECTED_RESPONSE_CODES);
  }

  /** The six operations the generated client gains, named individually so a gap is legible. */
  @Test
  public void testTheSixNewOperationsAreGenerated() throws Exception {
    assertThat(generatedUserTableOperations().keySet())
        .contains(
            "get /hts/entities",
            "get /hts/views",
            "get /hts/views/query",
            "get /v1/hts/views/query",
            "put /hts/views",
            "delete /hts/views");
  }

  /**
   * The scoped advice is {@code @Hidden}; without that springdoc would attach its responses to
   * every operation on the advised controller and silently widen the generated client.
   */
  @Test
  public void testTheScopedAdviceContributesNoResponsesToAnyOperation() throws Exception {
    Map<String, Set<String>> generated = generatedUserTableOperations();

    Assertions.assertEquals(
        EXPECTED_RESPONSE_CODES.get("put /hts/views"), generated.get("put /hts/views"));
    Assertions.assertEquals(
        EXPECTED_RESPONSE_CODES.get("delete /hts/views"), generated.get("delete /hts/views"));
    assertThat(generated.get("put /hts/tables")).doesNotContain("500");
    assertThat(generated.get("patch /hts/tables/rename")).doesNotContain("500");
  }
}
