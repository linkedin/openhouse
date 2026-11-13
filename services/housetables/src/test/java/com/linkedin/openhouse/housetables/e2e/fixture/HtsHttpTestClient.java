package com.linkedin.openhouse.housetables.e2e.fixture;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Real-HTTP transport for the HTS integration suite. Every call returns the status and raw body
 * rather than throwing on 4xx/5xx, because the tests assert on error statuses and bodies.
 *
 * <p>The listing helpers enforce the wire envelopes: the legacy table query answers {@code results}
 * with an explicit JSON-null {@code pageResults}, and the versioned query answers the reverse. View
 * listings read every page before returning, so cleanup never deletes while paging.
 */
public final class HtsHttpTestClient {

  public static final String INITIAL_VERSION = "INITIAL_VERSION";

  public static final int PAGE_SIZE = 50;

  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  private static final ObjectMapper JSON = new ObjectMapper();

  private final WebClient web;

  public HtsHttpTestClient(WebClient web) {
    this.web = web;
  }

  public static HtsHttpTestClient forPort(int port) {
    return new HtsHttpTestClient(WebClient.builder().baseUrl("http://localhost:" + port).build());
  }

  /** One exchange: what was requested, the status, and the body exactly as received. */
  public static final class Response {
    private final String request;
    private final int status;
    private final String body;

    Response(String request, int status, String body) {
      this.request = request;
      this.status = status;
      this.body = body;
    }

    public int status() {
      return status;
    }

    public String body() {
      return body;
    }

    public JsonNode json() {
      try {
        JsonNode node = JSON.readTree(body);
        if (node == null || node.isMissingNode()) {
          throw new AssertionError("Expected a JSON body. " + describe());
        }
        return node;
      } catch (JsonProcessingException e) {
        throw new AssertionError("Expected a JSON body. " + describe(), e);
      }
    }

    public JsonNode entity() {
      JsonNode entity = json().get("entity");
      if (entity == null || !entity.isObject()) {
        throw new AssertionError("Expected an entity object. " + describe());
      }
      return entity;
    }

    public String message() {
      return text(json(), "message");
    }

    public String describe() {
      return request + " -> " + status + " " + body;
    }
  }

  /** The field's text, or null when it is absent or JSON null. */
  public static String text(JsonNode node, String field) {
    JsonNode value = node.get(field);
    return value == null || value.isNull() ? null : value.asText();
  }

  public static Set<String> tableIds(Collection<JsonNode> entities) {
    Set<String> ids = new LinkedHashSet<>();
    for (JsonNode entity : entities) {
      ids.add(text(entity, "tableId"));
    }
    return ids;
  }

  public static void expectStatus(Response response, int expected, String what) {
    assertThat(response.status())
        .as("%s: expected HTTP %d, got %s", what, expected, response.describe())
        .isEqualTo(expected);
  }

  public static void expectMessage(Response response, String expected, String what) {
    assertThat(response.message())
        .as("%s: expected message %s. %s", what, expected, response.describe())
        .isEqualTo(expected);
  }

  public static Map<String, String> params(String... namesAndValues) {
    Map<String, String> params = new LinkedHashMap<>();
    for (int i = 0; i < namesAndValues.length; i += 2) {
      params.put(namesAndValues[i], namesAndValues[i + 1]);
    }
    return params;
  }

  /** Query values are expanded as URI variables, so they are always strictly encoded. */
  public Response exchange(
      HttpMethod method, String path, Map<String, String> params, Object jsonBody) {
    Map<String, String> variables = new HashMap<>();
    StringBuilder described = new StringBuilder(method.name()).append(' ').append(path);
    String separator = "?";
    for (Map.Entry<String, String> param : params.entrySet()) {
      described.append(separator).append(param.getKey()).append('=').append(param.getValue());
      separator = "&";
    }
    WebClient.RequestBodySpec request =
        web.method(method)
            .uri(
                builder -> {
                  builder.path(path);
                  int index = 0;
                  for (Map.Entry<String, String> param : params.entrySet()) {
                    String variable = "p" + index++;
                    builder.queryParam(param.getKey(), "{" + variable + "}");
                    variables.put(variable, param.getValue());
                  }
                  return builder.build(variables);
                });
    WebClient.RequestHeadersSpec<?> ready =
        jsonBody == null
            ? request
            : request.contentType(MediaType.APPLICATION_JSON).bodyValue(jsonBody);
    Response response =
        ready
            .exchangeToMono(
                result ->
                    result
                        .bodyToMono(String.class)
                        .defaultIfEmpty("")
                        .map(
                            body ->
                                new Response(described.toString(), result.rawStatusCode(), body)))
            .block(TIMEOUT);
    if (response == null) {
      throw new AssertionError("No response for " + described);
    }
    return response;
  }

  public Response get(String path, Map<String, String> params) {
    return exchange(HttpMethod.GET, path, params, null);
  }

  /** PUT /hts/{kind}. {@code tableVersion} is the compare-and-swap token, not a counter. */
  public Response putEntity(
      String kind,
      String databaseId,
      String tableId,
      String tableVersion,
      String metadataLocation,
      String entityType) {
    Map<String, Object> entity = new LinkedHashMap<>();
    entity.put("databaseId", databaseId);
    entity.put("tableId", tableId);
    entity.put("tableVersion", tableVersion);
    entity.put(
        "metadataLocation",
        metadataLocation != null ? metadataLocation : defaultLocation(databaseId, tableId));
    entity.put("storageType", "hdfs");
    if (entityType != null) {
      entity.put("entityType", entityType);
    }
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("entity", entity);
    return exchange(HttpMethod.PUT, "/hts/" + kind, params(), body);
  }

  public Response putEntity(String kind, String databaseId, String tableId) {
    return putEntity(kind, databaseId, tableId, INITIAL_VERSION, null, null);
  }

  public static String defaultLocation(String databaseId, String tableId) {
    return "/tmp/" + databaseId + "/" + tableId;
  }

  public JsonNode createTable(String databaseId, String tableId) {
    return create("tables", databaseId, tableId);
  }

  public JsonNode createView(String databaseId, String tableId) {
    return create("views", databaseId, tableId);
  }

  private JsonNode create(String kind, String databaseId, String tableId) {
    Response response = putEntity(kind, databaseId, tableId);
    expectStatus(response, 201, "seeding " + kind + " " + databaseId + "." + tableId);
    return response.entity();
  }

  /** GET /hts/tables, /hts/views or /hts/entities for one key. */
  public Response getEntity(String kind, String databaseId, String tableId) {
    return get("/hts/" + kind, params("databaseId", databaseId, "tableId", tableId));
  }

  public Response deleteEntity(String kind, String databaseId, String tableId) {
    return exchange(
        HttpMethod.DELETE,
        "/hts/" + kind,
        params("databaseId", databaseId, "tableId", tableId),
        null);
  }

  public Response softDeleteTable(String databaseId, String tableId) {
    return exchange(
        HttpMethod.DELETE,
        "/v1/hts/tables",
        params("databaseId", databaseId, "tableId", tableId, "isSoftDelete", "true"),
        null);
  }

  public Response restoreTable(String databaseId, String tableId, long deletedAtMs) {
    return exchange(
        HttpMethod.PUT,
        "/hts/tables/restore",
        params(
            "databaseId",
            databaseId,
            "tableId",
            tableId,
            "deletedAtMs",
            Long.toString(deletedAtMs)),
        null);
  }

  public Response purgeTable(String databaseId, String tableId) {
    return exchange(
        HttpMethod.DELETE,
        "/hts/tables/purge",
        params("databaseId", databaseId, "tableId", tableId),
        null);
  }

  public Response renameTable(
      String databaseId, String fromTableId, String toTableId, String metadataLocation) {
    return exchange(
        HttpMethod.PATCH,
        "/hts/tables/rename",
        params(
            "fromDatabaseId",
            databaseId,
            "fromTableId",
            fromTableId,
            "toDatabaseId",
            databaseId,
            "toTableId",
            toTableId,
            "metadataLocation",
            metadataLocation != null ? metadataLocation : defaultLocation(databaseId, toTableId)),
        null);
  }

  /** Tables through the legacy route, views through every page of their versioned route. */
  public List<JsonNode> queryEntities(String kind, String databaseId) {
    if ("views".equals(kind)) {
      return queryEntitiesPaginated(kind, databaseId);
    }
    Response response = get("/hts/" + kind + "/query", params("databaseId", databaseId));
    expectStatus(response, 200, "querying " + kind + " in " + databaseId);
    JsonNode body = response.json();
    assertThat(body.has("pageResults") && body.get("pageResults").isNull())
        .as("the legacy query must carry an explicit null pageResults. %s", response.describe())
        .isTrue();
    return elements(body.get("results"), "results", response);
  }

  /** Reads every page before returning, so a caller never mutates rows mid-listing. */
  public List<JsonNode> queryEntitiesPaginated(String kind, String databaseId) {
    List<JsonNode> entities = new ArrayList<>();
    for (int page = 0; ; page++) {
      Response response =
          get(
              "/v1/hts/" + kind + "/query",
              params(
                  "databaseId",
                  databaseId,
                  "page",
                  Integer.toString(page),
                  "size",
                  Integer.toString(PAGE_SIZE),
                  "sortBy",
                  "tableId"));
      expectStatus(response, 200, "paginated query of " + kind + " in " + databaseId);
      JsonNode body = response.json();
      assertThat(body.has("results") && body.get("results").isNull())
          .as("a paginated query must carry an explicit null results. %s", response.describe())
          .isTrue();
      JsonNode result = body.get("pageResults");
      assertThat(result != null && result.isObject())
          .as("missing pageResults. %s", response.describe())
          .isTrue();
      assertThat(result.path("number").isInt() && result.get("number").asInt() == page)
          .as("expected page number %d. %s", page, response.describe())
          .isTrue();
      assertThat(result.path("last").isBoolean())
          .as("missing Boolean last-page flag. %s", response.describe())
          .isTrue();
      List<JsonNode> content = elements(result.get("content"), "pageResults.content", response);
      entities.addAll(content);
      if (result.get("last").asBoolean()) {
        return entities;
      }
      assertThat(content).as("empty non-final page. %s", response.describe()).isNotEmpty();
    }
  }

  /**
   * A single page of archived rows. Fails rather than truncating when there is more than one page:
   * this is not a bulk archive reader.
   */
  public List<JsonNode> querySoftDeleted(String databaseId, String tableId) {
    Map<String, String> params = params("databaseId", databaseId);
    if (tableId != null) {
      params.put("tableId", tableId);
    }
    Response response = get("/hts/tables/querySoftDeleted", params);
    expectStatus(response, 200, "querying soft deleted tables in " + databaseId);
    JsonNode result = response.json().get("pageResults");
    assertThat(result != null && result.path("last").asBoolean(false))
        .as("expected a single, final page of soft deleted rows. %s", response.describe())
        .isTrue();
    return elements(result.get("content"), "pageResults.content", response);
  }

  /**
   * Deletes a database's live views and tables and purges its archives over HTTP, validating every
   * delete. All view pages are read before the first delete, so a failed later page deletes
   * nothing.
   */
  public void cleanup(String databaseId) {
    for (JsonNode view : queryEntities("views", databaseId)) {
      String tableId = text(view, "tableId");
      expectStatus(deleteEntity("views", databaseId, tableId), 204, "cleanup view " + tableId);
    }
    for (JsonNode table : queryEntities("tables", databaseId)) {
      String tableId = text(table, "tableId");
      expectStatus(deleteEntity("tables", databaseId, tableId), 204, "cleanup table " + tableId);
    }
    for (String tableId : tableIds(querySoftDeleted(databaseId, null))) {
      expectStatus(purgeTable(databaseId, tableId), 204, "cleanup archive " + tableId);
    }
  }

  private static List<JsonNode> elements(JsonNode array, String field, Response response) {
    assertThat(array != null && array.isArray())
        .as("expected %s to be an array. %s", field, response.describe())
        .isTrue();
    List<JsonNode> elements = new ArrayList<>();
    array.forEach(elements::add);
    return elements;
  }
}
