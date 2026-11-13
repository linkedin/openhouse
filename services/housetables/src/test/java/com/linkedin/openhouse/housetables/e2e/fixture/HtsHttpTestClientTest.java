package com.linkedin.openhouse.housetables.e2e.fixture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/** The listing and cleanup rules of {@link HtsHttpTestClient}, against synthetic responses. */
public class HtsHttpTestClientTest {

  /** Every request in order, rendered as "METHOD path?query". */
  private final List<String> requests = new ArrayList<>();

  private HtsHttpTestClient client(Function<ClientRequest, ClientResponse> server) {
    return new HtsHttpTestClient(
        WebClient.builder()
            .baseUrl("http://localhost")
            .exchangeFunction(
                request -> {
                  requests.add(
                      request.method()
                          + " "
                          + request.url().getPath()
                          + (request.url().getQuery() == null
                              ? ""
                              : "?" + request.url().getQuery()));
                  return Mono.just(server.apply(request));
                })
            .build());
  }

  private HtsHttpTestClient client(String... bodies) {
    Deque<String> queue = new ArrayDeque<>(java.util.Arrays.asList(bodies));
    return client(request -> json(HttpStatus.OK, queue.removeFirst()));
  }

  private static ClientResponse json(HttpStatus status, String body) {
    return ClientResponse.create(status)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
        .body(body)
        .build();
  }

  private static String page(int number, boolean last, String... tableIds) {
    return "{\"results\":null,\"pageResults\":{\"number\":"
        + number
        + ",\"last\":"
        + last
        + ",\"content\":["
        + java.util.Arrays.stream(tableIds)
            .map(id -> "{\"tableId\":\"" + id + "\"}")
            .collect(Collectors.joining(","))
        + "]}}";
  }

  private static List<String> ids(List<JsonNode> entities) {
    return entities.stream().map(e -> e.get("tableId").asText()).collect(Collectors.toList());
  }

  private static String viewPageRequest(int page) {
    return "GET /v1/hts/views/query?databaseId=db&page=" + page + "&size=50&sortBy=tableId";
  }

  @Test
  public void viewsUseTheVersionedRouteAndCollectEveryPage() {
    HtsHttpTestClient http = client(page(0, false, "v1"), page(1, true, "v2"));

    assertThat(ids(http.queryEntities("views", "db"))).containsExactly("v1", "v2");
    assertThat(requests).containsExactly(viewPageRequest(0), viewPageRequest(1));
  }

  @Test
  public void tablesKeepTheLegacyEnvelope() {
    HtsHttpTestClient http = client("{\"results\":[{\"tableId\":\"t\"}],\"pageResults\":null}");

    assertThat(ids(http.queryEntities("tables", "db"))).containsExactly("t");
    assertThat(requests).containsExactly("GET /hts/tables/query?databaseId=db");

    assertThatThrownBy(() -> client("{\"results\":[]}").queryEntities("tables", "db"))
        .as("an omitted pageResults is not the explicit null the legacy envelope carries")
        .isInstanceOf(AssertionError.class);
  }

  @Test
  public void anEmptyViewListingIsValid() {
    assertThat(client(page(0, true)).queryEntities("views", "db")).isEmpty();
  }

  @Test
  public void aFailedLaterPageIsNotPartialSuccess() {
    Deque<ClientResponse> responses = new ArrayDeque<>();
    responses.add(json(HttpStatus.OK, page(0, false, "v")));
    responses.add(json(HttpStatus.INTERNAL_SERVER_ERROR, "{}"));
    HtsHttpTestClient http = client(request -> responses.removeFirst());

    assertThatThrownBy(() -> http.queryEntities("views", "db")).isInstanceOf(AssertionError.class);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"results\":null,\"pageResults\":{\"number\":0,\"last\":false,\"content\":[]}}",
        "{\"results\":null,\"pageResults\":{\"number\":1,\"last\":true,\"content\":[]}}",
        "{\"results\":null,\"pageResults\":{\"number\":0,\"last\":\"true\",\"content\":[]}}",
        "{\"results\":null,\"pageResults\":{\"number\":\"0\",\"last\":true,\"content\":[]}}",
        "{\"results\":null,\"pageResults\":{\"number\":0,\"last\":true}}",
        "{\"results\":[],\"pageResults\":{\"number\":0,\"last\":true,\"content\":[]}}",
        "{\"pageResults\":{\"number\":0,\"last\":true,\"content\":[]}}"
      })
  public void aMalformedOrNonAdvancingPageFails(String body) {
    assertThatThrownBy(() -> client(body).queryEntities("views", "db"))
        .isInstanceOf(AssertionError.class);
  }

  /** Answers listings from fixed bodies and every DELETE with {@code deleteStatus}. */
  private HtsHttpTestClient cleanupServer(HttpStatus secondViewPage, HttpStatus deleteStatus) {
    return client(
        request -> {
          String path = request.url().getPath();
          if (request.method() == HttpMethod.DELETE) {
            return ClientResponse.create(deleteStatus).build();
          }
          if (path.equals("/v1/hts/views/query")) {
            return request.url().getQuery().contains("page=0")
                ? json(HttpStatus.OK, page(0, false, "v0"))
                : json(secondViewPage, page(1, true, "v1"));
          }
          if (path.equals("/hts/tables/query")) {
            return json(HttpStatus.OK, "{\"results\":[],\"pageResults\":null}");
          }
          return json(
              HttpStatus.OK, "{\"results\":null,\"pageResults\":{\"last\":true,\"content\":[]}}");
        });
  }

  @Test
  public void cleanupFetchesEveryViewPageBeforeDeleting() {
    cleanupServer(HttpStatus.OK, HttpStatus.NO_CONTENT).cleanup("db");

    assertThat(requests)
        .containsExactly(
            viewPageRequest(0),
            viewPageRequest(1),
            "DELETE /hts/views?databaseId=db&tableId=v0",
            "DELETE /hts/views?databaseId=db&tableId=v1",
            "GET /hts/tables/query?databaseId=db",
            "GET /hts/tables/querySoftDeleted?databaseId=db");
  }

  @Test
  public void cleanupDeletesNothingWhenALaterViewPageFails() {
    HtsHttpTestClient http = cleanupServer(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.NO_CONTENT);

    assertThatThrownBy(() -> http.cleanup("db")).isInstanceOf(AssertionError.class);
    assertThat(requests).noneMatch(request -> request.startsWith("DELETE"));
  }

  @Test
  public void cleanupValidatesEachDeleteResponse() {
    HtsHttpTestClient http = cleanupServer(HttpStatus.OK, HttpStatus.NOT_FOUND);

    assertThatThrownBy(() -> http.cleanup("db")).isInstanceOf(AssertionError.class);
    assertThat(requests).filteredOn(request -> request.startsWith("DELETE")).hasSize(1);
  }
}
