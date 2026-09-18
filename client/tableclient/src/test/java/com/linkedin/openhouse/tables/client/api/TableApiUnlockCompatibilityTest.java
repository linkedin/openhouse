package com.linkedin.openhouse.tables.client.api;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

class TableApiUnlockCompatibilityTest {
  private List<ClientRequest> requests;
  private TableApi api;

  @BeforeEach
  void setUp() {
    requests = new ArrayList<>();
    WebClient webClient =
        WebClient.builder()
            .exchangeFunction(
                request -> {
                  requests.add(request);
                  return Mono.just(ClientResponse.create(HttpStatus.NO_CONTENT).build());
                })
            .build();
    api = new TableApi(new ApiClient(webClient));
  }

  @Test
  void legacyTwoArgumentMethodsRemainCompatible() {
    assertNull(api.deleteLockV1("db", "table").block());
    assertEquals(
        HttpStatus.NO_CONTENT, api.deleteLockV1WithHttpInfo("db", "table").block().getStatusCode());
    assertEquals(2, requests.size());
    requests.forEach(
        request -> {
          assertEquals(HttpMethod.DELETE, request.method());
          assertEquals("/v1/databases/db/tables/table/lock", request.url().getPath());
          assertNull(request.url().getQuery());
        });
  }

  @Test
  void guardedUnlockUsesSeparateOperation() {
    assertNull(
        api.deleteLockByReasonV1("db", "table", "TIER3_AUTO_CLEANUP", "generation", "lock-owner")
            .block());
    assertEquals(1, requests.size());
    assertEquals(HttpMethod.DELETE, requests.get(0).method());
    assertEquals(
        "/v1/databases/db/tables/table/lock/TIER3_AUTO_CLEANUP", requests.get(0).url().getPath());
    assertEquals(
        "expectedTableUUID=generation&lockOwner=lock-owner", requests.get(0).url().getQuery());
  }
}
