package com.linkedin.openhouse.internal.catalog.repository;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.housetables.client.api.NamespaceApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceException.Reason;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;

class HouseNamespaceRepositoryImplTest {
  private MockWebServer server;
  private HouseNamespaceRepository repository;

  @BeforeEach
  void setUp() throws IOException {
    server = new MockWebServer();
    server.start();
    ApiClient client =
        new ApiClient(
            ApiClient.createDefaultObjectMapper(null)
                .registerModule(new HouseNamespaceResponseModule()),
            null);
    client.setBasePath(server.url("/").toString());
    repository =
        new HouseNamespaceRepositoryImpl(new NamespaceApi(client), new UserTableApi(client));
  }

  @AfterEach
  void tearDown() throws IOException {
    server.shutdown();
  }

  @Test
  void generatedClientPreservesCreateReadUpdateAndDeleteContracts() throws Exception {
    server.enqueue(
        new MockResponse()
            .setResponseCode(201)
            .addHeader("Content-Type", "application/json")
            .setBody(
                "{\"namespaceUuid\":\"foo\",\"namespaceName\":\"bar\",\"properties\":{\"a\":\"1\"}}"));
    assertEquals(
        "foo", repository.create("bar", Collections.singletonMap("a", "1")).getNamespaceUuid());
    RecordedRequest create = server.takeRequest();
    assertEquals("POST", create.getMethod());
    assertEquals("/hts/namespaces", create.getPath());
    assertEquals(
        new ObjectMapper().readTree("{\"namespaceName\":\"bar\",\"properties\":{\"a\":\"1\"}}"),
        new ObjectMapper().readTree(create.getBody().readUtf8()));

    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody(
                "{\"namespaceUuid\":\"foo\",\"namespaceName\":\"bar\",\"properties\":{\"a\":\"1\"}}"));
    assertEquals("1", repository.findByName("bar").get().getProperties().get("a"));
    assertEquals("/hts/namespaces/bar", server.takeRequest().getPath());

    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"updated\":[\"b\"],\"removed\":[\"a\"],\"missing\":[]}"));
    assertEquals(
        Collections.singletonList("a"),
        repository
            .updateProperties("bar", Collections.singletonMap("b", "2"), Collections.singleton("a"))
            .getRemoved());
    RecordedRequest update = server.takeRequest();
    assertEquals("/hts/namespaces/bar/properties", update.getPath());
    assertEquals(
        new ObjectMapper().readTree("{\"updates\":{\"b\":\"2\"},\"removals\":[\"a\"]}"),
        new ObjectMapper().readTree(update.getBody().readUtf8()));

    server.enqueue(new MockResponse().setResponseCode(204));
    repository.delete("bar");
    assertEquals("DELETE", server.takeRequest().getMethod());
  }

  @Test
  void onlyNotFoundReadsBecomeAbsence() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(404));
    assertFalse(repository.findByName("foo").isPresent());
    server.enqueue(new MockResponse().setResponseCode(503).setBody("secret"));
    assertEquals(
        Reason.UNAVAILABLE,
        assertThrows(HouseNamespaceException.class, () -> repository.findByName("foo"))
            .getReason());
    assertEquals(2, server.getRequestCount());
  }

  @ParameterizedTest
  @CsvSource({
    "400,BAD_REQUEST",
    "404,NOT_FOUND",
    "409,CONCURRENT_MODIFICATION",
    "422,INVALID_PROPERTIES",
    "500,UNKNOWN_OUTCOME",
    "503,UNAVAILABLE",
    "403,UNAVAILABLE"
  })
  void mapsWriteFailuresWithoutRetry(int status, Reason reason) {
    server.enqueue(new MockResponse().setResponseCode(status).setBody("secret"));
    HouseNamespaceException exception =
        assertThrows(
            HouseNamespaceException.class,
            () ->
                repository.updateProperties("foo", Collections.emptyMap(), Collections.emptySet()));
    assertEquals(reason, exception.getReason());
    assertFalse(exception.getMessage().contains("secret"));
    assertEquals(1, server.getRequestCount());
  }

  @Test
  void emptySuccessAndMalformedResponsesAreFailures() {
    server.enqueue(new MockResponse().setResponseCode(200));
    assertEquals(
        Reason.FAILURE,
        assertThrows(HouseNamespaceException.class, () -> repository.findByName("foo"))
            .getReason());
    server.enqueue(new MockResponse().addHeader("Content-Type", "application/json").setBody("{"));
    assertEquals(
        Reason.FAILURE,
        assertThrows(HouseNamespaceException.class, () -> repository.findByName("foo"))
            .getReason());
  }

  @Test
  void checksBothEntityTypesWithSizeOneAndStopsAfterOccupiedTable() throws Exception {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"pageResults\":{\"content\":[]}}"));
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody(
                "{\"pageResults\":{\"content\":[{\"tableId\":\"foo\",\"entityType\":\"VIEW\"}]}}"));
    assertTrue(repository.containsEntities("bar"));
    RecordedRequest tables = server.takeRequest();
    RecordedRequest views = server.takeRequest();
    assertEquals("1", tables.getRequestUrl().queryParameter("size"));
    assertEquals("0", tables.getRequestUrl().queryParameter("page"));
    assertEquals("bar", tables.getRequestUrl().queryParameter("databaseId"));
    assertTrue(views.getRequestUrl().encodedPath().contains("views"));
    assertEquals("1", views.getRequestUrl().queryParameter("size"));
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"pageResults\":{\"content\":[{\"tableId\":\"foo\"}]}}"));
    assertTrue(repository.containsEntities("bar"));
    assertEquals(3, server.getRequestCount());
  }

  @Test
  void emptyNamespaceRequiresTwoSuccessfulEmptyListings() throws Exception {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"pageResults\":{\"content\":[]}}"));
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"pageResults\":{\"content\":[]}}"));
    assertFalse(repository.containsEntities("foo"));
    server.enqueue(new MockResponse().setResponseCode(404));
    assertEquals(
        Reason.UNAVAILABLE,
        assertThrows(HouseNamespaceException.class, () -> repository.containsEntities("foo"))
            .getReason());
  }

  @Test
  void ambiguousWriteTimeoutIsCheckedAndSentOnce() {
    NamespaceApi namespaceApi = mock(NamespaceApi.class);
    when(namespaceApi.deleteNamespaceWithHttpInfo("foo"))
        .thenReturn(Mono.error(new TimeoutException("secret")));
    HouseNamespaceRepository timedRepository =
        new HouseNamespaceRepositoryImpl(namespaceApi, mock(UserTableApi.class));
    assertEquals(
        Reason.UNKNOWN_OUTCOME,
        assertThrows(HouseNamespaceException.class, () -> timedRepository.delete("foo"))
            .getReason());
    verify(namespaceApi).deleteNamespaceWithHttpInfo("foo");
    verifyNoMoreInteractions(namespaceApi);
  }

  @Test
  void malformedDomainValuesRemainCheckedFailures() {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody(
                "{\"namespaceUuid\":\"foo\",\"namespaceName\":\"bar\",\"properties\":{\"key\":null}}"));
    assertEquals(
        Reason.FAILURE,
        assertThrows(HouseNamespaceException.class, () -> repository.findByName("bar"))
            .getReason());
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"updated\":[null],\"removed\":[],\"missing\":[]}"));
    assertEquals(
        Reason.UNKNOWN_OUTCOME,
        assertThrows(
                HouseNamespaceException.class,
                () ->
                    repository.updateProperties(
                        "bar", Collections.emptyMap(), Collections.emptySet()))
            .getReason());
  }

  @Test
  void failedViewListingCannotEstablishEmptyNamespace() {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"pageResults\":{\"content\":[]}}"));
    server.enqueue(new MockResponse().setResponseCode(503));
    assertEquals(
        Reason.UNAVAILABLE,
        assertThrows(HouseNamespaceException.class, () -> repository.containsEntities("foo"))
            .getReason());
    assertEquals(2, server.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{}",
        "{\"updated\":[],\"removed\":[]}",
        "{\"updated\":[],\"missing\":[]}",
        "{\"removed\":[],\"missing\":[]}"
      })
  void omittedUpdateFieldsAreUnknownOutcomes(String response) {
    server.enqueue(
        new MockResponse().addHeader("Content-Type", "application/json").setBody(response));
    assertEquals(
        Reason.UNKNOWN_OUTCOME,
        assertThrows(
                HouseNamespaceException.class,
                () -> repository.updateProperties("foo", Map.of("a", "1"), Set.of("b")))
            .getReason());
    assertEquals(1, server.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"namespaceUuid\":\"foo\",\"namespaceName\":\"bar\"}",
        "{\"namespaceUuid\":\"foo\",\"properties\":{}}",
        "{\"namespaceName\":\"bar\",\"properties\":{}}"
      })
  void omittedMetadataFieldsFailReadsAndCreates(String response) {
    server.enqueue(
        new MockResponse().addHeader("Content-Type", "application/json").setBody(response));
    assertEquals(
        Reason.FAILURE,
        assertThrows(HouseNamespaceException.class, () -> repository.findByName("bar"))
            .getReason());
    server.enqueue(
        new MockResponse()
            .setResponseCode(201)
            .addHeader("Content-Type", "application/json")
            .setBody(response));
    assertEquals(
        Reason.UNKNOWN_OUTCOME,
        assertThrows(HouseNamespaceException.class, () -> repository.create("bar", Map.of()))
            .getReason());
    assertEquals(2, server.getRequestCount());
  }

  @Test
  void completeEmptyCollectionsRemainValid() throws HouseNamespaceException {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"namespaceUuid\":\"foo\",\"namespaceName\":\"bar\",\"properties\":{}}"));
    assertTrue(repository.findByName("bar").orElseThrow().getProperties().isEmpty());
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"updated\":[],\"removed\":[],\"missing\":[]}"));
    NamespacePropertyUpdate update = repository.updateProperties("bar", Map.of(), Set.of());
    assertTrue(update.getUpdated().isEmpty());
    assertTrue(update.getRemoved().isEmpty());
    assertTrue(update.getMissing().isEmpty());
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 202, 302})
  void deletionRequiresNoContentStatus(int status) {
    server.enqueue(new MockResponse().setResponseCode(status));
    assertEquals(
        Reason.UNKNOWN_OUTCOME,
        assertThrows(HouseNamespaceException.class, () -> repository.delete("foo")).getReason());
    assertEquals(1, server.getRequestCount());
  }

  @Test
  void conflictReasonsComeFromTheOperation() {
    server.enqueue(new MockResponse().setResponseCode(409));
    assertEquals(
        Reason.ALREADY_EXISTS,
        assertThrows(HouseNamespaceException.class, () -> repository.create("foo", Map.of()))
            .getReason());
    server.enqueue(new MockResponse().setResponseCode(409));
    assertEquals(
        Reason.CONCURRENT_MODIFICATION,
        assertThrows(HouseNamespaceException.class, () -> repository.delete("namespaces"))
            .getReason());
  }
}
