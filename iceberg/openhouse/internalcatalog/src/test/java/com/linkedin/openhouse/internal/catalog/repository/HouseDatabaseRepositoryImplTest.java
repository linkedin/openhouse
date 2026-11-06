package com.linkedin.openhouse.internal.catalog.repository;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.housetables.client.api.DatabaseApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException.Reason;
import java.io.IOException;
import java.util.Collections;
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

class HouseDatabaseRepositoryImplTest {
  private MockWebServer server;
  private HouseDatabaseRepository repository;

  @BeforeEach
  void setUp() throws IOException {
    server = new MockWebServer();
    server.start();
    ApiClient client =
        new ApiClient(
            ApiClient.createDefaultObjectMapper(null).registerModule(new DatabaseResponseModule()),
            null);
    client.setBasePath(server.url("/").toString());
    repository = new HouseDatabaseRepositoryImpl(new DatabaseApi(client), new UserTableApi(client));
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
                "{\"databaseUuid\":\"foo\",\"databaseName\":\"bar\",\"properties\":{\"a\":\"1\"}}"));
    assertEquals(
        "foo", repository.create("bar", Collections.singletonMap("a", "1")).getDatabaseUuid());
    RecordedRequest create = server.takeRequest();
    assertEquals("POST", create.getMethod());
    assertEquals("/hts/databases", create.getPath());
    assertEquals(
        new ObjectMapper().readTree("{\"databaseName\":\"bar\",\"properties\":{\"a\":\"1\"}}"),
        new ObjectMapper().readTree(create.getBody().readUtf8()));

    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody(
                "{\"databaseUuid\":\"foo\",\"databaseName\":\"bar\",\"properties\":{\"a\":\"1\"}}"));
    assertEquals("1", repository.findByName("bar").get().getProperties().get("a"));
    assertEquals("/hts/databases/bar", server.takeRequest().getPath());

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
    assertEquals("/hts/databases/bar/properties", update.getPath());
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
        assertThrows(HouseDatabaseException.class, () -> repository.findByName("foo")).getReason());
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
    HouseDatabaseException exception =
        assertThrows(
            HouseDatabaseException.class,
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
        assertThrows(HouseDatabaseException.class, () -> repository.findByName("foo")).getReason());
    server.enqueue(new MockResponse().addHeader("Content-Type", "application/json").setBody("{"));
    assertEquals(
        Reason.FAILURE,
        assertThrows(HouseDatabaseException.class, () -> repository.findByName("foo")).getReason());
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
        assertThrows(HouseDatabaseException.class, () -> repository.containsEntities("foo"))
            .getReason());
  }

  @Test
  void ambiguousWriteTimeoutIsCheckedAndSentOnce() {
    DatabaseApi databaseApi = mock(DatabaseApi.class);
    when(databaseApi.deleteDatabaseWithHttpInfo("foo"))
        .thenReturn(Mono.error(new TimeoutException("secret")));
    HouseDatabaseRepository timedRepository =
        new HouseDatabaseRepositoryImpl(databaseApi, mock(UserTableApi.class));
    assertEquals(
        Reason.UNKNOWN_OUTCOME,
        assertThrows(HouseDatabaseException.class, () -> timedRepository.delete("foo"))
            .getReason());
    verify(databaseApi).deleteDatabaseWithHttpInfo("foo");
    verifyNoMoreInteractions(databaseApi);
  }

  @Test
  void malformedDomainValuesRemainCheckedFailures() {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody(
                "{\"databaseUuid\":\"foo\",\"databaseName\":\"bar\",\"properties\":{\"key\":null}}"));
    assertEquals(
        Reason.FAILURE,
        assertThrows(HouseDatabaseException.class, () -> repository.findByName("bar")).getReason());
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"updated\":[null],\"removed\":[],\"missing\":[]}"));
    assertEquals(
        Reason.UNKNOWN_OUTCOME,
        assertThrows(
                HouseDatabaseException.class,
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
        assertThrows(HouseDatabaseException.class, () -> repository.containsEntities("foo"))
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
                HouseDatabaseException.class,
                () ->
                    repository.updateProperties(
                        "foo", java.util.Map.of("a", "1"), java.util.Set.of("b")))
            .getReason());
    assertEquals(1, server.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"databaseUuid\":\"foo\",\"databaseName\":\"bar\"}",
        "{\"databaseUuid\":\"foo\",\"properties\":{}}",
        "{\"databaseName\":\"bar\",\"properties\":{}}"
      })
  void omittedMetadataFieldsFailReadsAndCreates(String response) {
    server.enqueue(
        new MockResponse().addHeader("Content-Type", "application/json").setBody(response));
    assertEquals(
        Reason.FAILURE,
        assertThrows(HouseDatabaseException.class, () -> repository.findByName("bar")).getReason());
    server.enqueue(
        new MockResponse()
            .setResponseCode(201)
            .addHeader("Content-Type", "application/json")
            .setBody(response));
    assertEquals(
        Reason.UNKNOWN_OUTCOME,
        assertThrows(
                HouseDatabaseException.class, () -> repository.create("bar", java.util.Map.of()))
            .getReason());
    assertEquals(2, server.getRequestCount());
  }

  @Test
  void completeEmptyCollectionsRemainValid() throws HouseDatabaseException {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"databaseUuid\":\"foo\",\"databaseName\":\"bar\",\"properties\":{}}"));
    assertTrue(repository.findByName("bar").orElseThrow().getProperties().isEmpty());
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"updated\":[],\"removed\":[],\"missing\":[]}"));
    com.linkedin.openhouse.internal.catalog.model.DatabasePropertyUpdate update =
        repository.updateProperties("bar", java.util.Map.of(), java.util.Set.of());
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
        assertThrows(HouseDatabaseException.class, () -> repository.delete("foo")).getReason());
    assertEquals(1, server.getRequestCount());
  }

  @Test
  void conflictReasonsComeFromTheOperation() {
    server.enqueue(new MockResponse().setResponseCode(409));
    assertEquals(
        Reason.ALREADY_EXISTS,
        assertThrows(
                HouseDatabaseException.class, () -> repository.create("foo", java.util.Map.of()))
            .getReason());
    server.enqueue(new MockResponse().setResponseCode(409));
    assertEquals(
        Reason.CONCURRENT_MODIFICATION,
        assertThrows(HouseDatabaseException.class, () -> repository.delete("namespaces"))
            .getReason());
  }
}
