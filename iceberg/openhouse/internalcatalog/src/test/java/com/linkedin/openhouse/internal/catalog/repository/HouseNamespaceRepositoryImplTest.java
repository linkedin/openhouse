package com.linkedin.openhouse.internal.catalog.repository;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.housetables.client.api.NamespaceApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceAlreadyExistsException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceConcurrentModificationException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceMutationUncertainException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceResponseException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceUnavailableException;
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
        HouseNamespaceUnavailableException.class,
        assertThrows(HouseNamespaceException.class, () -> repository.findByName("foo")).getClass());
    assertEquals(2, server.getRequestCount());
  }

  @ParameterizedTest
  @CsvSource({
    "400,HouseNamespaceInvalidRequestException",
    "404,HouseNamespaceNotFoundException",
    "409,HouseNamespaceConcurrentModificationException",
    "422,HouseNamespaceInvalidPropertiesException",
    "500,HouseNamespaceMutationUncertainException",
    "503,HouseNamespaceUnavailableException",
    "403,HouseNamespaceUnavailableException"
  })
  void mapsWriteFailuresWithoutRetry(int status, String failureType) {
    server.enqueue(new MockResponse().setResponseCode(status).setBody("secret"));
    HouseNamespaceException exception =
        assertThrows(
            HouseNamespaceException.class,
            () ->
                repository.updateProperties("foo", Collections.emptyMap(), Collections.emptySet()));
    assertEquals(failureType, exception.getClass().getSimpleName());
    assertFalse(exception.getMessage().contains("secret"));
    assertEquals(1, server.getRequestCount());
  }

  @Test
  void emptySuccessAndMalformedResponsesAreFailures() {
    server.enqueue(new MockResponse().setResponseCode(200));
    assertEquals(
        HouseNamespaceResponseException.class,
        assertThrows(HouseNamespaceException.class, () -> repository.findByName("foo")).getClass());
    server.enqueue(new MockResponse().addHeader("Content-Type", "application/json").setBody("{"));
    assertEquals(
        HouseNamespaceResponseException.class,
        assertThrows(HouseNamespaceException.class, () -> repository.findByName("foo")).getClass());
  }

  @Test
  void viewOccupancyUsesSizeOneListings() throws Exception {
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
    assertEquals(2, server.getRequestCount());
  }

  @Test
  void tableOccupancyNeedsOneRequest() throws Exception {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"pageResults\":{\"content\":[{\"tableId\":\"foo\"}]}}"));
    assertTrue(repository.containsEntities("bar"));
    assertEquals(1, server.getRequestCount());
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
    assertEquals(2, server.getRequestCount());
  }

  @Test
  void missingListingCannotEstablishEmptiness() {
    server.enqueue(new MockResponse().setResponseCode(404));
    assertEquals(
        HouseNamespaceUnavailableException.class,
        assertThrows(HouseNamespaceException.class, () -> repository.containsEntities("foo"))
            .getClass());
  }

  @Test
  void ambiguousWriteTimeoutIsCheckedAndSentOnce() {
    NamespaceApi namespaceApi = mock(NamespaceApi.class);
    when(namespaceApi.deleteNamespaceWithHttpInfo("foo"))
        .thenReturn(Mono.error(new TimeoutException("secret")));
    HouseNamespaceRepository timedRepository =
        new HouseNamespaceRepositoryImpl(namespaceApi, mock(UserTableApi.class));
    assertEquals(
        HouseNamespaceMutationUncertainException.class,
        assertThrows(HouseNamespaceException.class, () -> timedRepository.delete("foo"))
            .getClass());
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
        HouseNamespaceResponseException.class,
        assertThrows(HouseNamespaceException.class, () -> repository.findByName("bar")).getClass());
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"updated\":[null],\"removed\":[],\"missing\":[]}"));
    assertEquals(
        HouseNamespaceMutationUncertainException.class,
        assertThrows(
                HouseNamespaceException.class,
                () ->
                    repository.updateProperties(
                        "bar", Collections.emptyMap(), Collections.emptySet()))
            .getClass());
  }

  @Test
  void failedViewListingCannotEstablishEmptyNamespace() {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"pageResults\":{\"content\":[]}}"));
    server.enqueue(new MockResponse().setResponseCode(503));
    assertEquals(
        HouseNamespaceUnavailableException.class,
        assertThrows(HouseNamespaceException.class, () -> repository.containsEntities("foo"))
            .getClass());
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
  void omittedUpdateCollectionsUseGeneratedDefaults(String response) throws Exception {
    server.enqueue(
        new MockResponse().addHeader("Content-Type", "application/json").setBody(response));
    NamespacePropertyUpdate update = repository.updateProperties("foo", Map.of(), Set.of());
    assertTrue(update.getUpdated().isEmpty());
    assertTrue(update.getRemoved().isEmpty());
    assertTrue(update.getMissing().isEmpty());
    assertEquals(1, server.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"namespaceUuid\":\"foo\",\"namespaceName\":\"bar\"}",
        "{\"namespaceUuid\":\"foo\",\"namespaceName\":\"bar\",\"properties\":{}}"
      })
  void generatedMetadataCollectionsAcceptOmittedProperties(String response) throws Exception {
    server.enqueue(
        new MockResponse().addHeader("Content-Type", "application/json").setBody(response));
    assertTrue(repository.findByName("bar").orElseThrow().getProperties().isEmpty());
    server.enqueue(
        new MockResponse()
            .setResponseCode(201)
            .addHeader("Content-Type", "application/json")
            .setBody(response));
    assertTrue(repository.create("bar", Map.of()).getProperties().isEmpty());
    assertEquals(2, server.getRequestCount());
  }

  @Test
  void mismatchedReadIdentityFails() {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"namespaceUuid\":\"foo\",\"namespaceName\":\"other\",\"properties\":{}}"));
    assertThrows(HouseNamespaceResponseException.class, () -> repository.findByName("bar"));
  }

  @Test
  void mismatchedCreateIdentityLeavesMutationUncertain() {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"namespaceUuid\":\"foo\",\"namespaceName\":\"other\",\"properties\":{}}"));
    assertThrows(
        HouseNamespaceMutationUncertainException.class, () -> repository.create("bar", Map.of()));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"updated\":null,\"removed\":[],\"missing\":[]}",
        "{\"updated\":[],\"removed\":null,\"missing\":[]}"
      })
  void nullUpdateCollectionsLeaveMutationUncertain(String body) {
    server.enqueue(new MockResponse().addHeader("Content-Type", "application/json").setBody(body));
    assertThrows(
        HouseNamespaceMutationUncertainException.class,
        () -> repository.updateProperties("bar", Map.of(), Set.of()));
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
        HouseNamespaceMutationUncertainException.class,
        assertThrows(HouseNamespaceException.class, () -> repository.delete("foo")).getClass());
    assertEquals(1, server.getRequestCount());
  }

  @Test
  void conflictsRetainOperationMeaning() {
    server.enqueue(new MockResponse().setResponseCode(409));
    assertEquals(
        HouseNamespaceAlreadyExistsException.class,
        assertThrows(HouseNamespaceException.class, () -> repository.create("foo", Map.of()))
            .getClass());
    server.enqueue(new MockResponse().setResponseCode(409));
    assertEquals(
        HouseNamespaceConcurrentModificationException.class,
        assertThrows(HouseNamespaceException.class, () -> repository.delete("namespaces"))
            .getClass());
  }
}
