package com.linkedin.openhouse.internal.catalog.repository;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.common.exception.checked.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.checked.NamespaceException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.housetables.client.api.NamespaceApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
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
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientResponseException;
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
    assertEquals("1", repository.getByName("bar").getProperties().get("a"));
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
    server.enqueue(errorResponse(404, "NotFoundException"));
    NotFoundException absence =
        assertThrows(NotFoundException.class, () -> repository.getByName("foo"));
    assertNotNull(absence.getCause());
    server.enqueue(new MockResponse().setResponseCode(503).setBody("secret"));
    assertEquals(
        NamespaceException.class,
        assertThrows(NamespaceException.class, () -> repository.getByName("foo")).getClass());
    assertEquals(2, server.getRequestCount());
  }

  @ParameterizedTest
  @CsvSource({
    "400,InvalidRequestException,NamespaceException",
    "404,NotFoundException,NotFoundException",
    "409,VersionConflictException,NamespaceException",
    "422,InvalidPropertyUpdateException,NamespaceException",
    "500,NamespaceException,NamespaceException",
    "500,MutationUncertainException,NamespaceException",
    "500,ServiceFailureException,NamespaceException",
    "503,UnavailableException,NamespaceException",
    "401,AuthenticationRequiredException,NamespaceException",
    "403,PermissionDeniedException,NamespaceException"
  })
  void mapsWriteFailuresWithoutRetry(int status, String wireType, String failureType) {
    server.enqueue(errorResponse(status, wireType));
    NamespaceException exception =
        assertThrows(
            NamespaceException.class,
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
        NamespaceException.class,
        assertThrows(NamespaceException.class, () -> repository.getByName("foo")).getClass());
    server.enqueue(new MockResponse().addHeader("Content-Type", "application/json").setBody("{"));
    assertEquals(
        NamespaceException.class,
        assertThrows(NamespaceException.class, () -> repository.getByName("foo")).getClass());
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
        NamespaceException.class,
        assertThrows(NamespaceException.class, () -> repository.containsEntities("foo"))
            .getClass());
  }

  @Test
  void writeTimeoutIsCheckedAndSentOnce() {
    NamespaceApi namespaceApi = mock(NamespaceApi.class);
    when(namespaceApi.deleteNamespaceWithHttpInfo("foo"))
        .thenReturn(Mono.error(new TimeoutException("secret")));
    HouseNamespaceRepository timedRepository =
        new HouseNamespaceRepositoryImpl(namespaceApi, mock(UserTableApi.class));
    assertEquals(
        NamespaceException.class,
        assertThrows(NamespaceException.class, () -> timedRepository.delete("foo")).getClass());
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
        NamespaceException.class,
        assertThrows(NamespaceException.class, () -> repository.getByName("bar")).getClass());
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"updated\":[null],\"removed\":[],\"missing\":[]}"));
    assertEquals(
        NamespaceException.class,
        assertThrows(
                NamespaceException.class,
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
        NamespaceException.class,
        assertThrows(NamespaceException.class, () -> repository.containsEntities("foo"))
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
    assertTrue(repository.getByName("bar").getProperties().isEmpty());
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
    assertThrows(NamespaceException.class, () -> repository.getByName("bar"));
  }

  @Test
  void mismatchedCreateIdentityFails() {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"namespaceUuid\":\"foo\",\"namespaceName\":\"other\",\"properties\":{}}"));
    assertThrows(NamespaceException.class, () -> repository.create("bar", Map.of()));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"updated\":null,\"removed\":[],\"missing\":[]}",
        "{\"updated\":[],\"removed\":null,\"missing\":[]}"
      })
  void nullUpdateCollectionsFail(String body) {
    server.enqueue(new MockResponse().addHeader("Content-Type", "application/json").setBody(body));
    assertThrows(
        NamespaceException.class, () -> repository.updateProperties("bar", Map.of(), Set.of()));
  }

  @Test
  void completeEmptyCollectionsRemainValid() throws NamespaceException {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"namespaceUuid\":\"foo\",\"namespaceName\":\"bar\",\"properties\":{}}"));
    assertTrue(repository.getByName("bar").getProperties().isEmpty());
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
        NamespaceException.class,
        assertThrows(NamespaceException.class, () -> repository.delete("foo")).getClass());
    assertEquals(1, server.getRequestCount());
  }

  @Test
  void conflictsRetainOperationMeaning() {
    server.enqueue(errorResponse(409, "AlreadyExistsException"));
    assertEquals(
        AlreadyExistsException.class,
        assertThrows(NamespaceException.class, () -> repository.create("foo", Map.of()))
            .getClass());
    server.enqueue(errorResponse(409, "VersionConflictException"));
    assertEquals(
        NamespaceException.class,
        assertThrows(NamespaceException.class, () -> repository.delete("namespaces")).getClass());
  }

  @Test
  void legacyVersionConflictCannotBecomeDuplicateCreation() {
    server.enqueue(errorResponse(409, "VersionConflictException"));
    assertEquals(
        NamespaceException.class,
        assertThrows(NamespaceException.class, () -> repository.create("foo", Map.of()))
            .getClass());
    assertEquals(1, server.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "<html>foo</html>",
        "{}",
        "{\"status\":\"NOT_FOUND\",\"error\":\"Not Found\",\"message\":\"foo\",\"type\":\"RESTException\"}",
        "{\"status\":\"INTERNAL_SERVER_ERROR\",\"error\":\"Not Found\",\"message\":\"foo\",\"type\":\"NotFoundException\"}"
      })
  void unverifiedAbsenceCannotBecomeRequestedAbsence(String body) {
    server.enqueue(new MockResponse().setResponseCode(404).setBody(body));
    assertThrows(NamespaceException.class, () -> repository.getByName("foo"));
    assertEquals(1, server.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "<html>foo</html>",
        "{}",
        "{\"status\":\"NOT_FOUND\",\"error\":\"Not Found\",\"message\":\"foo\",\"type\":\"RESTException\"}",
        "{\"status\":\"INTERNAL_SERVER_ERROR\",\"error\":\"Not Found\",\"message\":\"foo\",\"type\":\"NotFoundException\"}"
      })
  void unverifiedAbsenceFailsDeletion(String body) {
    server.enqueue(new MockResponse().setResponseCode(404).setBody(body));
    assertThrows(NamespaceException.class, () -> repository.delete("foo"));
    assertEquals(1, server.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 401, 403, 409, 422, 500, 503})
  void unverifiedMutationRejectionPreservesHttpFailure(int status) {
    server.enqueue(new MockResponse().setResponseCode(status).setBody("secret"));
    NamespaceException failure =
        assertThrows(NamespaceException.class, () -> repository.create("foo", Map.of()));
    assertNotNull(failure.getCause());
    assertFalse(failure.getMessage().contains("secret"));
    assertEquals(1, server.getRequestCount());
  }

  @Test
  void internallyConstructedCreationRejectionIsServiceOwned() {
    server.enqueue(errorResponse(400, "InvalidRequestException"));
    NamespaceException failure =
        assertThrows(
            NamespaceException.class, () -> repository.create("foo", Map.of("bar", "value")));
    assertInstanceOf(WebClientResponseException.BadRequest.class, failure.getCause());
  }

  @Test
  void dependencyCredentialRejectionIsServiceOwned() {
    server.enqueue(errorResponse(403, "PermissionDeniedException"));
    NamespaceException failure =
        assertThrows(NamespaceException.class, () -> repository.getByName("foo"));
    assertInstanceOf(WebClientResponseException.Forbidden.class, failure.getCause());
  }

  @Test
  void missingRequiredViewListingIsServiceFailure() {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"pageResults\":{\"content\":[]}}"));
    server.enqueue(errorResponse(404, "NotFoundException"));
    assertThrows(NamespaceException.class, () -> repository.containsEntities("foo"));
    assertEquals(2, server.getRequestCount());
  }

  private static MockResponse errorResponse(int status, String type) {
    return new MockResponse()
        .setResponseCode(status)
        .addHeader("Content-Type", "application/json")
        .setBody(
            "{\"status\":\""
                + HttpStatus.valueOf(status).name()
                + "\",\"error\":\""
                + HttpStatus.valueOf(status).getReasonPhrase()
                + "\",\"message\":\"secret\",\"type\":\""
                + type
                + "\"}");
  }
}
