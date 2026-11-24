package com.linkedin.openhouse.internal.catalog.repository;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.housetables.client.api.NamespaceApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

class HouseNamespaceRepositoryImplTest {
  private MockWebServer server;
  private HouseNamespaceRepository repository;

  @BeforeEach
  void setUp() throws IOException {
    server = new MockWebServer();
    server.start();
    ApiClient client = new ApiClient();
    client.setBasePath(server.url("/").toString());
    repository = new HouseNamespaceRepositoryImpl(new NamespaceApi(client));
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
                "{\"namespaceUuid\":\"foo\",\"namespaceName\":\"bar\",\"properties\":{\"a\":\"1\"},\"revision\":\"0\"}"));
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
                "{\"namespaceUuid\":\"foo\",\"namespaceName\":\"bar\",\"properties\":{\"a\":\"1\"},\"revision\":\"0\"}"));
    assertEquals("1", repository.getByName("bar").getProperties().get("a"));
    assertEquals("/hts/namespaces/bar", server.takeRequest().getPath());

    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody(
                "{\"namespaceUuid\":\"foo\",\"namespaceName\":\"bar\",\"properties\":{\"b\":\"2\"},\"revision\":\"1\"}"));
    assertEquals(
        "1",
        repository
            .update("bar", new NamespaceMetadata("foo", "bar", Map.of("b", "2"), "0"))
            .getRevision());
    RecordedRequest update = server.takeRequest();
    assertEquals("PUT", update.getMethod());
    assertEquals("/hts/namespaces/bar", update.getPath());
    assertEquals(
        new ObjectMapper()
            .readTree(
                "{\"namespaceUuid\":\"foo\",\"namespaceName\":\"bar\",\"properties\":{\"b\":\"2\"},\"revision\":\"0\"}"),
        new ObjectMapper().readTree(update.getBody().readUtf8()));

    server.enqueue(new MockResponse().setResponseCode(204));
    repository.delete("bar");
    assertEquals("DELETE", server.takeRequest().getMethod());
  }

  @Test
  void notFoundStatusBecomesNamespaceAbsence() {
    server.enqueue(errorResponse(404));
    NotFoundException exception =
        assertThrows(NotFoundException.class, () -> repository.getByName("foo"));
    assertInstanceOf(WebClientResponseException.NotFound.class, exception.getCause());
    assertEquals(1, server.getRequestCount());
  }

  @Test
  void deleteNotFoundStatusBecomesNamespaceAbsence() {
    server.enqueue(errorResponse(404));
    NotFoundException exception =
        assertThrows(NotFoundException.class, () -> repository.delete("foo"));
    assertInstanceOf(WebClientResponseException.NotFound.class, exception.getCause());
    assertEquals(1, server.getRequestCount());
  }

  @Test
  void updateNotFoundStatusBecomesNamespaceAbsence() {
    server.enqueue(errorResponse(404));
    NotFoundException exception =
        assertThrows(
            NotFoundException.class,
            () -> repository.update("foo", new NamespaceMetadata("id", "foo", Map.of(), "0")));
    assertInstanceOf(WebClientResponseException.NotFound.class, exception.getCause());
    assertEquals(1, server.getRequestCount());
  }

  @Test
  void conflictStatusBecomesDuplicateCreation() {
    server.enqueue(errorResponse(409));
    DuplicateCreationException exception =
        assertThrows(DuplicateCreationException.class, () -> repository.create("foo", Map.of()));
    assertInstanceOf(WebClientResponseException.Conflict.class, exception.getCause());
    assertEquals(1, server.getRequestCount());
  }

  @Test
  void updateAndDeleteConflictsRemainCommitConflicts() {
    server.enqueue(errorResponse(409));
    EntityConcurrentModificationException update =
        assertThrows(
            EntityConcurrentModificationException.class,
            () -> repository.update("foo", new NamespaceMetadata("id", "foo", Map.of(), "0")));
    assertInstanceOf(WebClientResponseException.Conflict.class, update.getCause());
    server.enqueue(errorResponse(409));
    EntityConcurrentModificationException deletion =
        assertThrows(EntityConcurrentModificationException.class, () -> repository.delete("foo"));
    assertInstanceOf(WebClientResponseException.Conflict.class, deletion.getCause());
    assertEquals(2, server.getRequestCount());
  }

  @Test
  void otherHttpFailuresKeepTheirGeneratedTypeAndOneAttempt() {
    server.enqueue(errorResponse(503));
    assertThrows(
        WebClientResponseException.ServiceUnavailable.class,
        () -> repository.update("foo", new NamespaceMetadata("id", "foo", Map.of(), "0")));
    assertEquals(1, server.getRequestCount());
  }

  @Test
  void malformedMetadataDoesNotBecomeAConflict() {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/json")
            .setBody("{\"namespaceUuid\":\"id\",\"namespaceName\":\"foo\",\"properties\":{}}"));
    assertThrows(NullPointerException.class, () -> repository.getByName("foo"));
    assertEquals(1, server.getRequestCount());
  }

  @Test
  void transportTimeoutKeepsCauseAndOneRequest() {
    NamespaceApi namespaceApi = mock(NamespaceApi.class);
    when(namespaceApi.deleteNamespace("foo"))
        .thenReturn(Mono.error(new TimeoutException("secret")));
    HouseNamespaceRepository timedRepository = new HouseNamespaceRepositoryImpl(namespaceApi);
    assertInstanceOf(
        TimeoutException.class,
        assertThrows(RuntimeException.class, () -> timedRepository.delete("foo")).getCause());
    verify(namespaceApi).deleteNamespace("foo");
  }

  @Test
  void interruptedWaitRestoresInterruptAndReportsCancellation() {
    NamespaceApi namespaceApi = mock(NamespaceApi.class);
    AtomicBoolean requestCancelled = new AtomicBoolean();
    when(namespaceApi.deleteNamespace("foo"))
        .thenReturn(Mono.<Void>never().doOnCancel(() -> requestCancelled.set(true)));
    HouseNamespaceRepository interruptedRepository = new HouseNamespaceRepositoryImpl(namespaceApi);
    Thread.currentThread().interrupt();
    try {
      CancellationException exception =
          assertThrows(CancellationException.class, () -> interruptedRepository.delete("foo"));
      assertInstanceOf(InterruptedException.class, exception.getCause());
      assertTrue(Thread.currentThread().isInterrupted());
      assertTrue(requestCancelled.get());
    } finally {
      Thread.interrupted();
    }
    verify(namespaceApi).deleteNamespace("foo");
  }

  private static MockResponse errorResponse(int status) {
    return new MockResponse()
        .setResponseCode(status)
        .addHeader("Content-Type", "application/json")
        .setBody("{}");
  }
}
