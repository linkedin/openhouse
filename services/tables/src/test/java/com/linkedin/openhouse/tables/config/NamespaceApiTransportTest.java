package com.linkedin.openhouse.tables.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.housetables.client.api.NamespaceApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.model.CreateNamespaceRequestBody;
import com.linkedin.openhouse.internal.catalog.repository.HouseNamespaceRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceMutationUncertainException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseNamespaceResponseException;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

class NamespaceApiTransportTest {
  @Test
  void configuredReadAcceptsGeneratedEmptyProperties() throws Exception {
    withResponse(
        "{\"namespaceUuid\":\"foo\",\"namespaceName\":\"foo\"}",
        repository ->
            assertEquals(
                Collections.emptyMap(),
                repository.findByName("foo").orElseThrow().getProperties()));
  }

  @Test
  void configuredReadRejectsNumericPropertyValues() throws Exception {
    withResponse(
        "{\"namespaceUuid\":\"foo\",\"namespaceName\":\"foo\",\"properties\":{\"a\":1}}",
        repository ->
            assertThrows(
                HouseNamespaceResponseException.class, () -> repository.findByName("foo")));
  }

  @Test
  void configuredUpdateAcceptsGeneratedEmptyAcknowledgements() throws Exception {
    withResponse(
        "{\"updated\":[],\"removed\":[]}",
        repository ->
            assertEquals(
                Collections.emptyList(),
                repository
                    .updateProperties("foo", Collections.emptyMap(), Collections.emptySet())
                    .getMissing()));
  }

  @Test
  void configuredUpdateRejectsNumericAcknowledgementAsUncertain() throws Exception {
    withResponse(
        "{\"updated\":[1],\"removed\":[],\"missing\":[]}",
        repository ->
            assertThrows(
                HouseNamespaceMutationUncertainException.class,
                () ->
                    repository.updateProperties(
                        "foo", Collections.emptyMap(), Collections.emptySet())));
  }

  @FunctionalInterface
  private interface RepositoryAssertion {
    void verify(HouseNamespaceRepository repository) throws Exception;
  }

  private void withResponse(String response, RepositoryAssertion assertion) throws Exception {
    HttpServer server =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/hts/namespaces",
        exchange -> {
          byte[] body = response.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          try {
            exchange.getResponseBody().write(body);
          } finally {
            exchange.close();
          }
        });
    server.start();
    try {
      ClusterProperties properties = mock(ClusterProperties.class);
      when(properties.getClusterHouseTablesBaseUri())
          .thenReturn("http://localhost:" + server.getAddress().getPort());
      MainApplicationConfig configuration = new MainApplicationConfig();
      ReflectionTestUtils.setField(configuration, "clusterProperties", properties);
      HouseNamespaceRepository repository =
          configuration.provideNamespaceRepository(
              configuration.provideNamespaceApiInstance(), mock(UserTableApi.class));
      assertion.verify(repository);
    } finally {
      server.stop(0);
    }
  }

  @Test
  void legacyTableClientRetainsNumericStringCoercion() throws Exception {
    HttpServer server =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          byte[] body =
              "{\"pageResults\":{\"content\":[{\"tableId\":1}]}}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          try {
            exchange.getResponseBody().write(body);
          } finally {
            exchange.close();
          }
        });
    server.start();
    try {
      ClusterProperties properties = mock(ClusterProperties.class);
      when(properties.getClusterHouseTablesBaseUri())
          .thenReturn("http://localhost:" + server.getAddress().getPort());
      MainApplicationConfig configuration = new MainApplicationConfig();
      ReflectionTestUtils.setField(configuration, "clusterProperties", properties);
      assertEquals(
          "1",
          configuration
              .provideApiInstance()
              .getPaginatedUserTables(Collections.emptyMap(), 0, 1, "tableId")
              .toFuture()
              .get(10, TimeUnit.SECONDS)
              .getPageResults()
              .getContent()
              .get(0)
              .getTableId());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void resetAfterReceivingMutationSendsOneRequest() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    AtomicInteger received = new AtomicInteger();
    try (ServerSocket server = new ServerSocket(0, 10, InetAddress.getLoopbackAddress())) {
      server.setSoTimeout(10000);
      Future<?> resetter =
          executor.submit(
              () -> {
                try (Socket connection = server.accept()) {
                  connection.setSoTimeout(10000);
                  BufferedReader reader =
                      new BufferedReader(
                          new InputStreamReader(
                              connection.getInputStream(), StandardCharsets.US_ASCII));
                  assertTrue(reader.readLine().startsWith("POST /hts/namespaces "));
                  // Read the headers before resetting an established request
                  // connection.
                  reader.lines().takeWhile(line -> !line.isEmpty()).count();
                  received.incrementAndGet();
                  connection.setSoLinger(true, 0);
                }
                return null;
              });
      ClusterProperties properties = mock(ClusterProperties.class);
      when(properties.getClusterHouseTablesBaseUri())
          .thenReturn("http://127.0.0.1:" + server.getLocalPort());
      MainApplicationConfig configuration = new MainApplicationConfig();
      ReflectionTestUtils.setField(configuration, "clusterProperties", properties);
      NamespaceApi namespaceApi = configuration.provideNamespaceApiInstance();
      assertThrows(
          ExecutionException.class,
          () ->
              namespaceApi
                  .createNamespace(new CreateNamespaceRequestBody().namespaceName("foo"))
                  .toFuture()
                  .get(10, TimeUnit.SECONDS));
      resetter.get(10, TimeUnit.SECONDS);
      assertEquals(1, received.get());
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  void namespaceTransportAttemptsOnceOnEligibleReset() throws Exception {
    assertTransportAttempts(MainApplicationConfig::provideNamespaceApiInstance, 1);
  }

  @Test
  void legacyTransportPermitsOneEligibleResend() throws Exception {
    assertTransportAttempts(
        configuration -> new NamespaceApi(configuration.provideApiInstance().getApiClient()), 2);
  }

  private void assertTransportAttempts(
      Function<MainApplicationConfig, NamespaceApi> client, int expectedAttempts) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    HttpClient instrumented =
        HttpClient.create()
            .doOnRequest(
                (request, connection) -> {
                  attempts.incrementAndGet();
                  connection
                      .channel()
                      .pipeline()
                      .fireExceptionCaught(new IOException("Connection reset by peer"));
                });
    try (ServerSocket server = new ServerSocket(0, 10, InetAddress.getLoopbackAddress());
        MockedStatic<HttpClient> factory = mockStatic(HttpClient.class, CALLS_REAL_METHODS)) {
      factory.when(() -> HttpClient.create(any(ConnectionProvider.class))).thenReturn(instrumented);
      ClusterProperties properties = mock(ClusterProperties.class);
      when(properties.getClusterHouseTablesBaseUri())
          .thenReturn("http://127.0.0.1:" + server.getLocalPort());
      MainApplicationConfig configuration = new MainApplicationConfig();
      ReflectionTestUtils.setField(configuration, "clusterProperties", properties);
      NamespaceApi namespaceApi = client.apply(configuration);
      assertThrows(
          ExecutionException.class,
          () ->
              namespaceApi
                  .createNamespace(new CreateNamespaceRequestBody().namespaceName("foo"))
                  .toFuture()
                  .get(10, TimeUnit.SECONDS));
      assertEquals(expectedAttempts, attempts.get());
    }
  }
}
