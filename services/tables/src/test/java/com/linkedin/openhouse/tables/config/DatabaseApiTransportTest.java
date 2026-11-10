package com.linkedin.openhouse.tables.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.housetables.client.api.DatabaseApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.housetables.client.model.CreateDatabaseRequestBody;
import com.linkedin.openhouse.internal.catalog.repository.HouseDatabaseRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException.Reason;
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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

class DatabaseApiTransportTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void configuredClientDistinguishesOmittedAndEmptyCollections(boolean includeCollections)
      throws Exception {
    AtomicReference<String> response =
        new AtomicReference<>(
            "{\"databaseUuid\":\"foo\",\"databaseName\":\"foo\""
                + (includeCollections ? ",\"properties\":{}" : "")
                + "}");
    HttpServer server =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/hts/databases",
        exchange -> {
          byte[] body = response.get().getBytes(StandardCharsets.UTF_8);
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
      HouseDatabaseRepository repository =
          configuration.provideDatabaseRepository(
              configuration.provideDatabaseApiInstance(), mock(UserTableApi.class));
      if (includeCollections) {
        assertEquals(
            Collections.emptyMap(),
            repository.findByName("foo").orElseThrow(AssertionError::new).getProperties());
      } else {
        assertEquals(
            Reason.FAILURE,
            assertThrows(HouseDatabaseException.class, () -> repository.findByName("foo"))
                .getReason());
      }
      response.set(
          "{\"updated\":[],\"removed\":[]" + (includeCollections ? ",\"missing\":[]" : "") + "}");
      if (includeCollections) {
        assertEquals(
            Collections.emptyList(),
            repository
                .updateProperties("foo", Collections.emptyMap(), Collections.emptySet())
                .getMissing());
      } else {
        assertEquals(
            Reason.UNKNOWN_OUTCOME,
            assertThrows(
                    HouseDatabaseException.class,
                    () ->
                        repository.updateProperties(
                            "foo", Collections.emptyMap(), Collections.emptySet()))
                .getReason());
      }
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
                  assertTrue(reader.readLine().startsWith("POST /hts/databases "));
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
      DatabaseApi databaseApi = configuration.provideDatabaseApiInstance();
      assertThrows(
          ExecutionException.class,
          () ->
              databaseApi
                  .createDatabase(new CreateDatabaseRequestBody().databaseName("foo"))
                  .toFuture()
                  .get(10, TimeUnit.SECONDS));
      resetter.get(10, TimeUnit.SECONDS);
      assertEquals(1, received.get());
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void retryEligiblePreSendResetExercisesConfiguredTransport(boolean retryControl)
      throws Exception {
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
      DatabaseApi databaseApi =
          retryControl
              ? new DatabaseApi(configuration.provideApiInstance().getApiClient())
              : configuration.provideDatabaseApiInstance();
      assertThrows(
          ExecutionException.class,
          () ->
              databaseApi
                  .createDatabase(new CreateDatabaseRequestBody().databaseName("foo"))
                  .toFuture()
                  .get(10, TimeUnit.SECONDS));
      assertEquals(retryControl ? 2 : 1, attempts.get());
    }
  }
}
