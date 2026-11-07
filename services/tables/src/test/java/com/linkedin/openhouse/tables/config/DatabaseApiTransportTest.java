package com.linkedin.openhouse.tables.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.housetables.client.api.DatabaseApi;
import com.linkedin.openhouse.housetables.client.model.CreateDatabaseRequestBody;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

class DatabaseApiTransportTest {
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
