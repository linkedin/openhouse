package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Replication;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.config.ReplicationProperties;
import com.linkedin.openhouse.tables.model.TableDto;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * HTTP-level tests for source cascade forwarding, identity preservation, and peer authorization.
 */
public class ReplicationCascadeClientTest {

  private static final String SIGNING_KEY = "test-only-replication-cascade-signing-key-32-bytes";
  private final Map<String, MockWebServer> servers = new LinkedHashMap<>();
  private ReplicationCascadePropertiesFixture fixture;
  private AtomicInteger verifiedCalls;

  @BeforeEach
  public void setup() throws IOException {
    verifiedCalls = new AtomicInteger();
    ReplicationProperties properties = new ReplicationProperties();
    properties.setCascadeMode(ReplicationProperties.CascadeMode.SERVICE);
    for (String clusterId : Arrays.asList("clusterB", "clusterC", "clusterD")) {
      MockWebServer server = new MockWebServer();
      server.setDispatcher(createDispatcher());
      server.start();
      servers.put(clusterId, server);
      ReplicationProperties.Peer peer = new ReplicationProperties.Peer();
      peer.setTablesApiBaseUri(server.url("/").toString());
      peer.setCascadeSigningKey(SIGNING_KEY);
      properties.getPeers().put(clusterId, peer);
    }
    ReplicationProperties.Peer sourcePeer = new ReplicationProperties.Peer();
    sourcePeer.setTablesApiBaseUri("http://source-a:8080");
    sourcePeer.setCascadeSigningKey(SIGNING_KEY);
    properties.getPeers().put("sourceA", sourcePeer);

    ClusterProperties clusterProperties = new ClusterProperties();
    ReflectionTestUtils.setField(clusterProperties, "clusterName", "sourceA");
    ReplicationCascadeProof proof = new ReplicationCascadeProof();
    ReflectionTestUtils.setField(proof, "replicationProperties", properties);
    ReflectionTestUtils.setField(proof, "clusterProperties", clusterProperties);

    ReplicationCascadeClient client = new ReplicationCascadeClient();
    ReflectionTestUtils.setField(client, "replicationProperties", properties);
    ReflectionTestUtils.setField(client, "clusterProperties", clusterProperties);
    ReflectionTestUtils.setField(client, "proof", proof);
    fixture = new ReplicationCascadePropertiesFixture(client, proof, properties);
    MockHttpServletRequest callerRequest = new MockHttpServletRequest();
    callerRequest.addHeader("Authorization", "Bearer user-with-rename-permission");
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(callerRequest));
  }

  @Test
  public void sparkModeLeavesCascadeToSpark() {
    fixture.properties.setCascadeMode(ReplicationProperties.CascadeMode.SPARK);
    fixture.client.cascadeRename(replicatedSource("clusterB"), "db", "renamed", "alice");

    Assertions.assertEquals(0, verifiedCalls.get());
    Assertions.assertEquals(0, servers.get("clusterB").getRequestCount());
  }

  @Test
  public void missingPeerConfigurationDisablesServiceCascade() {
    fixture.properties.getPeers().clear();

    Assertions.assertDoesNotThrow(
        () -> fixture.client.cascadeRename(replicatedSource("clusterB"), "db", "renamed", "alice"));
    Assertions.assertEquals(0, verifiedCalls.get());
  }

  @Test
  public void partiallyConfiguredPeersRejectUnknownDestination() {
    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class,
            () ->
                fixture.client.cascadeRename(
                    replicatedSource("clusterE"), "db", "renamed", "alice"));

    Assertions.assertTrue(exception.getMessage().contains("clusterE is not configured"));
  }

  @AfterEach
  public void cleanup() throws IOException {
    RequestContextHolder.resetRequestAttributes();
    for (MockWebServer server : servers.values()) {
      server.shutdown();
    }
    servers.clear();
  }

  @Test
  public void fansOutRenameToMultiplePeersWithOriginalCredentialAndTrustedAssertion()
      throws Exception {
    TableDto sourceTable = replicatedSource("clusterB", "clusterC", "clusterD");

    fixture.client.cascadeRename(sourceTable, "db", "renamed", "alice");

    Assertions.assertEquals(3, verifiedCalls.get());
    for (MockWebServer server : servers.values()) {
      RecordedRequest request = server.takeRequest();
      Assertions.assertEquals("PATCH", request.getMethod());
      Assertions.assertTrue(request.getPath().contains("toTableId=renamed"));
      Assertions.assertEquals(
          "Bearer user-with-rename-permission", request.getHeader("Authorization"));
      Assertions.assertNotNull(request.getHeader(ReplicationCascadeProof.SIGNATURE_HEADER));
    }
  }

  @Test
  public void destinationPermissionDenialStopsFanoutAndSurfacesForbidden() {
    TableDto sourceTable = replicatedSource("clusterB", "clusterC");
    MockHttpServletRequest callerRequest = new MockHttpServletRequest();
    callerRequest.addHeader("Authorization", "Bearer no-remote-permission");
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(callerRequest));

    AccessDeniedException exception =
        Assertions.assertThrows(
            AccessDeniedException.class,
            () -> fixture.client.cascadeRename(sourceTable, "db", "renamed", "alice"));

    Assertions.assertTrue(exception.getMessage().contains("clusterB"));
    Assertions.assertEquals(1, verifiedCalls.get());
    Assertions.assertEquals(0, servers.get("clusterC").getRequestCount());
  }

  @Test
  public void missingReplicaIsAnIdempotentNoOpForRenameAndDrop() {
    TableDto sourceTable = replicatedSource("clusterB");
    servers
        .get("clusterB")
        .setDispatcher(
            new Dispatcher() {
              @Override
              public MockResponse dispatch(RecordedRequest request) {
                verifiedCalls.incrementAndGet();
                // A trusted destination handles a missing replica as a successful no-op.
                return new MockResponse().setResponseCode(HttpStatus.NO_CONTENT.value());
              }
            });

    Assertions.assertDoesNotThrow(
        () -> fixture.client.cascadeRename(sourceTable, "db", "renamed", "alice"));
    Assertions.assertDoesNotThrow(() -> fixture.client.cascadeDrop(sourceTable, "alice"));
    Assertions.assertEquals(2, servers.get("clusterB").getRequestCount());
  }

  @Test
  public void peerFailureReportsPreviouslyCompletedDestinations() {
    TableDto sourceTable = replicatedSource("clusterB", "clusterC");
    servers
        .get("clusterC")
        .setDispatcher(
            new Dispatcher() {
              @Override
              public MockResponse dispatch(RecordedRequest request) {
                return new MockResponse().setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR.value());
              }
            });

    ReplicationCascadeException exception =
        Assertions.assertThrows(
            ReplicationCascadeException.class,
            () -> fixture.client.cascadeRename(sourceTable, "db", "renamed", "alice"));

    Assertions.assertTrue(exception.getMessage().contains("clusterC"));
    Assertions.assertTrue(exception.getMessage().contains("[clusterB]"));
    Assertions.assertTrue(exception.getMessage().contains("can be retried"));
  }

  @Test
  public void peerTimeoutReportsPreviouslyCompletedDestinations() {
    TableDto sourceTable = replicatedSource("clusterB", "clusterC");
    servers
        .get("clusterC")
        .setDispatcher(
            new Dispatcher() {
              @Override
              public MockResponse dispatch(RecordedRequest request) {
                return new MockResponse()
                    .setResponseCode(HttpStatus.NO_CONTENT.value())
                    .setHeadersDelay(11, TimeUnit.SECONDS);
              }
            });

    ReplicationCascadeException exception =
        Assertions.assertThrows(
            ReplicationCascadeException.class,
            () -> fixture.client.cascadeRename(sourceTable, "db", "renamed", "alice"));

    Assertions.assertTrue(exception.getMessage().contains("clusterC"));
    Assertions.assertTrue(exception.getMessage().contains("[clusterB]"));
    Assertions.assertTrue(exception.getMessage().contains("can be retried"));
  }

  private Dispatcher createDispatcher() {
    return new Dispatcher() {
      @Override
      public MockResponse dispatch(RecordedRequest request) {
        String authorization = request.getHeader("Authorization");
        if (authorization == null
            || request.getHeader(ReplicationCascadeProof.SIGNATURE_HEADER) == null) {
          return new MockResponse().setResponseCode(HttpStatus.UNAUTHORIZED.value());
        }
        if (authorization.equals("Bearer no-remote-permission")) {
          verifiedCalls.incrementAndGet();
          return new MockResponse().setResponseCode(HttpStatus.FORBIDDEN.value());
        }

        MockHttpServletRequest peerRequest = new MockHttpServletRequest();
        peerRequest.addHeader("Authorization", authorization);
        peerRequest.addHeader(
            ReplicationCascadeProof.SOURCE_HEADER,
            request.getHeader(ReplicationCascadeProof.SOURCE_HEADER));
        peerRequest.addHeader(
            ReplicationCascadeProof.TIMESTAMP_HEADER,
            request.getHeader(ReplicationCascadeProof.TIMESTAMP_HEADER));
        peerRequest.addHeader(
            ReplicationCascadeProof.TABLE_UUID_HEADER,
            request.getHeader(ReplicationCascadeProof.TABLE_UUID_HEADER));
        peerRequest.addHeader(
            ReplicationCascadeProof.SIGNATURE_HEADER,
            request.getHeader(ReplicationCascadeProof.SIGNATURE_HEADER));
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(peerRequest));
        try {
          String operation = "PATCH".equals(request.getMethod()) ? "RENAME" : "DELETE";
          String toDatabaseId =
              "PATCH".equals(request.getMethod())
                  ? request.getRequestUrl().queryParameter("toDatabaseId")
                  : null;
          String toTableId =
              "PATCH".equals(request.getMethod())
                  ? request.getRequestUrl().queryParameter("toTableId")
                  : null;
          boolean valid =
              fixture == null
                  || fixture.proof.isTrustedCascade(
                      operation, "db", "source", toDatabaseId, toTableId, "alice");
          if (!valid) {
            return new MockResponse().setResponseCode(HttpStatus.FORBIDDEN.value());
          }
          verifiedCalls.incrementAndGet();
          return new MockResponse().setResponseCode(HttpStatus.NO_CONTENT.value());
        } finally {
          RequestContextHolder.resetRequestAttributes();
        }
      }
    };
  }

  private static TableDto replicatedSource(String... destinations) {
    return TableDto.builder()
        .databaseId("db")
        .tableId("source")
        .tableUUID("source-table-uuid")
        .tableType(TableType.PRIMARY_TABLE)
        .policies(
            Policies.builder()
                .replication(
                    Replication.builder()
                        .config(
                            Arrays.stream(destinations)
                                .map(
                                    destination ->
                                        ReplicationConfig.builder()
                                            .destination(destination)
                                            .build())
                                .collect(java.util.stream.Collectors.toList()))
                        .build())
                .build())
        .build();
  }

  private static final class ReplicationCascadePropertiesFixture {
    private final ReplicationCascadeClient client;
    private final ReplicationCascadeProof proof;
    private final ReplicationProperties properties;

    private ReplicationCascadePropertiesFixture(
        ReplicationCascadeClient client,
        ReplicationCascadeProof proof,
        ReplicationProperties properties) {
      this.client = client;
      this.proof = proof;
      this.properties = properties;
    }
  }
}
