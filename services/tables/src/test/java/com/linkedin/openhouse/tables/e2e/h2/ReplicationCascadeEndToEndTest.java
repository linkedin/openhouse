package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.model.TableModelConstants.TABLE_DTO;
import static com.linkedin.openhouse.tables.model.TableModelConstants.TEST_USER;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildCreateUpdateTableRequestBody;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.common.security.AuthenticationUtils;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor.DummySecurityJWT;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Replication;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.services.ReplicationCascadeProof;
import com.linkedin.openhouse.tables.services.TablesService;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.util.UriComponentsBuilder;

@SpringBootTest(
    classes = SpringH2Application.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
public class ReplicationCascadeEndToEndTest {

  private static final String SIGNING_KEY = "test-only-peer-secret-with-at-least-32-bytes";
  private static final List<Peer> PEERS = startPeers();
  private static final AtomicReference<ReplicationCascadeProof> PROOF = new AtomicReference<>();
  private static final Map<String, Set<String>> DENIED_USERS = new ConcurrentHashMap<>();
  private static final List<String> tableIdsToClean =
      Collections.synchronizedList(new ArrayList<>());

  @Autowired TablesService tablesService;

  @Autowired OpenHouseInternalRepository repository;

  @Autowired ReplicationCascadeProof replicationCascadeProof;

  @Autowired TestRestTemplate restTemplate;

  @MockBean AuthorizationHandler authorizationHandler;

  @LocalServerPort int port;

  private String authorization;

  @DynamicPropertySource
  static void peerProperties(DynamicPropertyRegistry registry) {
    registry.add("cluster.name", () -> "sourceA");
    registry.add(
        "cluster.security.token.interceptor.classname",
        () -> DummyTokenInterceptor.class.getName());
    for (Peer peer : PEERS) {
      registry.add(
          "cluster.replication.peers." + peer.clusterId + ".tables-api-base-uri",
          () -> peer.server.url("/").toString());
      registry.add(
          "cluster.replication.peers." + peer.clusterId + ".cascade-signing-key",
          () -> SIGNING_KEY);
    }
    registry.add(
        "cluster.replication.peers.sourceA.tables-api-base-uri", () -> "http://source-a.invalid");
    registry.add("cluster.replication.peers.sourceA.cascade-signing-key", () -> SIGNING_KEY);
  }

  @BeforeEach
  public void setup() throws Exception {
    PROOF.set(replicationCascadeProof);
    Mockito.when(
            authorizationHandler.checkAccessDecision(
                Mockito.any(), Mockito.any(DatabaseDto.class), Mockito.any()))
        .thenReturn(true);
    Mockito.when(
            authorizationHandler.checkAccessDecision(
                Mockito.any(), Mockito.any(TableDto.class), Mockito.any()))
        .thenReturn(true);
    authorization = "Bearer " + new DummySecurityJWT(TEST_USER).buildNoopJWT();
    for (Peer peer : PEERS) {
      peer.tables.clear();
      peer.requests.set(0);
      peer.failureReason.set(null);
    }
    DENIED_USERS.clear();
  }

  @AfterEach
  public void cleanup() {
    for (String tableId : tableIdsToClean) {
      repository.deleteById(
          TableDtoPrimaryKey.builder()
              .databaseId(TABLE_DTO.getDatabaseId())
              .tableId(tableId)
              .build());
    }
    tableIdsToClean.clear();
  }

  @AfterAll
  public static void stopPeers() throws IOException {
    PROOF.set(null);
    for (Peer peer : PEERS) {
      peer.server.shutdown();
    }
  }

  @Test
  public void renameAndDropFanOutToThreeDestinationsWithCallerCredential() throws Exception {
    TableDto source = createSource("clusterB", "clusterC", "clusterD");
    for (Peer peer : PEERS) {
      peer.tables.add(replicaKey(source.getTableId()));
    }
    String newTableId = source.getTableId() + "_renamed";
    ResponseEntity<Void> rename =
        request(HttpMethod.PATCH, renameUri(source.getTableId(), newTableId), authorization);

    Assertions.assertEquals(
        HttpStatus.NO_CONTENT, rename.getStatusCode(), PEERS.get(0).failureReason.get());
    replaceTrackedTableId(source.getTableId(), newTableId);
    Assertions.assertNotNull(tablesService.getTable(source.getDatabaseId(), newTableId, TEST_USER));
    for (Peer peer : PEERS) {
      Assertions.assertTrue(peer.tables.contains(replicaKey(newTableId)));
      Assertions.assertFalse(peer.tables.contains(replicaKey(source.getTableId())));
      Assertions.assertEquals(1, peer.requests.get());
      RecordedRequest outbound = peer.server.takeRequest();
      Assertions.assertEquals(authorization, outbound.getHeader(HttpHeaders.AUTHORIZATION));
      Assertions.assertNotNull(outbound.getHeader(ReplicationCascadeProof.SIGNATURE_HEADER));
    }

    ResponseEntity<Void> drop = request(HttpMethod.DELETE, tableUri(newTableId), authorization);
    Assertions.assertEquals(HttpStatus.NO_CONTENT, drop.getStatusCode());
    tableIdsToClean.remove(newTableId);
    Assertions.assertThrows(
        Exception.class,
        () -> tablesService.getTable(source.getDatabaseId(), newTableId, TEST_USER));
    for (Peer peer : PEERS) {
      Assertions.assertFalse(peer.tables.contains(replicaKey(newTableId)));
      Assertions.assertEquals(2, peer.requests.get());
      RecordedRequest outbound = peer.server.takeRequest();
      Assertions.assertEquals(HttpMethod.DELETE.name(), outbound.getMethod());
      Assertions.assertEquals(authorization, outbound.getHeader(HttpHeaders.AUTHORIZATION));
    }
  }

  @Test
  public void remotePermissionDenialLeavesSourceUntouched() throws Exception {
    TableDto source = createSource("clusterB");
    PEERS.get(0).tables.add(replicaKey(source.getTableId()));
    DENIED_USERS
        .computeIfAbsent("clusterB", ignored -> ConcurrentHashMap.newKeySet())
        .add(TEST_USER);
    String newTableId = source.getTableId() + "_denied";

    ResponseEntity<Void> response =
        request(HttpMethod.PATCH, renameUri(source.getTableId(), newTableId), authorization);

    Assertions.assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
    Assertions.assertEquals("peer denied the authenticated user", PEERS.get(0).failureReason.get());
    Assertions.assertNotNull(
        tablesService.getTable(source.getDatabaseId(), source.getTableId(), TEST_USER));
    Assertions.assertEquals(1, PEERS.get(0).requests.get());
    RecordedRequest outbound = PEERS.get(0).server.takeRequest();
    Assertions.assertEquals(authorization, outbound.getHeader(HttpHeaders.AUTHORIZATION));
    Assertions.assertNotNull(outbound.getHeader(ReplicationCascadeProof.SIGNATURE_HEADER));
  }

  @Test
  public void missingDestinationReplicaIsAnIdempotentNoOp() throws Exception {
    TableDto source = createSource("clusterB");
    String newTableId = source.getTableId() + "_missing";

    ResponseEntity<Void> response =
        request(HttpMethod.PATCH, renameUri(source.getTableId(), newTableId), authorization);

    Assertions.assertEquals(
        HttpStatus.NO_CONTENT, response.getStatusCode(), PEERS.get(0).failureReason.get());
    replaceTrackedTableId(source.getTableId(), newTableId);
    Assertions.assertNotNull(tablesService.getTable(source.getDatabaseId(), newTableId, TEST_USER));
    Assertions.assertTrue(PEERS.get(0).tables.isEmpty());
    Assertions.assertEquals(1, PEERS.get(0).requests.get());
    RecordedRequest outbound = PEERS.get(0).server.takeRequest();
    Assertions.assertEquals(authorization, outbound.getHeader(HttpHeaders.AUTHORIZATION));
    Assertions.assertNotNull(outbound.getHeader(ReplicationCascadeProof.SIGNATURE_HEADER));
  }

  @Test
  public void directReplicaRenameAndDropAreRejected() throws Exception {
    String tableId = "replica_" + UUID.randomUUID().toString().replace("-", "");
    String uuid = UUID.randomUUID().toString();
    TableDto replica =
        TABLE_DTO
            .toBuilder()
            .tableId(tableId)
            .tableType(TableType.REPLICA_TABLE)
            .tableProperties(
                ImmutableMap.of(
                    CatalogConstants.OPENHOUSE_UUID_KEY,
                    uuid,
                    CatalogConstants.OPENHOUSE_IS_TABLE_REPLICATED_KEY,
                    "true",
                    "openhouse.tableId",
                    tableId,
                    "openhouse.databaseId",
                    TABLE_DTO.getDatabaseId(),
                    "openhouse.tableLocation",
                    String.format(
                        "/tmp/%s/%s-%s/metadata.json", TABLE_DTO.getDatabaseId(), tableId, uuid)))
            .build();
    replica =
        tablesService
            .putTable(buildCreateUpdateTableRequestBody(replica), TEST_USER, true)
            .getFirst();
    tableIdsToClean.add(replica.getTableId());

    ResponseEntity<Void> rename =
        request(
            HttpMethod.PATCH,
            renameUri(replica.getTableId(), replica.getTableId() + "_renamed"),
            authorization);
    ResponseEntity<Void> drop =
        request(HttpMethod.DELETE, tableUri(replica.getTableId()), authorization);

    Assertions.assertEquals(HttpStatus.FORBIDDEN, rename.getStatusCode());
    Assertions.assertEquals(HttpStatus.FORBIDDEN, drop.getStatusCode());
    Assertions.assertNotNull(
        tablesService.getTable(replica.getDatabaseId(), replica.getTableId(), TEST_USER));
  }

  private TableDto createSource(String... destinations) {
    String tableId = "replicated_" + UUID.randomUUID().toString().replace("-", "");
    List<ReplicationConfig> configs =
        Arrays.stream(destinations)
            .map(destination -> ReplicationConfig.builder().destination(destination).build())
            .collect(java.util.stream.Collectors.toList());
    Policies policies =
        Policies.builder().replication(Replication.builder().config(configs).build()).build();
    TableDto source = TABLE_DTO.toBuilder().tableId(tableId).policies(policies).build();
    TableDto created =
        tablesService
            .putTable(buildCreateUpdateTableRequestBody(source), TEST_USER, true)
            .getFirst();
    tableIdsToClean.add(tableId);
    return created;
  }

  private static String replicaKey(String tableId) {
    return TABLE_DTO.getDatabaseId() + "/" + tableId;
  }

  private void replaceTrackedTableId(String oldTableId, String newTableId) {
    tableIdsToClean.remove(oldTableId);
    tableIdsToClean.add(newTableId);
  }

  private ResponseEntity<Void> request(HttpMethod method, URI uri, String bearer) {
    HttpHeaders headers = new HttpHeaders();
    headers.set(HttpHeaders.AUTHORIZATION, bearer);
    return restTemplate.exchange(uri, method, new HttpEntity<>(headers), Void.class);
  }

  private URI tableUri(String tableId) {
    return UriComponentsBuilder.fromHttpUrl("http://localhost:" + port)
        .pathSegment("v1", "databases", TABLE_DTO.getDatabaseId(), "tables", tableId)
        .build()
        .encode()
        .toUri();
  }

  private URI renameUri(String fromTableId, String toTableId) {
    return UriComponentsBuilder.fromHttpUrl("http://localhost:" + port)
        .pathSegment("v1", "databases", TABLE_DTO.getDatabaseId(), "tables", fromTableId, "rename")
        .queryParam("toDatabaseId", TABLE_DTO.getDatabaseId())
        .queryParam("toTableId", toTableId)
        .build()
        .encode()
        .toUri();
  }

  private static List<Peer> startPeers() {
    List<Peer> peers = new ArrayList<>();
    for (String clusterId : Arrays.asList("clusterB", "clusterC", "clusterD")) {
      Peer peer = new Peer(clusterId);
      peer.server.setDispatcher(peer.createDispatcher());
      try {
        peer.server.start();
      } catch (IOException exception) {
        throw new IllegalStateException("Unable to start replication test peer", exception);
      }
      peers.add(peer);
    }
    return peers;
  }

  private static final class Peer {
    private final String clusterId;
    private final MockWebServer server = new MockWebServer();
    private final Set<String> tables = ConcurrentHashMap.newKeySet();
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicReference<String> failureReason = new AtomicReference<>();

    private Peer(String clusterId) {
      this.clusterId = clusterId;
    }

    private Dispatcher createDispatcher() {
      return new Dispatcher() {
        @Override
        public MockResponse dispatch(RecordedRequest request) {
          requests.incrementAndGet();
          String authorization = request.getHeader(HttpHeaders.AUTHORIZATION);
          if (authorization == null) {
            return new MockResponse().setResponseCode(HttpStatus.UNAUTHORIZED.value());
          }

          String[] path = request.getPath().split("\\?");
          String[] segments = path[0].split("/");
          String databaseId = segments[3];
          String fromTableId = segments[5];
          boolean rename = HttpMethod.PATCH.name().equals(request.getMethod());
          String toDatabaseId =
              rename ? request.getRequestUrl().queryParameter("toDatabaseId") : null;
          String toTableId = rename ? request.getRequestUrl().queryParameter("toTableId") : null;

          MockHttpServletRequest peerRequest = new MockHttpServletRequest();
          peerRequest.addHeader(HttpHeaders.AUTHORIZATION, authorization);
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
            boolean authenticated =
                new DummyTokenInterceptor()
                    .preHandle(peerRequest, new MockHttpServletResponse(), null);
            if (!authenticated) {
              failureReason.set("peer authentication rejected the forwarded credential");
              return new MockResponse().setResponseCode(HttpStatus.UNAUTHORIZED.value());
            }
            String actingPrincipal = AuthenticationUtils.extractAuthenticatedUserPrincipal();
            boolean trusted =
                PROOF.get() != null
                    && PROOF
                        .get()
                        .isTrustedCascade(
                            rename ? "RENAME" : "DELETE",
                            databaseId,
                            fromTableId,
                            toDatabaseId,
                            toTableId,
                            actingPrincipal);
            if (!trusted) {
              failureReason.set("peer rejected the cascade assertion");
              return new MockResponse().setResponseCode(HttpStatus.FORBIDDEN.value());
            }
            if (DENIED_USERS
                .getOrDefault(clusterId, Collections.emptySet())
                .contains(actingPrincipal)) {
              failureReason.set("peer denied the authenticated user");
              return new MockResponse().setResponseCode(HttpStatus.FORBIDDEN.value());
            }

            String oldKey = databaseId + "/" + fromTableId;
            if (rename) {
              if (tables.remove(oldKey)) {
                tables.add(toDatabaseId + "/" + toTableId);
              }
            } else {
              tables.remove(oldKey);
            }
            return new MockResponse().setResponseCode(HttpStatus.NO_CONTENT.value());
          } catch (AccessDeniedException exception) {
            failureReason.set(exception.getMessage());
            return new MockResponse().setResponseCode(HttpStatus.FORBIDDEN.value());
          } finally {
            RequestContextHolder.resetRequestAttributes();
            SecurityContextHolder.clearContext();
          }
        }
      };
    }
  }
}
