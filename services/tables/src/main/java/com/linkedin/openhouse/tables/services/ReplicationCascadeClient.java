package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import com.linkedin.openhouse.tables.config.ReplicationProperties;
import com.linkedin.openhouse.tables.model.TableDto;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

/** Sends source-authorized DDL to each configured Tables API peer before committing local DDL. */
@Component
public class ReplicationCascadeClient {

  private static final String DELETE = "DELETE";
  private static final String RENAME = "RENAME";

  @Autowired private ReplicationProperties replicationProperties;

  @Autowired private ClusterProperties clusterProperties;

  @Autowired private ReplicationCascadeProof proof;

  private final HttpClient httpClient =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

  public List<String> cascadeDrop(TableDto table, String actingPrincipal) {
    return cascade(table, actingPrincipal, DELETE, null, null);
  }

  public List<String> cascadeRename(
      TableDto table, String toDatabaseId, String toTableId, String actingPrincipal) {
    return cascade(table, actingPrincipal, RENAME, toDatabaseId, toTableId);
  }

  private List<String> cascade(
      TableDto table,
      String actingPrincipal,
      String operation,
      String toDatabaseId,
      String toTableId) {
    if (replicationProperties.getCascadeMode() != ReplicationProperties.CascadeMode.SERVICE
        || replicationProperties.getPeers().isEmpty()) {
      return Collections.emptyList();
    }
    Set<String> destinations = replicationDestinations(table);
    if (destinations.isEmpty()) {
      return Collections.emptyList();
    }

    List<String> completed = new ArrayList<>();
    for (String destination : destinations) {
      ReplicationProperties.Peer peer = replicationProperties.getPeer(destination);
      if (peer == null) {
        throw new IllegalStateException(
            String.format(
                "Replication destination %s is not configured on cluster %s",
                destination, clusterProperties.getClusterName()));
      }
      try {
        send(peer, table, operation, toDatabaseId, toTableId, actingPrincipal);
        completed.add(destination);
      } catch (ReplicationCascadeException exception) {
        throw cascadeFailure(operation, destination, completed, exception);
      } catch (AccessDeniedException exception) {
        throw new AccessDeniedException(
            String.format(
                "%s was denied by replication destination %s after successful destinations %s; "
                    + "local table was not changed",
                operation, destination, completed),
            exception);
      }
    }
    return Collections.unmodifiableList(completed);
  }

  private void send(
      ReplicationProperties.Peer peer,
      TableDto table,
      String operation,
      String toDatabaseId,
      String toTableId,
      String actingPrincipal) {
    ReplicationCascadeProof.SignedHeaders signed =
        proof.sign(
            peer,
            operation,
            table.getDatabaseId(),
            table.getTableId(),
            toDatabaseId,
            toTableId,
            table.getTableUUID(),
            actingPrincipal);
    HttpHeaders headers = new HttpHeaders();
    headers.set(HttpHeaders.AUTHORIZATION, signed.getAuthorization());
    headers.set(ReplicationCascadeProof.SOURCE_HEADER, signed.getSourceCluster());
    headers.set(ReplicationCascadeProof.TIMESTAMP_HEADER, signed.getTimestamp());
    headers.set(ReplicationCascadeProof.TABLE_UUID_HEADER, signed.getTableUUID());
    headers.set(ReplicationCascadeProof.SIGNATURE_HEADER, signed.getSignature());

    URI uri =
        RENAME.equals(operation)
            ? UriComponentsBuilder.fromHttpUrl(peer.getTablesApiBaseUri())
                .pathSegment(
                    "v1",
                    "databases",
                    table.getDatabaseId(),
                    "tables",
                    table.getTableId(),
                    "rename")
                .queryParam("toDatabaseId", toDatabaseId)
                .queryParam("toTableId", toTableId)
                .build()
                .encode()
                .toUri()
            : UriComponentsBuilder.fromHttpUrl(peer.getTablesApiBaseUri())
                .pathSegment("v1", "databases", table.getDatabaseId(), "tables", table.getTableId())
                .build()
                .encode()
                .toUri();
    HttpMethod method = RENAME.equals(operation) ? HttpMethod.PATCH : HttpMethod.DELETE;
    HttpRequest.Builder request =
        HttpRequest.newBuilder(uri)
            .timeout(Duration.ofSeconds(10))
            .method(method.name(), HttpRequest.BodyPublishers.noBody());
    headers.forEach((name, values) -> values.forEach(value -> request.header(name, value)));
    HttpResponse<Void> response;
    try {
      response = httpClient.send(request.build(), HttpResponse.BodyHandlers.discarding());
    } catch (IOException exception) {
      throw new ReplicationCascadeException(
          "Unable to reach replication Tables API peer", exception);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new ReplicationCascadeException(
          "Interrupted while calling replication Tables API peer", exception);
    }
    if (response.statusCode() == 401 || response.statusCode() == 403) {
      throw new AccessDeniedException(
          String.format(
              "Authenticated user %s is not authorized at the remote cluster", actingPrincipal));
    }
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new ReplicationCascadeException(
          String.format("Replication Tables API returned HTTP %d", response.statusCode()), null);
    }
  }

  private Set<String> replicationDestinations(TableDto table) {
    Set<String> destinations = new LinkedHashSet<>();
    if (table.getPolicies() == null
        || table.getPolicies().getReplication() == null
        || table.getPolicies().getReplication().getConfig() == null) {
      return destinations;
    }
    for (ReplicationConfig config : table.getPolicies().getReplication().getConfig()) {
      String destination = normalizeDestination(config.getDestination());
      if (destination.equalsIgnoreCase(clusterProperties.getClusterName())) {
        throw new IllegalStateException(
            String.format("Replication destination %s is the source cluster", destination));
      }
      if (destinations.stream().noneMatch(existing -> existing.equalsIgnoreCase(destination))) {
        destinations.add(destination);
      }
    }
    return destinations;
  }

  private String normalizeDestination(String destination) {
    if (destination == null) {
      throw new IllegalStateException("Replication policy contains a null destination");
    }
    String normalized = destination.trim();
    if (normalized.length() >= 2
        && ((normalized.startsWith("'") && normalized.endsWith("'"))
            || (normalized.startsWith("\"") && normalized.endsWith("\"")))) {
      normalized = normalized.substring(1, normalized.length() - 1).trim();
    }
    if (normalized.isEmpty()) {
      throw new IllegalStateException("Replication policy contains an empty destination");
    }
    return normalized;
  }

  private ReplicationCascadeException cascadeFailure(
      String operation, String destination, List<String> completed, Throwable cause) {
    return new ReplicationCascadeException(
        String.format(
            "%s failed at replication destination %s after successful destinations %s; "
                + "local table was not changed and the operation can be retried",
            operation, destination, completed),
        cause);
  }
}
