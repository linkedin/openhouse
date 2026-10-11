package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.housetables.client.api.ReplicationStateControllerApi;
import com.linkedin.openhouse.housetables.client.model.ReplicationDestination;
import com.linkedin.openhouse.housetables.client.model.ReplicationEdgeState;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/**
 * Resolves the destination-side association for an authenticated, signed cascade.
 *
 * <p>This is intentionally separate from the user-facing replication-state facade, which requires
 * SYSTEM_ADMIN to expose association data. The result is used only to bind the peer assertion;
 * subsequent DDL still applies the authenticated caller's normal table and database ACLs.
 */
@Component
public class ReplicationCascadeAssociationResolver {
  @Autowired private ReplicationStateControllerApi replicationStateApi;

  public ReplicationDestination resolve(
      String sourceClusterId,
      String sourceTableUUID,
      long sourceCreationTime,
      String destinationClusterId) {
    List<ReplicationEdgeState> edges =
        replicationStateApi
            .getDestinations(sourceClusterId, sourceTableUUID, sourceCreationTime)
            .collectList()
            .block();
    if (edges == null) {
      throw new IllegalStateException("Replication destination lookup returned no response");
    }
    List<ReplicationDestination> matches =
        edges.stream()
            .map(
                edge -> {
                  if (edge == null || edge.getDestination() == null) {
                    throw new ResponseStatusException(
                        HttpStatus.CONFLICT, "Replication destination association is incomplete");
                  }
                  return edge.getDestination();
                })
            .filter(
                destination ->
                    sameLocator(destination.getDestinationClusterId(), destinationClusterId))
            .collect(Collectors.toList());
    if (matches.size() != 1) {
      throw new ResponseStatusException(
          matches.isEmpty() ? HttpStatus.FORBIDDEN : HttpStatus.CONFLICT,
          "Replication cascade has no unique destination association");
    }
    ReplicationDestination destination = matches.get(0);
    if (!sameLocator(destination.getSourceClusterId(), sourceClusterId)
        || !sameLocator(destination.getSourceTableUUID(), sourceTableUUID)
        || !Objects.equals(destination.getSourceCreationTime(), sourceCreationTime)
        || isBlank(destination.getDestinationTableUUID())
        || destination.getDestinationCreationTime() == null
        || isBlank(destination.getDestinationDatabaseId())
        || isBlank(destination.getDestinationTableId())) {
      throw new ResponseStatusException(
          HttpStatus.CONFLICT, "Replication cascade association does not match the source");
    }
    return destination;
  }

  private static boolean sameLocator(String stored, String requested) {
    return stored != null && requested != null && stored.equalsIgnoreCase(requested);
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
