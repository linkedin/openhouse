package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.housetables.client.api.ReplicationStateControllerApi;
import com.linkedin.openhouse.housetables.client.model.ReplicationCheckpoint;
import com.linkedin.openhouse.housetables.client.model.ReplicationCheckpointUpdate;
import com.linkedin.openhouse.housetables.client.model.ReplicationDestination;
import com.linkedin.openhouse.housetables.client.model.ReplicationEdgeState;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.List;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/** Authorized Tables-service facade for replication associations and checkpoint state. */
@Component
public class ReplicationStateCatalogService {
  @Autowired private ReplicationStateControllerApi replicationStateApi;
  @Autowired private TablesService tablesService;
  @Autowired private AuthorizationUtils authorizationUtils;

  public List<ReplicationEdgeState> getDestinations(
      String sourceClusterId,
      String sourceTableUUID,
      long sourceCreationTime,
      String actingPrincipal) {
    List<ReplicationEdgeState> edges =
        replicationStateApi
            .getDestinations(sourceClusterId, sourceTableUUID, sourceCreationTime)
            .collectList()
            .block();
    edges.forEach(edge -> resolveAndAuthorizeDestination(edge.getDestination(), actingPrincipal));
    return edges;
  }

  public ReplicationDestination putDestination(
      ReplicationDestination destination, String actingPrincipal) {
    TableDto destinationTable = authorizeDestination(destination, actingPrincipal);
    if (!sameLocator(destinationTable.getTableUUID(), destination.getDestinationTableUUID())
        || destinationTable.getCreationTime() != destination.getDestinationCreationTime()
        || !sameLocator(destinationTable.getClusterId(), destination.getDestinationClusterId())) {
      throw new ResponseStatusException(
          HttpStatus.CONFLICT, "Destination table generation does not match its current locator");
    }
    return replicationStateApi.putDestination(destination).block();
  }

  public ReplicationCheckpoint getCheckpoint(
      String sourceClusterId,
      String sourceTableUUID,
      long sourceCreationTime,
      String destinationClusterId,
      String destinationTableUUID,
      long destinationCreationTime,
      String actingPrincipal) {
    ReplicationDestination destination =
        findDestination(
            sourceClusterId,
            sourceTableUUID,
            sourceCreationTime,
            destinationClusterId,
            destinationTableUUID,
            destinationCreationTime);
    authorizeDestination(destination, actingPrincipal);
    return replicationStateApi
        .getCheckpoint(
            sourceClusterId,
            sourceTableUUID,
            sourceCreationTime,
            destinationClusterId,
            destinationTableUUID,
            destinationCreationTime)
        .block();
  }

  public ReplicationCheckpoint advanceCheckpoint(
      ReplicationCheckpointUpdate update, String actingPrincipal) {
    ReplicationDestination destination =
        findDestination(
            update.getSourceClusterId(),
            update.getSourceTableUUID(),
            update.getSourceCreationTime(),
            update.getDestinationClusterId(),
            update.getDestinationTableUUID(),
            update.getDestinationCreationTime());
    if (!sameLocator(destination.getSourceDatabaseId(), update.getSourceDatabaseId())
        || !sameLocator(destination.getSourceTableId(), update.getSourceTableId())
        || !sameLocator(destination.getDestinationDatabaseId(), update.getDestinationDatabaseId())
        || !sameLocator(destination.getDestinationTableId(), update.getDestinationTableId())) {
      throw new ResponseStatusException(
          HttpStatus.CONFLICT, "Replication locator changed before checkpoint advancement");
    }
    TableDto destinationTable = authorizeDestination(destination, actingPrincipal);
    if (!sameLocator(destinationTable.getTableUUID(), update.getDestinationTableUUID())
        || destinationTable.getCreationTime() != update.getDestinationCreationTime()
        || !sameLocator(destinationTable.getClusterId(), update.getDestinationClusterId())) {
      throw new ResponseStatusException(
          HttpStatus.CONFLICT,
          "Destination table generation changed before checkpoint advancement");
    }
    if (!Objects.equals(destinationTable.getTableVersion(), update.getDestinationTableVersion())) {
      throw new ResponseStatusException(
          HttpStatus.CONFLICT,
          "Destination version is no longer current; reconcile the snapshot commit before advancing");
    }
    return replicationStateApi.advanceCheckpoint(update).block();
  }

  private static boolean sameLocator(String stored, String requested) {
    return stored != null && requested != null && stored.equalsIgnoreCase(requested);
  }

  private ReplicationDestination findDestination(
      String sourceClusterId,
      String sourceTableUUID,
      long sourceCreationTime,
      String destinationClusterId,
      String destinationTableUUID,
      long destinationCreationTime) {
    List<ReplicationEdgeState> edges =
        replicationStateApi
            .getDestinations(sourceClusterId, sourceTableUUID, sourceCreationTime)
            .collectList()
            .block();
    return edges.stream()
        .map(ReplicationEdgeState::getDestination)
        .filter(
            destination ->
                sameLocator(destination.getDestinationClusterId(), destinationClusterId)
                    && sameLocator(destination.getDestinationTableUUID(), destinationTableUUID)
                    && Objects.equals(
                        destination.getDestinationCreationTime(), destinationCreationTime))
        .findFirst()
        .orElseThrow(
            () ->
                new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "Replication destination association not found"));
  }

  private TableDto authorizeDestination(
      ReplicationDestination destination, String actingPrincipal) {
    if (destination == null) {
      throw new ResponseStatusException(
          HttpStatus.NOT_FOUND, "Replication destination association not found");
    }
    TableDto table =
        tablesService.getTable(
            destination.getDestinationDatabaseId(),
            destination.getDestinationTableId(),
            actingPrincipal);
    authorizationUtils.checkTablePrivilege(table, actingPrincipal, Privileges.SYSTEM_ADMIN);
    return table;
  }

  private TableDto resolveAndAuthorizeDestination(
      ReplicationDestination destination, String actingPrincipal) {
    if (destination == null) {
      throw new ResponseStatusException(
          HttpStatus.NOT_FOUND, "Replication destination association not found");
    }
    TableDto table =
        tablesService.getTableByIdentity(
            destination.getDestinationClusterId(),
            destination.getDestinationTableUUID(),
            destination.getDestinationCreationTime(),
            actingPrincipal);
    authorizationUtils.checkTablePrivilege(table, actingPrincipal, Privileges.SYSTEM_ADMIN);
    destination.setDestinationDatabaseId(table.getDatabaseId());
    destination.setDestinationTableId(table.getTableId());
    return table;
  }
}
