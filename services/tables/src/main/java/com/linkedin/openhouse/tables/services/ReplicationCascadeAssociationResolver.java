package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/** Resolves a replica by its source table-format replication policy and catalog locator. */
@Component
public class ReplicationCascadeAssociationResolver {
  @Autowired private OpenHouseInternalRepository repository;

  @Autowired private ClusterProperties clusterProperties;

  public Optional<TableDto> resolve(String databaseId, String tableId) {
    Optional<TableDto> candidate =
        repository.findById(
            TableDtoPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build());
    if (!candidate.isPresent()) {
      return Optional.empty();
    }

    TableDto replica = candidate.get();
    if (replica.getTableType() != TableType.REPLICA_TABLE
        || replica.getTableUUID() == null
        || replica.getTableUUID().trim().isEmpty()
        || replica.getCreationTime() <= 0) {
      throw new ResponseStatusException(
          HttpStatus.FORBIDDEN, "Replication cascade target is not a valid replica table");
    }

    List<ReplicationConfig> policy =
        replica.getPolicies() == null || replica.getPolicies().getReplication() == null
            ? null
            : replica.getPolicies().getReplication().getConfig();
    boolean destinationConfigured =
        policy != null
            && policy.stream()
                .anyMatch(
                    config ->
                        config.getDestination() != null
                            && config
                                .getDestination()
                                .equalsIgnoreCase(clusterProperties.getClusterName()));
    if (!destinationConfigured) {
      throw new ResponseStatusException(
          HttpStatus.FORBIDDEN,
          "Replication cascade target does not contain the source replication policy");
    }

    return candidate;
  }
}
