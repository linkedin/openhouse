package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.housetables.client.api.ReplicationConfigurationApi;
import com.linkedin.openhouse.housetables.client.model.ReplicationConfiguration;
import com.linkedin.openhouse.housetables.client.model.ReplicationConfigurationSet;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Replication;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.utils.IntervalToCronConverter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@Component
public class ReplicationConfigurationCatalogService {
  @Autowired private ReplicationConfigurationApi replicationConfigurationApi;

  /**
   * Persist the request's replication policy, clearing it only when an existing catalog or legacy
   * Iceberg policy is present and the request omits replication.
   */
  public void synchronize(TableDto requested, TableDto existing, boolean preserveWhenOmitted) {
    Replication requestedReplication = getReplication(requested.getPolicies());
    if (requestedReplication != null) {
      List<ReplicationConfiguration> configurations =
          toCatalogConfigurations(requested, requestedReplication);
      replace(requested, true, configurations);
      return;
    }
    if (preserveWhenOmitted || existing == null) {
      return;
    }

    Optional<ReplicationConfigurationSet> catalogState =
        find(existing.getDatabaseId(), existing.getTableId());
    if (getReplication(existing.getPolicies()) != null
        || catalogState.filter(ReplicationConfigurationSet::getConfigured).isPresent()) {
      replace(requested, false, new ArrayList<>());
    }
  }

  /**
   * Prefer catalog state, while retaining legacy Iceberg metadata as a side-effect-free fallback.
   */
  public TableDto enrich(TableDto tableDto) {
    Optional<ReplicationConfigurationSet> catalogState =
        find(tableDto.getDatabaseId(), tableDto.getTableId());
    if (!catalogState.isPresent()) {
      return tableDto;
    }

    ReplicationConfigurationSet stored = catalogState.get();
    Replication replication =
        stored.getConfigured()
            ? Replication.builder().config(toApiConfigurations(stored.getConfigurations())).build()
            : null;
    Policies policies =
        tableDto.getPolicies() == null
            ? Policies.builder().replication(replication).build()
            : tableDto.getPolicies().toBuilder().replication(replication).build();
    return tableDto.toBuilder().policies(policies).build();
  }

  public boolean hasReplication(TableDto tableDto) {
    Optional<ReplicationConfigurationSet> catalogState =
        find(tableDto.getDatabaseId(), tableDto.getTableId());
    if (catalogState.isPresent()) {
      return catalogState.get().getConfigured()
          && !catalogState.get().getConfigurations().isEmpty();
    }
    Replication legacy = getReplication(tableDto.getPolicies());
    return legacy != null && legacy.getConfig() != null && !legacy.getConfig().isEmpty();
  }

  private Optional<ReplicationConfigurationSet> find(String databaseId, String tableId) {
    try {
      return Optional.ofNullable(
          replicationConfigurationApi.getReplicationConfiguration(databaseId, tableId).block());
    } catch (WebClientResponseException e) {
      if (e.getStatusCode().value() == 404) {
        return Optional.empty();
      }
      throw e;
    }
  }

  private void replace(
      TableDto tableDto, boolean configured, List<ReplicationConfiguration> configurations) {
    ReplicationConfigurationSet set =
        new ReplicationConfigurationSet()
            .sourceDatabaseId(tableDto.getDatabaseId())
            .sourceTableId(tableDto.getTableId())
            .configured(configured)
            .configurations(configurations);
    replicationConfigurationApi.replaceReplicationConfiguration(set).block();
  }

  private List<ReplicationConfiguration> toCatalogConfigurations(
      TableDto source, Replication replication) {
    if (replication.getConfig() == null) {
      throw new IllegalArgumentException("Replication configuration list cannot be null");
    }
    return replication.getConfig().stream()
        .map(
            config -> {
              if (config == null || config.getDestination() == null) {
                throw new IllegalArgumentException(
                    "Replication configuration must include a destination cluster");
              }
              return new ReplicationConfiguration()
                  .destinationClusterId(config.getDestination())
                  .destinationDatabaseId(source.getDatabaseId())
                  .destinationTableId(source.getTableId())
                  .replicationInterval(config.getInterval());
            })
        .collect(Collectors.toList());
  }

  private List<ReplicationConfig> toApiConfigurations(
      List<ReplicationConfiguration> configurations) {
    return configurations.stream()
        .map(
            configuration ->
                ReplicationConfig.builder()
                    .destination(configuration.getDestinationClusterId())
                    .interval(configuration.getReplicationInterval())
                    .cronSchedule(
                        IntervalToCronConverter.generateCronExpression(
                            configuration.getReplicationInterval()))
                    .build())
        .collect(Collectors.toList());
  }

  private static Replication getReplication(Policies policies) {
    return policies == null ? null : policies.getReplication();
  }
}
