package com.linkedin.openhouse.housetables.repository.impl.iceberg;

import com.linkedin.openhouse.housetables.api.spec.model.ReplicationConfiguration;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationConfigurationSet;
import com.linkedin.openhouse.housetables.repository.ReplicationConfigurationStore;
import com.linkedin.openhouse.hts.catalog.model.replicationconfiguration.ReplicationConfigurationIcebergRow;
import com.linkedin.openhouse.hts.catalog.model.replicationconfiguration.ReplicationConfigurationIcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.model.replicationconfiguration.ReplicationConfigurationStateIcebergRow;
import com.linkedin.openhouse.hts.catalog.model.replicationconfiguration.ReplicationConfigurationStateIcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.repository.IcebergHtsRepository;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(value = "cluster.housetables.database.type", havingValue = "ICEBERG")
public class IcebergReplicationConfigurationStore implements ReplicationConfigurationStore {
  private final IcebergHtsRepository<
          ReplicationConfigurationIcebergRow, ReplicationConfigurationIcebergRowPrimaryKey>
      configurationRepository;

  private final IcebergHtsRepository<
          ReplicationConfigurationStateIcebergRow,
          ReplicationConfigurationStateIcebergRowPrimaryKey>
      stateRepository;

  public IcebergReplicationConfigurationStore(
      IcebergHtsRepository<
              ReplicationConfigurationIcebergRow, ReplicationConfigurationIcebergRowPrimaryKey>
          configurationRepository,
      IcebergHtsRepository<
              ReplicationConfigurationStateIcebergRow,
              ReplicationConfigurationStateIcebergRowPrimaryKey>
          stateRepository) {
    this.configurationRepository = configurationRepository;
    this.stateRepository = stateRepository;
  }

  @Override
  public Optional<ReplicationConfigurationSet> findBySource(
      String sourceDatabaseId, String sourceTableId) {
    ReplicationConfigurationStateIcebergRowPrimaryKey stateKey =
        stateKey(sourceDatabaseId, sourceTableId);
    return stateRepository
        .findById(stateKey)
        .map(
            state -> {
              List<ReplicationConfiguration> configurations =
                  state.getConfigured()
                      ? toConfigurations(
                          configurationRepository.searchByPartialId(
                              sourceKey(sourceDatabaseId, sourceTableId)))
                      : new ArrayList<>();
              return ReplicationConfigurationSet.builder()
                  .sourceDatabaseId(state.getSourceDatabaseId())
                  .sourceTableId(state.getSourceTableId())
                  .configured(state.getConfigured())
                  .configurations(configurations)
                  .build();
            });
  }

  @Override
  public void replace(ReplicationConfigurationSet replicationConfigurationSet) {
    String sourceDatabaseId = normalize(replicationConfigurationSet.getSourceDatabaseId());
    String sourceTableId = normalize(replicationConfigurationSet.getSourceTableId());
    boolean configured = replicationConfigurationSet.getConfigured();
    List<ReplicationConfiguration> configurations = replicationConfigurationSet.getConfigurations();
    validate(configured, configurations);

    ReplicationConfigurationStateIcebergRowPrimaryKey stateKey =
        stateKey(sourceDatabaseId, sourceTableId);
    Optional<ReplicationConfigurationStateIcebergRow> oldState = stateRepository.findById(stateKey);
    ReplicationConfigurationIcebergRowPrimaryKey partialKey =
        sourceKey(sourceDatabaseId, sourceTableId);
    List<ReplicationConfigurationIcebergRow> oldRows =
        toRows(configurationRepository.searchByPartialId(partialKey));
    Map<List<String>, String> versionsByDestination = new HashMap<>();
    oldRows.forEach(
        row ->
            versionsByDestination.put(
                destinationKey(
                    row.getDestinationClusterId(),
                    row.getDestinationDatabaseId(),
                    row.getDestinationTableId()),
                row.getCurrentVersion()));

    List<ReplicationConfigurationIcebergRow> newRows = new ArrayList<>();
    Set<List<String>> destinationKeys = new HashSet<>();
    for (ReplicationConfiguration configuration : configurations) {
      String destinationClusterId = normalize(configuration.getDestinationClusterId());
      String destinationDatabaseId = normalize(configuration.getDestinationDatabaseId());
      String destinationTableId = normalize(configuration.getDestinationTableId());
      List<String> destinationKey =
          destinationKey(destinationClusterId, destinationDatabaseId, destinationTableId);
      if (!destinationKeys.add(destinationKey)) {
        throw new IllegalArgumentException("Duplicate replication destination: " + destinationKey);
      }
      newRows.add(
          ReplicationConfigurationIcebergRow.builder()
              .sourceDatabaseId(sourceDatabaseId)
              .sourceTableId(sourceTableId)
              .destinationClusterId(destinationClusterId)
              .destinationDatabaseId(destinationDatabaseId)
              .destinationTableId(destinationTableId)
              .replicationInterval(configuration.getReplicationInterval())
              .version(versionsByDestination.get(destinationKey))
              .build());
    }

    ReplicationConfigurationStateIcebergRow stateRow =
        ReplicationConfigurationStateIcebergRow.builder()
            .sourceDatabaseId(sourceDatabaseId)
            .sourceTableId(sourceTableId)
            .configured(configured)
            .version(
                oldState
                    .map(ReplicationConfigurationStateIcebergRow::getCurrentVersion)
                    .orElse(null))
            .build();

    if (configured && !newRows.isEmpty()) {
      try {
        configurationRepository.replaceByPartialId(partialKey, newRows);
      } catch (CommitFailedException e) {
        throw new IllegalStateException(
            "Replication configuration changed concurrently; retry the request.", e);
      }
      stateRepository.save(stateRow);
    } else {
      stateRepository.save(stateRow);
      configurationRepository.replaceByPartialId(partialKey, newRows);
    }
  }

  private List<ReplicationConfiguration> toConfigurations(
      Iterable<ReplicationConfigurationIcebergRow> rows) {
    return toRows(rows).stream()
        .sorted(
            Comparator.comparing(ReplicationConfigurationIcebergRow::getDestinationClusterId)
                .thenComparing(ReplicationConfigurationIcebergRow::getDestinationDatabaseId)
                .thenComparing(ReplicationConfigurationIcebergRow::getDestinationTableId))
        .map(
            row ->
                ReplicationConfiguration.builder()
                    .destinationClusterId(row.getDestinationClusterId())
                    .destinationDatabaseId(row.getDestinationDatabaseId())
                    .destinationTableId(row.getDestinationTableId())
                    .replicationInterval(row.getReplicationInterval())
                    .build())
        .collect(Collectors.toList());
  }

  private List<ReplicationConfigurationIcebergRow> toRows(
      Iterable<ReplicationConfigurationIcebergRow> rows) {
    List<ReplicationConfigurationIcebergRow> result = new ArrayList<>();
    rows.forEach(result::add);
    return result;
  }

  private static ReplicationConfigurationIcebergRowPrimaryKey sourceKey(
      String sourceDatabaseId, String sourceTableId) {
    return ReplicationConfigurationIcebergRowPrimaryKey.builder()
        .sourceDatabaseId(normalize(sourceDatabaseId))
        .sourceTableId(normalize(sourceTableId))
        .build();
  }

  private static ReplicationConfigurationStateIcebergRowPrimaryKey stateKey(
      String sourceDatabaseId, String sourceTableId) {
    return ReplicationConfigurationStateIcebergRowPrimaryKey.builder()
        .sourceDatabaseId(normalize(sourceDatabaseId))
        .sourceTableId(normalize(sourceTableId))
        .build();
  }

  private static void validate(boolean configured, List<ReplicationConfiguration> configurations) {
    if (configurations == null) {
      throw new IllegalArgumentException("Replication configurations cannot be null");
    }
    if (!configured && !configurations.isEmpty()) {
      throw new IllegalArgumentException(
          "A cleared replication configuration cannot have destinations");
    }
  }

  private static List<String> destinationKey(
      String destinationClusterId, String destinationDatabaseId, String destinationTableId) {
    return List.of(destinationClusterId, destinationDatabaseId, destinationTableId);
  }

  private static String normalize(String value) {
    return value.toUpperCase(Locale.ROOT);
  }
}
