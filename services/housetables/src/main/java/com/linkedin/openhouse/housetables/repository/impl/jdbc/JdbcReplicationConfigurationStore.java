package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.api.spec.model.ReplicationConfiguration;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationConfigurationSet;
import com.linkedin.openhouse.housetables.model.ReplicationConfigurationRow;
import com.linkedin.openhouse.housetables.model.ReplicationConfigurationStateRow;
import com.linkedin.openhouse.housetables.model.ReplicationConfigurationStateRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.ReplicationConfigurationStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class JdbcReplicationConfigurationStore implements ReplicationConfigurationStore {
  private final ReplicationConfigurationHtsJdbcRepository configurationRepository;

  private final ReplicationConfigurationStateHtsJdbcRepository stateRepository;

  public JdbcReplicationConfigurationStore(
      ReplicationConfigurationHtsJdbcRepository configurationRepository,
      ReplicationConfigurationStateHtsJdbcRepository stateRepository) {
    this.configurationRepository = configurationRepository;
    this.stateRepository = stateRepository;
  }

  @Override
  @Transactional(readOnly = true)
  public Optional<ReplicationConfigurationSet> findBySource(
      String sourceDatabaseId, String sourceTableId) {
    return stateRepository
        .findBySourceDatabaseIdIgnoreCaseAndSourceTableIdIgnoreCase(sourceDatabaseId, sourceTableId)
        .map(
            state -> {
              List<ReplicationConfiguration> configurations =
                  state.isConfigured()
                      ? configurationRepository
                          .findAllBySourceDatabaseIdIgnoreCaseAndSourceTableIdIgnoreCase(
                              sourceDatabaseId, sourceTableId)
                          .stream()
                          .sorted(
                              Comparator.comparing(
                                      ReplicationConfigurationRow::getDestinationClusterId)
                                  .thenComparing(
                                      ReplicationConfigurationRow::getDestinationDatabaseId)
                                  .thenComparing(
                                      ReplicationConfigurationRow::getDestinationTableId))
                          .map(
                              row ->
                                  ReplicationConfiguration.builder()
                                      .destinationClusterId(row.getDestinationClusterId())
                                      .destinationDatabaseId(row.getDestinationDatabaseId())
                                      .destinationTableId(row.getDestinationTableId())
                                      .replicationInterval(row.getReplicationInterval())
                                      .build())
                          .collect(Collectors.toList())
                      : new ArrayList<>();
              return ReplicationConfigurationSet.builder()
                  .sourceDatabaseId(state.getSourceDatabaseId())
                  .sourceTableId(state.getSourceTableId())
                  .configured(state.isConfigured())
                  .configurations(configurations)
                  .build();
            });
  }

  @Override
  @Transactional
  public void replace(ReplicationConfigurationSet replicationConfigurationSet) {
    String sourceDatabaseId = normalize(replicationConfigurationSet.getSourceDatabaseId());
    String sourceTableId = normalize(replicationConfigurationSet.getSourceTableId());
    boolean configured = replicationConfigurationSet.getConfigured();
    List<ReplicationConfiguration> configurations = replicationConfigurationSet.getConfigurations();
    validate(configured, configurations);

    Set<List<String>> destinationKeys = new HashSet<>();
    List<ReplicationConfigurationRow> rows = new ArrayList<>();
    for (ReplicationConfiguration configuration : configurations) {
      String destinationClusterId = normalize(configuration.getDestinationClusterId());
      String destinationDatabaseId = normalize(configuration.getDestinationDatabaseId());
      String destinationTableId = normalize(configuration.getDestinationTableId());
      List<String> key = List.of(destinationClusterId, destinationDatabaseId, destinationTableId);
      if (!destinationKeys.add(key)) {
        throw new IllegalArgumentException("Duplicate replication destination: " + key);
      }
      rows.add(
          ReplicationConfigurationRow.builder()
              .sourceDatabaseId(sourceDatabaseId)
              .sourceTableId(sourceTableId)
              .destinationClusterId(destinationClusterId)
              .destinationDatabaseId(destinationDatabaseId)
              .destinationTableId(destinationTableId)
              .replicationInterval(configuration.getReplicationInterval())
              .build());
    }

    configurationRepository.deleteAllBySource(sourceDatabaseId, sourceTableId);
    configurationRepository.saveAll(rows);

    ReplicationConfigurationStateRowPrimaryKey stateKey =
        ReplicationConfigurationStateRowPrimaryKey.builder()
            .sourceDatabaseId(sourceDatabaseId)
            .sourceTableId(sourceTableId)
            .build();
    Optional<ReplicationConfigurationStateRow> oldState = stateRepository.findById(stateKey);
    stateRepository.save(
        ReplicationConfigurationStateRow.builder()
            .sourceDatabaseId(sourceDatabaseId)
            .sourceTableId(sourceTableId)
            .configured(configured)
            .version(oldState.map(ReplicationConfigurationStateRow::getVersion).orElse(null))
            .build());
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

  private static String normalize(String value) {
    return value.toUpperCase(Locale.ROOT);
  }
}
