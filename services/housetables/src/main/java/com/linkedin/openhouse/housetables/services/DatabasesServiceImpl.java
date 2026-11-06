package com.linkedin.openhouse.housetables.services;

import static com.linkedin.openhouse.housetables.services.DatabaseMetadataException.Reason.*;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.housetables.config.db.DatabaseConfiguration.SupportedDbTypes;
import com.linkedin.openhouse.housetables.model.DatabaseMetadata;
import com.linkedin.openhouse.housetables.model.DatabasePropertyUpdate;
import com.linkedin.openhouse.housetables.model.DatabaseRow;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.DatabaseHtsJdbcRepository;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;
import javax.persistence.LockModeType;
import javax.persistence.OptimisticLockException;
import javax.persistence.PersistenceException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class DatabasesServiceImpl implements DatabasesService {
  private final DatabaseHtsJdbcRepository repository;
  private final ClusterProperties clusterProperties;
  private final EntityManager entityManager;
  private final TransactionTemplate transactions;

  public DatabasesServiceImpl(
      DatabaseHtsJdbcRepository repository,
      ClusterProperties clusterProperties,
      EntityManager entityManager,
      PlatformTransactionManager transactionManager) {
    this.repository = repository;
    this.clusterProperties = clusterProperties;
    this.entityManager = entityManager;
    this.transactions = new TransactionTemplate(transactionManager);
    // Completing the transaction here includes commit failures in the checked failure contract.
    this.transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
  }

  @Override
  public DatabaseMetadata createDatabase(String databaseName, Map<String, String> properties)
      throws DatabaseMetadataException {
    return inTransaction(
        () -> {
          DatabaseRow database = new DatabaseRow(databaseName, properties);
          entityManager.persist(database);
          repository.flush();
          return DatabaseMetadata.builder()
              .databaseUuid(database.getDatabaseUuid())
              .databaseName(database.getDatabaseName())
              .properties(Collections.unmodifiableMap(new HashMap<>(database.getProperties())))
              .build();
        },
        true);
  }

  @Override
  public DatabaseMetadata getDatabase(String databaseName) throws DatabaseMetadataException {
    return inTransaction(
            () ->
                repository
                    .findByDatabaseNameKey(databaseName.toLowerCase(Locale.ROOT))
                    .map(
                        database ->
                            DatabaseMetadata.builder()
                                .databaseUuid(database.getDatabaseUuid())
                                .databaseName(database.getDatabaseName())
                                .properties(
                                    Collections.unmodifiableMap(
                                        new HashMap<>(database.getProperties())))
                                .build()),
            false)
        .orElseThrow(
            () -> new DatabaseMetadataException(NOT_FOUND, "Database not found: " + databaseName));
  }

  @Override
  public void deleteDatabase(String databaseName) throws DatabaseMetadataException {
    boolean deleted =
        inTransaction(
            () ->
                repository
                    .findByDatabaseNameKey(databaseName.toLowerCase(Locale.ROOT))
                    .map(
                        database -> {
                          repository.delete(database);
                          repository.flush();
                          return true;
                        })
                    .orElse(false),
            false);
    if (!deleted) {
      throw new DatabaseMetadataException(NOT_FOUND, "Database not found: " + databaseName);
    }
  }

  @Override
  public DatabasePropertyUpdate updateDatabaseProperties(
      String databaseName, Map<String, String> updates, Set<String> removals)
      throws DatabaseMetadataException {
    if (!Collections.disjoint(updates.keySet(), removals)) {
      throw new DatabaseMetadataException(
          INVALID_UPDATE, "Property updates and removals must have disjoint keys.");
    }
    return inTransaction(
            () ->
                repository
                    .findByDatabaseNameKey(databaseName.toLowerCase(Locale.ROOT))
                    .map(
                        database -> {
                          // Empty updates also participate in optimistic concurrency checks.
                          entityManager.lock(database, LockModeType.OPTIMISTIC_FORCE_INCREMENT);
                          List<String> removed =
                              removals.stream()
                                  .filter(database.getProperties()::containsKey)
                                  .sorted()
                                  .collect(Collectors.toList());
                          List<String> missing =
                              removals.stream()
                                  .filter(key -> !database.getProperties().containsKey(key))
                                  .sorted()
                                  .collect(Collectors.toList());
                          Map<String, String> properties = new HashMap<>(database.getProperties());
                          properties.keySet().removeAll(removals);
                          properties.putAll(updates);
                          database.replaceProperties(properties);
                          repository.flush();
                          return DatabasePropertyUpdate.builder()
                              .updated(
                                  updates.keySet().stream().sorted().collect(Collectors.toList()))
                              .removed(removed)
                              .missing(missing)
                              .build();
                        }),
            false)
        .orElseThrow(
            () -> new DatabaseMetadataException(NOT_FOUND, "Database not found: " + databaseName));
  }

  private <T> T inTransaction(Supplier<T> operation, boolean creating)
      throws DatabaseMetadataException {
    if (SupportedDbTypes.ICEBERG
        .name()
        .equals(clusterProperties.getClusterHouseTablesDatabaseType())) {
      throw new DatabaseMetadataException(
          UNAVAILABLE, "Database metadata requires a JDBC HouseTables backend.");
    }
    try {
      return transactions.execute(status -> operation.get());
    } catch (OptimisticLockingFailureException | OptimisticLockException exception) {
      throw new DatabaseMetadataException(
          CONCURRENT_MODIFICATION,
          "Database metadata changed concurrently. Retry the request.",
          exception);
    } catch (DataIntegrityViolationException exception) {
      boolean duplicateName =
          creating
              && Optional.ofNullable(exception.getCause())
                  .filter(org.hibernate.exception.ConstraintViolationException.class::isInstance)
                  .map(org.hibernate.exception.ConstraintViolationException.class::cast)
                  .map(org.hibernate.exception.ConstraintViolationException::getConstraintName)
                  .map(name -> name.toLowerCase(Locale.ROOT).contains("uq_database_name"))
                  .orElse(false);
      throw new DatabaseMetadataException(
          duplicateName ? ALREADY_EXISTS : STORAGE_FAILURE,
          duplicateName
              ? "Database name already exists."
              : "Database metadata could not be persisted.",
          exception);
    } catch (DataAccessException | TransactionException | PersistenceException exception) {
      throw new DatabaseMetadataException(
          STORAGE_FAILURE, "Database metadata storage failed.", exception);
    }
  }
}
