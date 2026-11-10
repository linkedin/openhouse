package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.housetables.config.db.DatabaseConfiguration.SupportedDbTypes;
import com.linkedin.openhouse.housetables.model.NamespaceMetadata;
import com.linkedin.openhouse.housetables.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.housetables.model.NamespaceRow;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.NamespaceHtsJdbcRepository;
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
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class NamespacesServiceImpl implements NamespacesService {
  private final NamespaceHtsJdbcRepository repository;
  private final ClusterProperties clusterProperties;
  private final EntityManager entityManager;
  private final TransactionTemplate transactions;

  public NamespacesServiceImpl(
      NamespaceHtsJdbcRepository repository,
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
  public NamespaceMetadata createNamespace(String namespaceName, Map<String, String> properties)
      throws NamespaceMetadataException {
    return inTransaction(
        () -> {
          NamespaceRow namespace = new NamespaceRow(namespaceName, properties);
          entityManager.persist(namespace);
          repository.flush();
          return NamespaceMetadata.builder()
              .namespaceUuid(namespace.getNamespaceUuid())
              .namespaceName(namespace.getNamespaceName())
              .properties(Collections.unmodifiableMap(new HashMap<>(namespace.getProperties())))
              .build();
        },
        true);
  }

  @Override
  public NamespaceMetadata getNamespace(String namespaceName) throws NamespaceMetadataException {
    return inTransaction(
            () ->
                repository
                    .findByNamespaceName(namespaceName.toLowerCase(Locale.ROOT))
                    .map(
                        namespace ->
                            NamespaceMetadata.builder()
                                .namespaceUuid(namespace.getNamespaceUuid())
                                .namespaceName(namespace.getNamespaceName())
                                .properties(
                                    Collections.unmodifiableMap(
                                        new HashMap<>(namespace.getProperties())))
                                .build()),
            false)
        .orElseThrow(() -> new NamespaceNotFoundException("Namespace not found: " + namespaceName));
  }

  @Override
  public void deleteNamespace(String namespaceName) throws NamespaceMetadataException {
    boolean deleted =
        inTransaction(
            () ->
                repository
                    .findByNamespaceName(namespaceName.toLowerCase(Locale.ROOT))
                    .map(
                        namespace -> {
                          repository.delete(namespace);
                          repository.flush();
                          return true;
                        })
                    .orElse(false),
            false);
    if (!deleted) {
      throw new NamespaceNotFoundException("Namespace not found: " + namespaceName);
    }
  }

  @Override
  public NamespacePropertyUpdate updateNamespaceProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws NamespaceMetadataException {
    if (!Collections.disjoint(updates.keySet(), removals)) {
      throw new InvalidNamespacePropertyUpdateException(
          "Property updates and removals must have disjoint keys.");
    }
    return inTransaction(
            () ->
                repository
                    .findByNamespaceName(namespaceName.toLowerCase(Locale.ROOT))
                    .map(
                        namespace -> {
                          // Empty updates also participate in optimistic concurrency checks.
                          entityManager.lock(namespace, LockModeType.OPTIMISTIC_FORCE_INCREMENT);
                          List<String> removed =
                              removals.stream()
                                  .filter(namespace.getProperties()::containsKey)
                                  .sorted()
                                  .collect(Collectors.toList());
                          List<String> missing =
                              removals.stream()
                                  .filter(key -> !namespace.getProperties().containsKey(key))
                                  .sorted()
                                  .collect(Collectors.toList());
                          Map<String, String> properties = new HashMap<>(namespace.getProperties());
                          properties.keySet().removeAll(removals);
                          properties.putAll(updates);
                          namespace.replaceProperties(properties);
                          repository.flush();
                          return NamespacePropertyUpdate.builder()
                              .updated(
                                  updates.keySet().stream().sorted().collect(Collectors.toList()))
                              .removed(removed)
                              .missing(missing)
                              .build();
                        }),
            false)
        .orElseThrow(() -> new NamespaceNotFoundException("Namespace not found: " + namespaceName));
  }

  private <T> T inTransaction(Supplier<T> operation, boolean creating)
      throws NamespaceMetadataException {
    if (SupportedDbTypes.ICEBERG
        .name()
        .equals(clusterProperties.getClusterHouseTablesDatabaseType())) {
      throw new NamespaceMetadataUnavailableException(
          "Namespace metadata requires a JDBC HouseTables backend.");
    }
    try {
      return transactions.execute(status -> operation.get());
    } catch (OptimisticLockingFailureException | OptimisticLockException exception) {
      throw new NamespaceConcurrentModificationException(
          "Namespace metadata changed concurrently. Retry the request.", exception);
    } catch (DataIntegrityViolationException exception) {
      boolean duplicateName =
          creating
              && Optional.ofNullable(exception.getCause())
                  .filter(ConstraintViolationException.class::isInstance)
                  .map(ConstraintViolationException.class::cast)
                  .map(ConstraintViolationException::getConstraintName)
                  .map(name -> name.toLowerCase(Locale.ROOT).contains("uq_namespace_name"))
                  .orElse(false);
      if (duplicateName) {
        throw new NamespaceAlreadyExistsException("Namespace name already exists.", exception);
      }
      throw new NamespaceMetadataStorageException(
          "Namespace metadata could not be persisted.", exception);
    } catch (DataAccessException | TransactionException | PersistenceException exception) {
      throw new NamespaceMetadataStorageException("Namespace metadata storage failed.", exception);
    }
  }
}
