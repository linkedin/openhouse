package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.InvalidPropertyUpdateException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
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
import java.util.stream.Collectors;
import javax.persistence.EntityManager;
import javax.persistence.LockModeType;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class NamespacesServiceImpl implements NamespacesService {
  private final NamespaceHtsJdbcRepository repository;
  private final ClusterProperties clusterProperties;
  private final EntityManager entityManager;
  private final PlatformTransactionManager transactionManager;
  private final TransactionTemplate transactions;

  public NamespacesServiceImpl(
      NamespaceHtsJdbcRepository repository,
      ClusterProperties clusterProperties,
      EntityManager entityManager,
      PlatformTransactionManager transactionManager) {
    this.repository = repository;
    this.clusterProperties = clusterProperties;
    this.entityManager = entityManager;
    this.transactionManager = transactionManager;
    this.transactions = new TransactionTemplate(transactionManager);
    // Each operation owns transaction completion and failure handling.
    this.transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
  }

  @Override
  public NamespaceMetadata createNamespace(String namespaceName, Map<String, String> properties)
      throws DuplicateCreationException {
    if (SupportedDbTypes.ICEBERG
        .name()
        .equals(clusterProperties.getClusterHouseTablesDatabaseType())) {
      throw new IllegalStateException("Namespace metadata requires a JDBC HouseTables backend.");
    }
    TransactionStatus transaction = transactionManager.getTransaction(transactions);
    NamespaceMetadata result;
    try {
      NamespaceRow namespace = new NamespaceRow(namespaceName, properties);
      entityManager.persist(namespace);
      repository.flush();
      result =
          NamespaceMetadata.builder()
              .namespaceUuid(namespace.getNamespaceUuid())
              .namespaceName(namespace.getNamespaceName())
              .properties(Collections.unmodifiableMap(new HashMap<>(namespace.getProperties())))
              .build();
    } catch (DataIntegrityViolationException exception) {
      rollback(transaction, exception);
      boolean duplicateName =
          Optional.ofNullable(exception.getCause())
              .filter(ConstraintViolationException.class::isInstance)
              .map(ConstraintViolationException.class::cast)
              .map(ConstraintViolationException::getConstraintName)
              .map(name -> name.toLowerCase(Locale.ROOT).contains("uq_namespace_name"))
              .orElse(false);
      if (duplicateName) {
        throw new DuplicateCreationException("Namespace name already exists.", exception);
      }
      throw exception;
    } catch (RuntimeException exception) {
      rollback(transaction, exception);
      throw exception;
    } catch (Error failure) {
      rollback(transaction, failure);
      throw failure;
    }
    commit(transaction);
    return result;
  }

  @Override
  public NamespaceMetadata getNamespace(String namespaceName) throws NotFoundException {
    if (SupportedDbTypes.ICEBERG
        .name()
        .equals(clusterProperties.getClusterHouseTablesDatabaseType())) {
      throw new IllegalStateException("Namespace metadata requires a JDBC HouseTables backend.");
    }
    return transactions
        .execute(
            status ->
                repository
                    .findByNamespaceName(namespaceName)
                    .map(
                        namespace ->
                            NamespaceMetadata.builder()
                                .namespaceUuid(namespace.getNamespaceUuid())
                                .namespaceName(namespace.getNamespaceName())
                                .properties(
                                    Collections.unmodifiableMap(
                                        new HashMap<>(namespace.getProperties())))
                                .build()))
        .orElseThrow(() -> new NotFoundException("Namespace not found: " + namespaceName));
  }

  @Override
  public void deleteNamespace(String namespaceName) throws NotFoundException {
    if (SupportedDbTypes.ICEBERG
        .name()
        .equals(clusterProperties.getClusterHouseTablesDatabaseType())) {
      throw new IllegalStateException("Namespace metadata requires a JDBC HouseTables backend.");
    }
    TransactionStatus transaction = transactionManager.getTransaction(transactions);
    Optional<NamespaceRow> namespace;
    try {
      namespace = repository.findByNamespaceName(namespaceName);
    } catch (RuntimeException exception) {
      rollback(transaction, exception);
      throw exception;
    } catch (Error failure) {
      rollback(transaction, failure);
      throw failure;
    }
    if (!namespace.isPresent()) {
      commit(transaction);
      throw new NotFoundException("Namespace not found: " + namespaceName);
    }
    try {
      repository.delete(namespace.get());
      repository.flush();
    } catch (RuntimeException exception) {
      rollback(transaction, exception);
      throw exception;
    } catch (Error failure) {
      rollback(transaction, failure);
      throw failure;
    }
    commit(transaction);
  }

  @Override
  public NamespacePropertyUpdate updateNamespaceProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws NotFoundException, InvalidPropertyUpdateException {
    if (!Collections.disjoint(updates.keySet(), removals)) {
      throw new InvalidPropertyUpdateException(
          "Property updates and removals must have disjoint keys.");
    }
    if (SupportedDbTypes.ICEBERG
        .name()
        .equals(clusterProperties.getClusterHouseTablesDatabaseType())) {
      throw new IllegalStateException("Namespace metadata requires a JDBC HouseTables backend.");
    }
    TransactionStatus transaction = transactionManager.getTransaction(transactions);
    Optional<NamespaceRow> target;
    try {
      target = repository.findByNamespaceName(namespaceName);
    } catch (RuntimeException exception) {
      rollback(transaction, exception);
      throw exception;
    } catch (Error failure) {
      rollback(transaction, failure);
      throw failure;
    }
    if (!target.isPresent()) {
      commit(transaction);
      throw new NotFoundException("Namespace not found: " + namespaceName);
    }
    NamespaceRow namespace = target.get();
    NamespacePropertyUpdate result;
    try {
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
      // Check the loaded version synchronously, including empty updates, to reject stale writers.
      entityManager.lock(namespace, LockModeType.PESSIMISTIC_FORCE_INCREMENT);
      repository.flush();
      result =
          NamespacePropertyUpdate.builder()
              .updated(updates.keySet().stream().sorted().collect(Collectors.toList()))
              .removed(removed)
              .missing(missing)
              .build();
    } catch (RuntimeException exception) {
      rollback(transaction, exception);
      throw exception;
    } catch (Error failure) {
      rollback(transaction, failure);
      throw failure;
    }
    commit(transaction);
    return result;
  }

  private void commit(TransactionStatus transaction) {
    try {
      transactionManager.commit(transaction);
    } catch (TransactionSystemException exception) {
      Optional.ofNullable(exception.getApplicationException()).ifPresent(exception::addSuppressed);
      throw exception;
    }
  }

  private void rollback(TransactionStatus transaction, Throwable originalFailure) {
    try {
      transactionManager.rollback(transaction);
    } catch (RuntimeException completionFailure) {
      if (completionFailure != originalFailure) {
        completionFailure.addSuppressed(originalFailure);
      }
      throw completionFailure;
    }
  }
}
