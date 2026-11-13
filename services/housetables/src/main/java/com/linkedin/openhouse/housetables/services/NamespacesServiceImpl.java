package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.exception.checked.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.checked.DomainException;
import com.linkedin.openhouse.common.exception.checked.InvalidPropertyUpdateException;
import com.linkedin.openhouse.common.exception.checked.MutationUncertainException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.common.exception.checked.ServiceFailureException;
import com.linkedin.openhouse.common.exception.checked.UnavailableException;
import com.linkedin.openhouse.common.exception.checked.VersionConflictException;
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
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.UnexpectedRollbackException;
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
    // Completing the transaction here includes commit failures in the checked failure contract.
    this.transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
  }

  @Override
  public NamespaceMetadata createNamespace(String namespaceName, Map<String, String> properties)
      throws DomainException {
    if (SupportedDbTypes.ICEBERG
        .name()
        .equals(clusterProperties.getClusterHouseTablesDatabaseType())) {
      throw new UnavailableException("Namespace metadata requires a JDBC HouseTables backend.");
    }
    TransactionStatus transaction = beginMutation();
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
    } catch (OptimisticLockingFailureException | OptimisticLockException exception) {
      rollbackMutation(transaction, exception);
      throw new VersionConflictException("Namespace metadata changed concurrently.", exception);
    } catch (DataIntegrityViolationException exception) {
      rollbackMutation(transaction, exception);
      boolean duplicateName =
          Optional.ofNullable(exception.getCause())
              .filter(ConstraintViolationException.class::isInstance)
              .map(ConstraintViolationException.class::cast)
              .map(ConstraintViolationException::getConstraintName)
              .map(name -> name.toLowerCase(Locale.ROOT).contains("uq_namespace_name"))
              .orElse(false);
      if (duplicateName) {
        throw new AlreadyExistsException("Namespace name already exists.", exception);
      }
      throw new ServiceFailureException("Namespace metadata could not be persisted.", exception);
    } catch (RuntimeException exception) {
      rollbackMutation(transaction, exception);
      throw new ServiceFailureException("Namespace metadata storage failed.", exception);
    } catch (Error failure) {
      rollbackMutation(transaction, failure);
      throw failure;
    }
    commitMutation(transaction);
    return result;
  }

  @Override
  public NamespaceMetadata getNamespace(String namespaceName) throws DomainException {
    if (SupportedDbTypes.ICEBERG
        .name()
        .equals(clusterProperties.getClusterHouseTablesDatabaseType())) {
      throw new UnavailableException("Namespace metadata requires a JDBC HouseTables backend.");
    }
    try {
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
    } catch (OptimisticLockingFailureException | OptimisticLockException exception) {
      throw new VersionConflictException(
          "Namespace metadata changed concurrently. Retry the read.", exception);
    } catch (DataAccessException | TransactionException | PersistenceException exception) {
      throw new ServiceFailureException("Namespace metadata storage failed.", exception);
    }
  }

  @Override
  public void deleteNamespace(String namespaceName) throws DomainException {
    if (SupportedDbTypes.ICEBERG
        .name()
        .equals(clusterProperties.getClusterHouseTablesDatabaseType())) {
      throw new UnavailableException("Namespace metadata requires a JDBC HouseTables backend.");
    }
    TransactionStatus transaction = beginMutation();
    Optional<NamespaceRow> namespace;
    try {
      namespace = repository.findByNamespaceName(namespaceName);
    } catch (OptimisticLockingFailureException | OptimisticLockException exception) {
      rollbackLookup(transaction, exception);
      throw new VersionConflictException("Namespace metadata changed concurrently.", exception);
    } catch (RuntimeException exception) {
      rollbackLookup(transaction, exception);
      throw new ServiceFailureException("Namespace metadata storage failed.", exception);
    } catch (Error failure) {
      rollbackLookup(transaction, failure);
      throw failure;
    }
    if (!namespace.isPresent()) {
      commitLookup(transaction);
      throw new NotFoundException("Namespace not found: " + namespaceName);
    }
    try {
      repository.delete(namespace.get());
      repository.flush();
    } catch (OptimisticLockingFailureException | OptimisticLockException exception) {
      rollbackMutation(transaction, exception);
      throw new VersionConflictException("Namespace metadata changed concurrently.", exception);
    } catch (RuntimeException exception) {
      rollbackMutation(transaction, exception);
      throw new ServiceFailureException("Namespace metadata storage failed.", exception);
    } catch (Error failure) {
      rollbackMutation(transaction, failure);
      throw failure;
    }
    commitMutation(transaction);
  }

  @Override
  public NamespacePropertyUpdate updateNamespaceProperties(
      String namespaceName, Map<String, String> updates, Set<String> removals)
      throws DomainException {
    if (!Collections.disjoint(updates.keySet(), removals)) {
      throw new InvalidPropertyUpdateException(
          "Property updates and removals must have disjoint keys.");
    }
    if (SupportedDbTypes.ICEBERG
        .name()
        .equals(clusterProperties.getClusterHouseTablesDatabaseType())) {
      throw new UnavailableException("Namespace metadata requires a JDBC HouseTables backend.");
    }
    TransactionStatus transaction = beginMutation();
    Optional<NamespaceRow> target;
    try {
      target = repository.findByNamespaceName(namespaceName);
    } catch (OptimisticLockingFailureException | OptimisticLockException exception) {
      rollbackLookup(transaction, exception);
      throw new VersionConflictException("Namespace metadata changed concurrently.", exception);
    } catch (RuntimeException exception) {
      rollbackLookup(transaction, exception);
      throw new ServiceFailureException("Namespace metadata storage failed.", exception);
    } catch (Error failure) {
      rollbackLookup(transaction, failure);
      throw failure;
    }
    if (!target.isPresent()) {
      commitLookup(transaction);
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
      // Check the loaded version synchronously, including empty updates, so a stale writer
      // can be reported as a conflict after successful rollback.
      entityManager.lock(namespace, LockModeType.PESSIMISTIC_FORCE_INCREMENT);
      repository.flush();
      result =
          NamespacePropertyUpdate.builder()
              .updated(updates.keySet().stream().sorted().collect(Collectors.toList()))
              .removed(removed)
              .missing(missing)
              .build();
    } catch (OptimisticLockingFailureException | OptimisticLockException exception) {
      rollbackMutation(transaction, exception);
      throw new VersionConflictException("Namespace metadata changed concurrently.", exception);
    } catch (DataIntegrityViolationException exception) {
      rollbackMutation(transaction, exception);
      throw new ServiceFailureException("Namespace metadata could not be persisted.", exception);
    } catch (RuntimeException exception) {
      rollbackMutation(transaction, exception);
      throw new ServiceFailureException("Namespace metadata storage failed.", exception);
    } catch (Error failure) {
      rollbackMutation(transaction, failure);
      throw failure;
    }
    commitMutation(transaction);
    return result;
  }

  private TransactionStatus beginMutation() throws ServiceFailureException {
    try {
      return transactionManager.getTransaction(transactions);
    } catch (TransactionException exception) {
      throw new ServiceFailureException("Namespace metadata storage failed.", exception);
    }
  }

  private void commitLookup(TransactionStatus transaction) throws ServiceFailureException {
    try {
      transactionManager.commit(transaction);
    } catch (RuntimeException exception) {
      throw new ServiceFailureException("Namespace metadata lookup failed.", exception);
    }
  }

  private void rollbackLookup(TransactionStatus transaction, Throwable originalFailure)
      throws ServiceFailureException {
    try {
      transactionManager.rollback(transaction);
    } catch (RuntimeException completionFailure) {
      if (completionFailure != originalFailure) {
        completionFailure.addSuppressed(originalFailure);
      }
      throw new ServiceFailureException("Namespace metadata lookup failed.", completionFailure);
    }
  }

  private void commitMutation(TransactionStatus transaction) throws DomainException {
    try {
      transactionManager.commit(transaction);
    } catch (UnexpectedRollbackException exception) {
      throw new ServiceFailureException("Namespace metadata storage failed.", exception);
    } catch (TransactionSystemException exception) {
      Optional.ofNullable(exception.getApplicationException()).ifPresent(exception::addSuppressed);
      throw new MutationUncertainException("Namespace mutation outcome is unknown.", exception);
    } catch (RuntimeException exception) {
      throw new MutationUncertainException("Namespace mutation outcome is unknown.", exception);
    }
  }

  private void rollbackMutation(TransactionStatus transaction, Throwable originalFailure)
      throws MutationUncertainException {
    try {
      transactionManager.rollback(transaction);
    } catch (RuntimeException completionFailure) {
      if (completionFailure != originalFailure) {
        completionFailure.addSuppressed(originalFailure);
      }
      throw new MutationUncertainException(
          "Namespace mutation outcome is unknown.", completionFailure);
    }
  }
}
