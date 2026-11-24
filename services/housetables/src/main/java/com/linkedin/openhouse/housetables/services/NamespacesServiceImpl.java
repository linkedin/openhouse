package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.housetables.config.db.DatabaseConfiguration.SupportedDbTypes;
import com.linkedin.openhouse.housetables.model.InvalidNamespaceDocumentException;
import com.linkedin.openhouse.housetables.model.NamespaceMetadata;
import com.linkedin.openhouse.housetables.model.NamespaceRow;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.NamespaceHtsJdbcRepository;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.persistence.EntityManager;
import javax.persistence.LockModeType;
import javax.persistence.OptimisticLockException;
import org.hibernate.StaleStateException;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
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
    try {
      return transactions.execute(
          status -> {
            NamespaceRow namespace = new NamespaceRow(namespaceName, properties);
            entityManager.persist(namespace);
            repository.flush();
            return metadata(namespace, namespace.getVersion());
          });
    } catch (DataIntegrityViolationException exception) {
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
    }
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
                    .map(namespace -> metadata(namespace, namespace.getVersion())))
        .orElseThrow(() -> new NotFoundException("Namespace not found: " + namespaceName));
  }

  @Override
  public void deleteNamespace(String namespaceName) throws NotFoundException {
    if (SupportedDbTypes.ICEBERG
        .name()
        .equals(clusterProperties.getClusterHouseTablesDatabaseType())) {
      throw new IllegalStateException("Namespace metadata requires a JDBC HouseTables backend.");
    }
    boolean deleted;
    try {
      deleted =
          transactions.execute(
              status ->
                  repository
                      .findByNamespaceName(namespaceName)
                      .map(
                          namespace -> {
                            repository.delete(namespace);
                            repository.flush();
                            return true;
                          })
                      .orElse(false));
    } catch (RuntimeException exception) {
      throw translateConflict(namespaceName, exception);
    }
    if (!deleted) {
      throw new NotFoundException("Namespace not found: " + namespaceName);
    }
  }

  @Override
  public NamespaceMetadata updateNamespace(
      String namespaceName, NamespaceMetadata namespaceMetadata) throws NotFoundException {
    if (SupportedDbTypes.ICEBERG
        .name()
        .equals(clusterProperties.getClusterHouseTablesDatabaseType())) {
      throw new IllegalStateException("Namespace metadata requires a JDBC HouseTables backend.");
    }
    try {
      return transactions
          .execute(
              status ->
                  repository
                      .findByNamespaceName(namespaceName)
                      .map(
                          namespace -> {
                            if (!namespace
                                    .getNamespaceUuid()
                                    .equals(namespaceMetadata.getNamespaceUuid())
                                || !Long.toString(namespace.getVersion())
                                    .equals(namespaceMetadata.getRevision())) {
                              throw new EntityConcurrentModificationException(namespaceName, null);
                            }
                            if (!namespace
                                    .getNamespaceName()
                                    .equals(namespaceMetadata.getNamespaceName())
                                || namespaceMetadata.getProperties() == null) {
                              throw new InvalidNamespaceDocumentException();
                            }
                            long version;
                            if (namespace
                                .getProperties()
                                .equals(namespaceMetadata.getProperties())) {
                              entityManager.lock(
                                  namespace, LockModeType.OPTIMISTIC_FORCE_INCREMENT);
                              version = namespace.getVersion() + 1;
                            } else {
                              namespace.replaceProperties(namespaceMetadata.getProperties());
                              repository.flush();
                              version = namespace.getVersion();
                            }
                            return metadata(namespace, version);
                          }))
          .orElseThrow(() -> new NotFoundException("Namespace not found: " + namespaceName));
    } catch (RuntimeException exception) {
      throw translateConflict(namespaceName, exception);
    }
  }

  private NamespaceMetadata metadata(NamespaceRow namespace, long version) {
    return NamespaceMetadata.builder()
        .namespaceUuid(namespace.getNamespaceUuid())
        .namespaceName(namespace.getNamespaceName())
        .properties(Collections.unmodifiableMap(new HashMap<>(namespace.getProperties())))
        .revision(Long.toString(version))
        .build();
  }

  private RuntimeException translateConflict(String namespaceName, RuntimeException exception) {
    for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
      if (cause instanceof OptimisticLockingFailureException
          || cause instanceof OptimisticLockException
          || cause instanceof StaleStateException) {
        return new EntityConcurrentModificationException(namespaceName, exception);
      }
    }
    return exception;
  }
}
