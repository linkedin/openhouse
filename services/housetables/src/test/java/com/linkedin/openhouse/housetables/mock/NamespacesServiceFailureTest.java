package com.linkedin.openhouse.housetables.mock;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.housetables.model.NamespaceRow;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.NamespaceHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.NamespacesService;
import com.linkedin.openhouse.housetables.services.NamespacesServiceImpl;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.persistence.EntityManager;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.SimpleTransactionStatus;

class NamespacesServiceFailureTest {
  private final NamespaceHtsJdbcRepository repository = mock(NamespaceHtsJdbcRepository.class);
  private final ClusterProperties clusterProperties = mock(ClusterProperties.class);
  private final EntityManager entityManager = mock(EntityManager.class);
  private final PlatformTransactionManager transactionManager =
      mock(PlatformTransactionManager.class);
  private final NamespacesService namespacesService =
      new NamespacesServiceImpl(repository, clusterProperties, entityManager, transactionManager);

  @Test
  void testIcebergBackendReportsNamespaceMetadataUnavailable() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("ICEBERG");
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> namespacesService.getNamespace("foo"))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> namespacesService.deleteNamespace("foo"))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(
            () ->
                namespacesService.updateNamespaceProperties("foo", Map.of("bar", "one"), Set.of()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void testRepositoryFailurePropagates() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByNamespaceName("foo"))
        .thenThrow(new DataAccessResourceFailureException("storage unavailable"));
    assertThatThrownBy(() -> namespacesService.getNamespace("foo"))
        .isInstanceOf(DataAccessResourceFailureException.class)
        .hasMessage("storage unavailable");
    verify(transactionManager).rollback(any());
  }

  @Test
  void testCommitFailureReportsFailure() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    doThrow(new TransactionSystemException("commit failed")).when(transactionManager).commit(any());
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(TransactionSystemException.class)
        .hasMessage("commit failed");
  }

  @Test
  void testReadCommitFailurePropagates() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByNamespaceName("foo"))
        .thenReturn(Optional.of(new NamespaceRow("foo", Map.of())));
    TransactionSystemException failure = new TransactionSystemException("commit failed");
    doThrow(failure).when(transactionManager).commit(any());

    assertThatThrownBy(() -> namespacesService.getNamespace("foo")).isSameAs(failure);
  }

  @Test
  void testDeleteCommitFailureReportsFailure() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByNamespaceName("foo"))
        .thenReturn(Optional.of(new NamespaceRow("foo", Map.of())));
    TransactionSystemException failure = new TransactionSystemException("commit failed");
    doThrow(failure).when(transactionManager).commit(any());

    assertThatThrownBy(() -> namespacesService.deleteNamespace("foo")).isSameAs(failure);
  }

  @Test
  void testNamespaceNameConstraintDuringUpdateBecomesStorageFailure() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByNamespaceName("foo"))
        .thenReturn(Optional.of(new NamespaceRow("foo", Map.of())));
    DataIntegrityViolationException failure =
        new DataIntegrityViolationException(
            "constraint failed",
            new ConstraintViolationException(
                "constraint failed", new SQLException(), "uq_namespace_name"));
    doThrow(failure).when(repository).flush();

    assertThatThrownBy(
            () ->
                namespacesService.updateNamespaceProperties("foo", Map.of("bar", "one"), Set.of()))
        .isSameAs(failure);
    verify(transactionManager).rollback(any());
  }

  @Test
  void testOnlyDuplicateNamespaceNameBecomesConflict() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    DataIntegrityViolationException duplicateName =
        new DataIntegrityViolationException(
            "constraint failed",
            new ConstraintViolationException(
                "constraint failed", new SQLException(), "uq_namespace_name"));
    doThrow(duplicateName).when(repository).flush();
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(DuplicateCreationException.class)
        .hasCause(duplicateName);
    DataIntegrityViolationException otherConstraint =
        new DataIntegrityViolationException(
            "constraint failed",
            new ConstraintViolationException(
                "constraint failed", new SQLException(), "other_constraint"));
    doThrow(otherConstraint).when(repository).flush();
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isSameAs(otherConstraint);
    verify(transactionManager, times(2)).rollback(any());
  }

  @Test
  void duplicateNamespaceNameAtCommitReportsConflict() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    DataIntegrityViolationException duplicateName =
        new DataIntegrityViolationException(
            "constraint failed",
            new ConstraintViolationException(
                "constraint failed", new SQLException(), "uq_namespace_name"));
    doThrow(duplicateName).when(transactionManager).commit(any());

    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(DuplicateCreationException.class)
        .hasCause(duplicateName);
  }

  @Test
  void acquisitionFailurePreventsExecution() {
    CannotCreateTransactionException failure = new CannotCreateTransactionException("foo");
    when(transactionManager.getTransaction(any())).thenThrow(failure);
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of())).isSameAs(failure);
    verify(entityManager, never()).persist(any());
  }

  @Test
  void unexpectedRollbackPreservesCause() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    UnexpectedRollbackException failure = new UnexpectedRollbackException("foo");
    doThrow(failure).when(transactionManager).commit(any());
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of())).isSameAs(failure);
  }

  @Test
  void optimisticCallbackRejectionRollsBackAndPreservesCause() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    OptimisticLockingFailureException failure = new OptimisticLockingFailureException("foo");
    doThrow(failure).when(repository).flush();
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of())).isSameAs(failure);
    verify(transactionManager).rollback(any());
  }

  @Test
  void failedRollbackReportsFailure() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    OptimisticLockingFailureException failure = new OptimisticLockingFailureException("foo");
    doThrow(failure).when(repository).flush();
    TransactionSystemException rollback = new TransactionSystemException("bar", failure);
    doThrow(rollback).when(transactionManager).rollback(any());
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of())).isSameAs(rollback);
    verify(transactionManager).rollback(any());
  }

  @Test
  void translatedDataAccessCommitFailurePreservesCause() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    DataAccessResourceFailureException failure = new DataAccessResourceFailureException("foo");
    doThrow(failure).when(transactionManager).commit(any());
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of())).isSameAs(failure);
  }

  @Test
  void commitFailureWithApplicationExceptionPropagates() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    OptimisticLockingFailureException applicationFailure =
        new OptimisticLockingFailureException("foo");
    TransactionSystemException completionFailure = new TransactionSystemException("bar");
    completionFailure.initApplicationException(applicationFailure);
    doThrow(completionFailure).when(transactionManager).commit(any());
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isSameAs(completionFailure);
  }

  @Test
  void updateCommitFailurePreservesCause() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByNamespaceName("foo"))
        .thenReturn(Optional.of(new NamespaceRow("foo", Map.of())));
    TransactionSystemException failure = new TransactionSystemException("foo");
    doThrow(failure).when(transactionManager).commit(any());
    assertThatThrownBy(
            () ->
                namespacesService.updateNamespaceProperties("foo", Map.of("bar", "one"), Set.of()))
        .isSameAs(failure);
  }

  @Test
  void missingDeleteTargetReportsFailureWhenReadCompletionFails() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByNamespaceName("foo")).thenReturn(Optional.empty());
    TransactionSystemException failure = new TransactionSystemException("foo");
    doThrow(failure).when(transactionManager).commit(any());

    assertThatThrownBy(() -> namespacesService.deleteNamespace("foo")).isSameAs(failure);
    verify(repository, never()).delete(any());
  }

  @Test
  void missingUpdateTargetReportsFailureWhenReadCompletionFails() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByNamespaceName("foo")).thenReturn(Optional.empty());
    TransactionSystemException failure = new TransactionSystemException("foo");
    doThrow(failure).when(transactionManager).commit(any());

    assertThatThrownBy(
            () ->
                namespacesService.updateNamespaceProperties("foo", Map.of("bar", "one"), Set.of()))
        .isSameAs(failure);
    verify(entityManager, never()).lock(any(), any());
  }

  @Test
  void failedPrerequisiteLookupReportsRollbackFailure() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    DataAccessResourceFailureException lookupFailure =
        new DataAccessResourceFailureException("foo");
    when(repository.findByNamespaceName("foo")).thenThrow(lookupFailure);
    TransactionSystemException completionFailure = new TransactionSystemException("bar");
    doThrow(completionFailure).when(transactionManager).rollback(any());

    assertThatThrownBy(() -> namespacesService.deleteNamespace("foo")).isSameAs(completionFailure);
    verify(repository, never()).delete(any());
  }

  @Test
  void optimisticReadRejectionPreservesCause() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    OptimisticLockingFailureException failure = new OptimisticLockingFailureException("foo");
    when(repository.findByNamespaceName("foo")).thenThrow(failure);
    assertThatThrownBy(() -> namespacesService.getNamespace("foo")).isSameAs(failure);
    verify(transactionManager).rollback(any());
  }
}
