package com.linkedin.openhouse.housetables.mock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.exception.checked.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.checked.InvalidPropertyUpdateException;
import com.linkedin.openhouse.common.exception.checked.NamespaceException;
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
        .isInstanceOf(NamespaceException.class);
    assertThatThrownBy(() -> namespacesService.getNamespace("foo"))
        .isInstanceOf(NamespaceException.class);
    assertThatThrownBy(() -> namespacesService.deleteNamespace("foo"))
        .isInstanceOf(NamespaceException.class);
    assertThatThrownBy(
            () ->
                namespacesService.updateNamespaceProperties("foo", Map.of("bar", "one"), Set.of()))
        .isInstanceOf(NamespaceException.class);
  }

  @Test
  void testRepositoryFailureBecomesCheckedStorageFailure() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByNamespaceName("foo"))
        .thenThrow(new DataAccessResourceFailureException("storage unavailable"));
    assertThatThrownBy(() -> namespacesService.getNamespace("foo"))
        .isInstanceOf(NamespaceException.class)
        .hasMessage("Namespace metadata storage failed.")
        .hasCauseInstanceOf(DataAccessResourceFailureException.class);
    verify(transactionManager).rollback(any());
  }

  @Test
  void testCommitFailureReportsFailure() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    doThrow(new TransactionSystemException("commit failed")).when(transactionManager).commit(any());
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(NamespaceException.class)
        .hasCauseInstanceOf(TransactionSystemException.class);
  }

  @Test
  void testReadCommitFailureBecomesCheckedStorageFailure() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByNamespaceName("foo"))
        .thenReturn(Optional.of(new NamespaceRow("foo", Map.of())));
    TransactionSystemException failure = new TransactionSystemException("commit failed");
    doThrow(failure).when(transactionManager).commit(any());

    assertThatThrownBy(() -> namespacesService.getNamespace("foo"))
        .isInstanceOf(NamespaceException.class)
        .hasMessage("Namespace metadata storage failed.")
        .hasCause(failure);
  }

  @Test
  void testDeleteCommitFailureReportsFailure() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByNamespaceName("foo"))
        .thenReturn(Optional.of(new NamespaceRow("foo", Map.of())));
    TransactionSystemException failure = new TransactionSystemException("commit failed");
    doThrow(failure).when(transactionManager).commit(any());

    assertThatThrownBy(() -> namespacesService.deleteNamespace("foo"))
        .isInstanceOf(NamespaceException.class)
        .hasMessage("Namespace metadata storage failed.")
        .hasCause(failure);
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
        .isInstanceOf(NamespaceException.class)
        .hasMessage("Namespace metadata storage failed.")
        .hasCause(failure);
    verify(transactionManager).rollback(any());
  }

  @Test
  void testOverlappingPropertyUpdatePrecedesUnsupportedBackendRejection() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("ICEBERG");

    assertThatThrownBy(
            () ->
                namespacesService.updateNamespaceProperties(
                    "foo", Map.of("bar", "one"), Set.of("bar")))
        .isInstanceOf(InvalidPropertyUpdateException.class)
        .hasMessage("Property updates and removals must have disjoint keys.");
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
        .isInstanceOf(AlreadyExistsException.class)
        .hasCause(duplicateName);
    DataIntegrityViolationException otherConstraint =
        new DataIntegrityViolationException(
            "constraint failed",
            new ConstraintViolationException(
                "constraint failed", new SQLException(), "other_constraint"));
    doThrow(otherConstraint).when(repository).flush();
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(NamespaceException.class)
        .hasCause(otherConstraint);
    verify(transactionManager, times(2)).rollback(any());
  }

  @Test
  void acquisitionFailurePreventsExecution() {
    CannotCreateTransactionException failure = new CannotCreateTransactionException("foo");
    when(transactionManager.getTransaction(any())).thenThrow(failure);
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(NamespaceException.class)
        .hasCause(failure);
    verify(entityManager, never()).persist(any());
  }

  @Test
  void unexpectedRollbackPreservesCause() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    UnexpectedRollbackException failure = new UnexpectedRollbackException("foo");
    doThrow(failure).when(transactionManager).commit(any());
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(NamespaceException.class)
        .hasCause(failure);
  }

  @Test
  void optimisticCallbackRejectionRollsBackAndPreservesCause() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    OptimisticLockingFailureException failure = new OptimisticLockingFailureException("foo");
    doThrow(failure).when(repository).flush();
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(NamespaceException.class)
        .hasCause(failure);
    verify(transactionManager).rollback(any());
  }

  @Test
  void failedRollbackPreservesCallbackCause() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    OptimisticLockingFailureException failure = new OptimisticLockingFailureException("foo");
    doThrow(failure).when(repository).flush();
    TransactionSystemException rollback = new TransactionSystemException("bar", failure);
    doThrow(rollback).when(transactionManager).rollback(any());
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(NamespaceException.class)
        .hasCause(rollback)
        .satisfies(exception -> assertThat(exception.getCause().getSuppressed()).contains(failure));
    verify(transactionManager).rollback(any());
  }

  @Test
  void translatedDataAccessCommitFailurePreservesCause() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    DataAccessResourceFailureException failure = new DataAccessResourceFailureException("foo");
    doThrow(failure).when(transactionManager).commit(any());
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(NamespaceException.class)
        .hasCause(failure);
  }

  @Test
  void nestedOptimisticCommitFailurePreservesApplicationCause() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    OptimisticLockingFailureException applicationFailure =
        new OptimisticLockingFailureException("foo");
    TransactionSystemException completionFailure = new TransactionSystemException("bar");
    completionFailure.initApplicationException(applicationFailure);
    doThrow(completionFailure).when(transactionManager).commit(any());
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(NamespaceException.class)
        .hasCause(completionFailure);
    assertThat(completionFailure.getSuppressed()).contains(applicationFailure);
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
        .isInstanceOf(NamespaceException.class)
        .hasCause(failure);
  }

  @Test
  void missingDeleteTargetReportsFailureWhenReadCompletionFails() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByNamespaceName("foo")).thenReturn(Optional.empty());
    TransactionSystemException failure = new TransactionSystemException("foo");
    doThrow(failure).when(transactionManager).commit(any());

    assertThatThrownBy(() -> namespacesService.deleteNamespace("foo"))
        .isInstanceOf(NamespaceException.class)
        .hasCause(failure);
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
        .isInstanceOf(NamespaceException.class)
        .hasCause(failure);
    verify(entityManager, never()).lock(any(), any());
  }

  @Test
  void failedPrerequisiteLookupPreservesCauseWhenRollbackFails() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    DataAccessResourceFailureException lookupFailure =
        new DataAccessResourceFailureException("foo");
    when(repository.findByNamespaceName("foo")).thenThrow(lookupFailure);
    TransactionSystemException completionFailure = new TransactionSystemException("bar");
    doThrow(completionFailure).when(transactionManager).rollback(any());

    assertThatThrownBy(() -> namespacesService.deleteNamespace("foo"))
        .isInstanceOf(NamespaceException.class)
        .hasCause(completionFailure);
    assertThat(completionFailure.getSuppressed()).contains(lookupFailure);
    verify(repository, never()).delete(any());
  }

  @Test
  void optimisticReadRejectionPreservesCause() {
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    OptimisticLockingFailureException failure = new OptimisticLockingFailureException("foo");
    when(repository.findByNamespaceName("foo")).thenThrow(failure);
    assertThatThrownBy(() -> namespacesService.getNamespace("foo"))
        .isInstanceOf(NamespaceException.class)
        .hasCause(failure);
    verify(transactionManager).rollback(any());
  }
}
