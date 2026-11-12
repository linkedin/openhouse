package com.linkedin.openhouse.housetables.mock;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.NamespaceHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.NamespaceAlreadyExistsException;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataStorageException;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataUnavailableException;
import com.linkedin.openhouse.housetables.services.NamespacesService;
import com.linkedin.openhouse.housetables.services.NamespacesServiceImpl;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import javax.persistence.EntityManager;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionSystemException;
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
        .isInstanceOf(NamespaceMetadataUnavailableException.class);
    assertThatThrownBy(() -> namespacesService.getNamespace("foo"))
        .isInstanceOf(NamespaceMetadataUnavailableException.class);
    assertThatThrownBy(() -> namespacesService.deleteNamespace("foo"))
        .isInstanceOf(NamespaceMetadataUnavailableException.class);
    assertThatThrownBy(
            () ->
                namespacesService.updateNamespaceProperties("foo", Map.of("bar", "one"), Set.of()))
        .isInstanceOf(NamespaceMetadataUnavailableException.class);
  }

  @Test
  void testRepositoryFailureBecomesCheckedStorageFailure() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByNamespaceName("foo"))
        .thenThrow(new DataAccessResourceFailureException("storage unavailable"));
    assertThatThrownBy(() -> namespacesService.getNamespace("foo"))
        .isInstanceOf(NamespaceMetadataStorageException.class)
        .hasMessage("Namespace metadata storage failed.")
        .hasCauseInstanceOf(DataAccessResourceFailureException.class);
    verify(transactionManager).rollback(any());
  }

  @Test
  void testCommitFailureBecomesCheckedStorageFailure() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    doThrow(new TransactionSystemException("commit failed")).when(transactionManager).commit(any());
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(NamespaceMetadataStorageException.class)
        .hasCauseInstanceOf(TransactionSystemException.class);
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
        .isInstanceOf(NamespaceAlreadyExistsException.class)
        .hasCause(duplicateName);
    DataIntegrityViolationException otherConstraint =
        new DataIntegrityViolationException(
            "constraint failed",
            new ConstraintViolationException(
                "constraint failed", new SQLException(), "other_constraint"));
    doThrow(otherConstraint).when(repository).flush();
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(NamespaceMetadataStorageException.class)
        .hasCause(otherConstraint);
    verify(transactionManager, times(2)).rollback(any());
  }
}
