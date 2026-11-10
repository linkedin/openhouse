package com.linkedin.openhouse.housetables.mock;

import static com.linkedin.openhouse.housetables.services.NamespaceMetadataException.Reason.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.NamespaceHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataException;
import com.linkedin.openhouse.housetables.services.NamespacesService;
import com.linkedin.openhouse.housetables.services.NamespacesServiceImpl;
import java.util.Map;
import java.util.Set;
import javax.persistence.EntityManager;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
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
  void testIcebergRejectsEveryOperationBeforeAccessingJdbc() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("ICEBERG");
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOfSatisfying(
            NamespaceMetadataException.class,
            exception -> assertThat(exception.getReason()).isEqualTo(UNAVAILABLE));
    assertThatThrownBy(() -> namespacesService.getNamespace("foo"))
        .isInstanceOfSatisfying(
            NamespaceMetadataException.class,
            exception -> assertThat(exception.getReason()).isEqualTo(UNAVAILABLE));
    assertThatThrownBy(() -> namespacesService.deleteNamespace("foo"))
        .isInstanceOfSatisfying(
            NamespaceMetadataException.class,
            exception -> assertThat(exception.getReason()).isEqualTo(UNAVAILABLE));
    assertThatThrownBy(
            () ->
                namespacesService.updateNamespaceProperties("foo", Map.of("bar", "one"), Set.of()))
        .isInstanceOfSatisfying(
            NamespaceMetadataException.class,
            exception -> assertThat(exception.getReason()).isEqualTo(UNAVAILABLE));
    verifyNoInteractions(repository, entityManager, transactionManager);
  }

  @Test
  void testRepositoryFailureBecomesCheckedStorageFailure() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByNamespaceName("foo"))
        .thenThrow(new DataAccessResourceFailureException("storage unavailable"));
    assertThatThrownBy(() -> namespacesService.getNamespace("foo"))
        .isInstanceOf(NamespaceMetadataException.class)
        .hasMessage("Namespace metadata storage failed.")
        .hasCauseInstanceOf(DataAccessResourceFailureException.class)
        .isInstanceOfSatisfying(
            NamespaceMetadataException.class,
            exception -> assertThat(exception.getReason()).isEqualTo(STORAGE_FAILURE));
    verify(transactionManager).rollback(any());
  }

  @Test
  void testCommitFailureBecomesCheckedStorageFailure() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    doThrow(new TransactionSystemException("commit failed")).when(transactionManager).commit(any());
    assertThatThrownBy(() -> namespacesService.createNamespace("foo", Map.of()))
        .isInstanceOf(NamespaceMetadataException.class)
        .hasCauseInstanceOf(TransactionSystemException.class)
        .isInstanceOfSatisfying(
            NamespaceMetadataException.class,
            exception -> assertThat(exception.getReason()).isEqualTo(STORAGE_FAILURE));
  }
}
