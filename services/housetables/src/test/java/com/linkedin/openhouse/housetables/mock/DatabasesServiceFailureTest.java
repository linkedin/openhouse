package com.linkedin.openhouse.housetables.mock;

import static com.linkedin.openhouse.housetables.services.DatabaseMetadataException.Reason.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.DatabaseHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.DatabaseMetadataException;
import com.linkedin.openhouse.housetables.services.DatabasesService;
import com.linkedin.openhouse.housetables.services.DatabasesServiceImpl;
import java.util.Map;
import java.util.Set;
import javax.persistence.EntityManager;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.SimpleTransactionStatus;

class DatabasesServiceFailureTest {
  private final DatabaseHtsJdbcRepository repository = mock(DatabaseHtsJdbcRepository.class);
  private final ClusterProperties clusterProperties = mock(ClusterProperties.class);
  private final EntityManager entityManager = mock(EntityManager.class);
  private final PlatformTransactionManager transactionManager =
      mock(PlatformTransactionManager.class);
  private final DatabasesService databasesService =
      new DatabasesServiceImpl(repository, clusterProperties, entityManager, transactionManager);

  @Test
  void testIcebergRejectsEveryOperationBeforeAccessingJdbc() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("ICEBERG");
    assertThatThrownBy(() -> databasesService.createDatabase("foo", Map.of()))
        .isInstanceOfSatisfying(
            DatabaseMetadataException.class,
            exception -> assertThat(exception.getReason()).isEqualTo(UNAVAILABLE));
    assertThatThrownBy(() -> databasesService.getDatabase("foo"))
        .isInstanceOfSatisfying(
            DatabaseMetadataException.class,
            exception -> assertThat(exception.getReason()).isEqualTo(UNAVAILABLE));
    assertThatThrownBy(() -> databasesService.deleteDatabase("foo"))
        .isInstanceOfSatisfying(
            DatabaseMetadataException.class,
            exception -> assertThat(exception.getReason()).isEqualTo(UNAVAILABLE));
    assertThatThrownBy(
            () -> databasesService.updateDatabaseProperties("foo", Map.of("bar", "one"), Set.of()))
        .isInstanceOfSatisfying(
            DatabaseMetadataException.class,
            exception -> assertThat(exception.getReason()).isEqualTo(UNAVAILABLE));
    verifyNoInteractions(repository, entityManager, transactionManager);
  }

  @Test
  void testRepositoryFailureBecomesCheckedStorageFailure() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    when(repository.findByDatabaseNameKey("foo"))
        .thenThrow(new DataAccessResourceFailureException("storage unavailable"));
    assertThatThrownBy(() -> databasesService.getDatabase("foo"))
        .isInstanceOf(DatabaseMetadataException.class)
        .hasMessage("Database metadata storage failed.")
        .hasCauseInstanceOf(DataAccessResourceFailureException.class)
        .isInstanceOfSatisfying(
            DatabaseMetadataException.class,
            exception -> assertThat(exception.getReason()).isEqualTo(STORAGE_FAILURE));
    verify(transactionManager).rollback(any());
  }

  @Test
  void testCommitFailureBecomesCheckedStorageFailure() {
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    doThrow(new TransactionSystemException("commit failed")).when(transactionManager).commit(any());
    assertThatThrownBy(() -> databasesService.createDatabase("foo", Map.of()))
        .isInstanceOf(DatabaseMetadataException.class)
        .hasCauseInstanceOf(TransactionSystemException.class)
        .isInstanceOfSatisfying(
            DatabaseMetadataException.class,
            exception -> assertThat(exception.getReason()).isEqualTo(STORAGE_FAILURE));
  }
}
