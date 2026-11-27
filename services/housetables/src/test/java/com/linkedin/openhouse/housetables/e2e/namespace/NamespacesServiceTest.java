package com.linkedin.openhouse.housetables.e2e.namespace;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.e2e.HtsDatabaseTestBase;
import com.linkedin.openhouse.housetables.model.NamespaceMetadata;
import com.linkedin.openhouse.housetables.model.NamespaceRow;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.NamespaceHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.NamespacesService;
import com.linkedin.openhouse.housetables.services.NamespacesServiceImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.DefaultTransactionStatus;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
class NamespacesServiceTest extends HtsDatabaseTestBase {
  @Autowired private NamespacesService namespacesService;
  @Autowired private DataSource dataSource;
  @Autowired private PlatformTransactionManager transactionManager;
  @Autowired private EntityManager entityManager;
  @SpyBean private NamespaceHtsJdbcRepository repository;
  @SpyBean private ClusterProperties clusterProperties;

  @AfterEach
  void cleanUp() {
    repository.deleteAll();
    new JdbcTemplate(dataSource)
        .update("DELETE FROM user_table_row WHERE database_id = 'namespace_metadata_foo'");
  }

  @Test
  void testIndependentIdentityAndExactPropertyKeysPersist() throws Exception {
    Map<String, String> properties =
        Map.of(
            "foo",
            "one",
            "FOO",
            "\ud800",
            "foo ",
            "three ",
            "fóo",
            "\"four\"\n",
            "",
            "",
            "😀",
            "😀");
    NamespaceMetadata created = namespacesService.createNamespace("Namespace_Foo", properties);
    assertThat(UUID.fromString(created.getNamespaceUuid()).toString())
        .isEqualTo(created.getNamespaceUuid());
    assertThat(created.getNamespaceName()).isEqualTo("Namespace_Foo");
    assertThat(namespacesService.getNamespace("NAMESPACE_FOO")).isEqualTo(created);
    assertThat(namespacesService.getNamespace("namespace_foo").getProperties())
        .containsExactlyInAnyOrderEntriesOf(properties);
    assertThat(repository.findById(created.getNamespaceUuid()))
        .get()
        .extracting(NamespaceRow::getNamespaceName)
        .isEqualTo("Namespace_Foo");
    assertThat(
            new JdbcTemplate(dataSource)
                .queryForObject(
                    "SELECT COUNT(*) FROM user_table_row WHERE UPPER(database_id) = 'NAMESPACE_FOO'",
                    Long.class))
        .isZero();
    assertThatThrownBy(() -> namespacesService.createNamespace("nAmEsPaCe_fOo", Map.of()))
        .isInstanceOf(DuplicateCreationException.class);
    assertThat(repository.count()).isEqualTo(1);
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(created);
  }

  @Test
  void testReplacementPreservesIdentityAndRejectsStaleRevision() throws Exception {
    NamespaceMetadata created =
        namespacesService.createNamespace(
            "Namespace_Foo", Map.of("foo", "one", "bar", "two", "keep", "three", "FOO", "four"));
    long version = repository.findById(created.getNamespaceUuid()).orElseThrow().getVersion();
    NamespaceMetadata response =
        namespacesService.updateNamespace(
            "NAMESPACE_FOO",
            document(created, Map.of("foo", "five", "baz", "six", "keep", "three", "FOO", "four")));
    assertThat(response.getRevision()).isNotEqualTo(created.getRevision());
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(response);
    assertThat(namespacesService.getNamespace("namespace_foo").getNamespaceUuid())
        .isEqualTo(created.getNamespaceUuid());
    assertThat(namespacesService.getNamespace("namespace_foo").getNamespaceName())
        .isEqualTo("Namespace_Foo");
    assertThat(namespacesService.getNamespace("namespace_foo").getProperties())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("foo", "five", "baz", "six", "keep", "three", "FOO", "four"));
    assertThat(repository.findById(created.getNamespaceUuid()).orElseThrow().getVersion())
        .isGreaterThan(version);
    assertThatThrownBy(() -> namespacesService.updateNamespace("namespace_foo", created))
        .isInstanceOf(EntityConcurrentModificationException.class);
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(response);
  }

  @Test
  void testEmptyUpdatePreservesMetadata() throws Exception {
    NamespaceMetadata created =
        namespacesService.createNamespace("namespace_foo", Map.of("foo", "one"));
    NamespaceMetadata response = namespacesService.updateNamespace("namespace_foo", created);
    assertThat(response.getProperties()).isEqualTo(created.getProperties());
    assertThat(response.getRevision()).isNotEqualTo(created.getRevision());
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(response);
    assertThatThrownBy(() -> namespacesService.updateNamespace("namespace_foo", created))
        .isInstanceOf(EntityConcurrentModificationException.class);
  }

  @Test
  void testEmptyDocumentPersistsAcrossPropertyChanges() throws Exception {
    NamespaceMetadata created = namespacesService.createNamespace("namespace_foo", Map.of());
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(created);
    NamespaceMetadata updated =
        namespacesService.updateNamespace("namespace_foo", document(created, Map.of("foo", "one")));
    assertThat(namespacesService.getNamespace("namespace_foo").getProperties())
        .containsExactlyInAnyOrderEntriesOf(Map.of("foo", "one"));
    NamespaceMetadata empty =
        namespacesService.updateNamespace("namespace_foo", document(updated, Map.of()));
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(empty);
    assertThat(empty.getProperties()).isEmpty();
  }

  @Test
  void testDeletionRemovesOnlyNamespaceMetadata() throws Exception {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    jdbcTemplate.update(
        "INSERT INTO user_table_row "
            + "(database_id, table_id, version, metadata_location, storage_type, entity_type) "
            + "VALUES ('namespace_metadata_foo', 'bar', 7, '/foo/bar', 'hdfs', 'TABLE')");
    Map<String, Object> tableBefore =
        jdbcTemplate.queryForMap(
            "SELECT * FROM user_table_row WHERE database_id = 'namespace_metadata_foo'");
    NamespaceMetadata created =
        namespacesService.createNamespace("Namespace_Metadata_Foo", Map.of("foo", "one"));
    namespacesService.deleteNamespace("NAMESPACE_METADATA_FOO");
    assertThat(repository.findById(created.getNamespaceUuid())).isEmpty();
    assertThat(
            jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM catalog_namespaces WHERE namespace_uuid = ?",
                Long.class,
                created.getNamespaceUuid()))
        .isZero();
    assertThat(
            jdbcTemplate.queryForMap(
                "SELECT * FROM user_table_row WHERE database_id = 'namespace_metadata_foo'"))
        .isEqualTo(tableBefore);
    assertThatThrownBy(() -> namespacesService.getNamespace("namespace_metadata_foo"))
        .isInstanceOf(NotFoundException.class);
    assertThatThrownBy(() -> namespacesService.deleteNamespace("namespace_metadata_foo"))
        .isInstanceOf(NotFoundException.class);
    assertThatThrownBy(
            () ->
                namespacesService.updateNamespace(
                    "namespace_metadata_foo", document(created, Map.of("foo", "two"))))
        .isInstanceOf(NotFoundException.class);
    NamespaceMetadata recreated =
        namespacesService.createNamespace("namespace_metadata_foo", Collections.emptyMap());
    assertThat(recreated.getNamespaceUuid()).isNotEqualTo(created.getNamespaceUuid());
    assertThatThrownBy(() -> namespacesService.updateNamespace("namespace_metadata_foo", created))
        .isInstanceOf(EntityConcurrentModificationException.class);
    assertThat(namespacesService.getNamespace("namespace_metadata_foo")).isEqualTo(recreated);
  }

  @Test
  void testConcurrentCaseVariantsCreateOneIdentity() throws Exception {
    CyclicBarrier start = new CyclicBarrier(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      List<Callable<Optional<NamespaceMetadata>>> creations =
          Arrays.asList("Namespace_Foo", "NAMESPACE_FOO").stream()
              .map(
                  name ->
                      (Callable<Optional<NamespaceMetadata>>)
                          () -> {
                            start.await(10, TimeUnit.SECONDS);
                            try {
                              return Optional.of(namespacesService.createNamespace(name, Map.of()));
                            } catch (DuplicateCreationException exception) {
                              return Optional.empty();
                            }
                          })
              .collect(Collectors.toList());
      List<Future<Optional<NamespaceMetadata>>> results =
          executor.invokeAll(creations, 20, TimeUnit.SECONDS);
      Optional<NamespaceMetadata> first = results.get(0).get();
      Optional<NamespaceMetadata> second = results.get(1).get();
      assertThat(Arrays.asList(first.isPresent(), second.isPresent()))
          .containsExactlyInAnyOrder(true, false);
      NamespaceMetadata stored = namespacesService.getNamespace("namespace_foo");
      assertThat(stored).isEqualTo(first.orElseGet(second::get));
      assertThat(stored.getNamespaceName()).isIn("Namespace_Foo", "NAMESPACE_FOO");
      assertThat(repository.count()).isEqualTo(1);
    } finally {
      executor.shutdownNow();
      assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void testStaleDeleteRollsBackPropertyRemoval() throws Exception {
    NamespaceMetadata created =
        namespacesService.createNamespace("namespace_metadata_foo", Map.of("foo", "one"));
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    jdbcTemplate.update(
        "INSERT INTO user_table_row "
            + "(database_id, table_id, version, metadata_location, storage_type, entity_type) "
            + "VALUES ('namespace_metadata_foo', 'bar', 7, '/foo/bar', 'hdfs', 'TABLE')");
    Map<String, Object> tableBefore =
        jdbcTemplate.queryForMap(
            "SELECT * FROM user_table_row WHERE database_id = 'namespace_metadata_foo'");
    CountDownLatch deleteLoaded = new CountDownLatch(1);
    CountDownLatch updateCommitted = new CountDownLatch(1);
    AtomicBoolean firstRead = new AtomicBoolean(true);
    doAnswer(
            invocation -> {
              Optional<NamespaceRow> namespace = repository.findById(created.getNamespaceUuid());
              namespace.orElseThrow().getProperties().size();
              if (firstRead.getAndSet(false)) {
                deleteLoaded.countDown();
                assertThat(updateCommitted.await(10, TimeUnit.SECONDS)).isTrue();
              }
              return namespace;
            })
        .when(repository)
        .findByNamespaceName("namespace_metadata_foo");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> deletion =
          executor.submit(
              () ->
                  assertThatThrownBy(
                          () -> namespacesService.deleteNamespace("namespace_metadata_foo"))
                      .isInstanceOf(EntityConcurrentModificationException.class));
      assertThat(deleteLoaded.await(10, TimeUnit.SECONDS)).isTrue();
      namespacesService.updateNamespace(
          "namespace_metadata_foo", document(created, Map.of("foo", "two", "bar", "three")));
      updateCommitted.countDown();
      deletion.get(10, TimeUnit.SECONDS);
    } finally {
      updateCommitted.countDown();
      executor.shutdownNow();
      assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      reset(repository);
    }
    assertThat(namespacesService.getNamespace("namespace_metadata_foo").getNamespaceUuid())
        .isEqualTo(created.getNamespaceUuid());
    assertThat(namespacesService.getNamespace("namespace_metadata_foo").getProperties())
        .containsExactlyInAnyOrderEntriesOf(Map.of("foo", "two", "bar", "three"));
    assertThat(
            jdbcTemplate.queryForMap(
                "SELECT * FROM user_table_row WHERE database_id = 'namespace_metadata_foo'"))
        .isEqualTo(tableBefore);
  }

  @Test
  void testUnsupportedBackendRejectionsPreserveStoredMetadata() throws Exception {
    NamespaceMetadata created =
        namespacesService.createNamespace("Namespace_Foo", Map.of("foo", "one"));
    doReturn("ICEBERG").when(clusterProperties).getClusterHouseTablesDatabaseType();
    try {
      assertThatThrownBy(() -> namespacesService.createNamespace("Namespace_Bar", Map.of()))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(
              () ->
                  namespacesService.updateNamespace(
                      "namespace_foo", document(created, Map.of("foo", "two"))))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> namespacesService.deleteNamespace("namespace_foo"))
          .isInstanceOf(IllegalStateException.class);
    } finally {
      reset(clusterProperties);
    }
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(created);
    assertThat(repository.findByNamespaceName("Namespace_Bar")).isEmpty();
    assertThat(repository.count()).isEqualTo(1);
  }

  @Test
  void testStorageFailureRollsBackSparseChanges() throws Exception {
    assertThat(transactionManager).isInstanceOf(JpaTransactionManager.class);
    NamespaceMetadata created =
        namespacesService.createNamespace("namespace_foo", Map.of("foo", "one", "bar", "two"));
    doThrow(new DataAccessResourceFailureException("storage unavailable")).when(repository).flush();
    assertThatThrownBy(
            () ->
                namespacesService.updateNamespace(
                    "namespace_foo", document(created, Map.of("baz", "three"))))
        .isInstanceOf(DataAccessResourceFailureException.class);
    reset(repository);
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(created);
  }

  @Test
  void testStorageFailureRollsBackDeletion() throws Exception {
    NamespaceMetadata created =
        namespacesService.createNamespace("namespace_foo", Map.of("foo", "one"));
    doThrow(new DataAccessResourceFailureException("storage unavailable")).when(repository).flush();

    assertThatThrownBy(() -> namespacesService.deleteNamespace("namespace_foo"))
        .isInstanceOf(DataAccessResourceFailureException.class);

    reset(repository);
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(created);
  }

  @Test
  void committedJpaMutationWithLostAcknowledgementReportsFailure() throws Exception {
    assertThat(transactionManager).isInstanceOf(JpaTransactionManager.class);
    TransactionSystemException failure = new TransactionSystemException("foo");
    JpaTransactionManager failingCompletion =
        new JpaTransactionManager(
            ((JpaTransactionManager) transactionManager).getEntityManagerFactory()) {
          @Override
          protected void doCommit(DefaultTransactionStatus status) {
            super.doCommit(status);
            throw failure;
          }
        };
    NamespacesService service =
        new NamespacesServiceImpl(repository, clusterProperties, entityManager, failingCompletion);
    assertThatThrownBy(() -> service.createNamespace("namespace_foo", Map.of("foo", "bar")))
        .isSameAs(failure);
    assertThat(namespacesService.getNamespace("namespace_foo").getProperties())
        .containsExactlyInAnyOrderEntriesOf(Map.of("foo", "bar"));
  }

  @ParameterizedTest
  @CsvSource({"false,false", "true,true", "true,false", "false,true"})
  void testConcurrentDocumentMutationsRejectLostUpdate(boolean firstNoOp, boolean secondNoOp)
      throws Exception {
    NamespaceMetadata created =
        namespacesService.createNamespace("namespace_foo", Map.of("keep", "one"));
    CyclicBarrier readers = new CyclicBarrier(2);
    doAnswer(
            invocation -> {
              Optional<NamespaceRow> namespace = repository.findById(created.getNamespaceUuid());
              namespace.orElseThrow().getProperties().size();
              readers.await(10, TimeUnit.SECONDS);
              return namespace;
            })
        .when(repository)
        .findByNamespaceName("namespace_foo");
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Optional<Class<? extends RuntimeException>> firstOutcome;
    Optional<Class<? extends RuntimeException>> secondOutcome;
    try {
      List<Callable<Optional<Class<? extends RuntimeException>>>> mutations =
          Arrays.asList(
              () -> {
                try {
                  namespacesService.updateNamespace(
                      "namespace_foo",
                      document(
                          created,
                          firstNoOp ? Map.of("keep", "one") : Map.of("keep", "one", "foo", "two")));
                  return Optional.empty();
                } catch (EntityConcurrentModificationException exception) {
                  return Optional.of(exception.getClass());
                }
              },
              () -> {
                try {
                  namespacesService.updateNamespace(
                      "namespace_foo",
                      document(
                          created,
                          secondNoOp
                              ? Map.of("keep", "one")
                              : Map.of("keep", "one", "bar", "three")));
                  return Optional.empty();
                } catch (EntityConcurrentModificationException exception) {
                  return Optional.of(exception.getClass());
                }
              });
      List<Future<Optional<Class<? extends RuntimeException>>>> results =
          executor.invokeAll(mutations, 20, TimeUnit.SECONDS);
      firstOutcome = results.get(0).get();
      secondOutcome = results.get(1).get();
      assertThat(Arrays.asList(firstOutcome.isPresent(), secondOutcome.isPresent()))
          .containsExactlyInAnyOrder(false, true);
    } finally {
      executor.shutdownNow();
      assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      reset(repository);
    }
    Map<String, String> expected =
        firstOutcome.isEmpty()
            ? (firstNoOp ? Map.of("keep", "one") : Map.of("keep", "one", "foo", "two"))
            : (secondNoOp ? Map.of("keep", "one") : Map.of("keep", "one", "bar", "three"));
    assertThat(namespacesService.getNamespace("namespace_foo").getProperties())
        .containsExactlyInAnyOrderEntriesOf(expected);
  }

  private static NamespaceMetadata document(
      NamespaceMetadata source, Map<String, String> properties) {
    return NamespaceMetadata.builder()
        .namespaceUuid(source.getNamespaceUuid())
        .namespaceName(source.getNamespaceName())
        .revision(source.getRevision())
        .properties(properties)
        .build();
  }
}
