package com.linkedin.openhouse.housetables.e2e.namespace;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.model.NamespaceMetadata;
import com.linkedin.openhouse.housetables.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.housetables.model.NamespaceRow;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.NamespaceHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.InvalidNamespacePropertyUpdateException;
import com.linkedin.openhouse.housetables.services.NamespaceAlreadyExistsException;
import com.linkedin.openhouse.housetables.services.NamespaceConcurrentModificationException;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataException;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataStorageException;
import com.linkedin.openhouse.housetables.services.NamespaceNotFoundException;
import com.linkedin.openhouse.housetables.services.NamespacesService;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
class NamespacesServiceTest {
  @Autowired private NamespacesService namespacesService;
  @Autowired private DataSource dataSource;
  @SpyBean private NamespaceHtsJdbcRepository repository;

  @AfterEach
  void cleanUp() {
    repository.deleteAll();
    new JdbcTemplate(dataSource)
        .update("DELETE FROM user_table_row WHERE database_id = 'namespace_metadata_foo'");
  }

  @Test
  void testIndependentIdentityAndExactPropertyKeysPersist() throws NamespaceMetadataException {
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
        .isInstanceOf(NamespaceAlreadyExistsException.class);
    assertThat(repository.count()).isEqualTo(1);
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(created);
  }

  @Test
  void testSparseUpdatePreservesIdentityAndOmittedProperties() throws NamespaceMetadataException {
    NamespaceMetadata created =
        namespacesService.createNamespace(
            "Namespace_Foo", Map.of("foo", "one", "bar", "two", "keep", "three", "FOO", "four"));
    long version = repository.findById(created.getNamespaceUuid()).orElseThrow().getVersion();
    NamespacePropertyUpdate response =
        namespacesService.updateNamespaceProperties(
            "NAMESPACE_FOO", Map.of("foo", "five", "baz", "six"), Set.of("bar", "missing"));
    assertThat(response.getUpdated()).containsExactly("baz", "foo");
    assertThat(response.getRemoved()).containsExactly("bar");
    assertThat(response.getMissing()).containsExactly("missing");
    assertThat(namespacesService.getNamespace("namespace_foo").getNamespaceUuid())
        .isEqualTo(created.getNamespaceUuid());
    assertThat(namespacesService.getNamespace("namespace_foo").getNamespaceName())
        .isEqualTo("Namespace_Foo");
    assertThat(namespacesService.getNamespace("namespace_foo").getProperties())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("foo", "five", "baz", "six", "keep", "three", "FOO", "four"));
    assertThat(repository.findById(created.getNamespaceUuid()).orElseThrow().getVersion())
        .isGreaterThan(version);
    NamespacePropertyUpdate repeated =
        namespacesService.updateNamespaceProperties(
            "namespace_foo", Map.of("foo", "five"), Set.of("bar"));
    assertThat(repeated.getUpdated()).containsExactly("foo");
    assertThat(repeated.getRemoved()).isEmpty();
    assertThat(repeated.getMissing()).containsExactly("bar");
  }

  @Test
  void testOverlapFailsWithoutMutation() throws NamespaceMetadataException {
    NamespaceMetadata created =
        namespacesService.createNamespace("namespace_foo", Map.of("foo", "one"));
    assertThatThrownBy(
            () ->
                namespacesService.updateNamespaceProperties(
                    "namespace_foo", Map.of("foo", "two"), Set.of("foo")))
        .isInstanceOf(InvalidNamespacePropertyUpdateException.class);
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(created);
  }

  @Test
  void testEmptyUpdatePreservesMetadata() throws NamespaceMetadataException {
    NamespaceMetadata created =
        namespacesService.createNamespace("namespace_foo", Map.of("foo", "one"));
    NamespacePropertyUpdate response =
        namespacesService.updateNamespaceProperties("namespace_foo", Map.of(), Set.of());
    assertThat(response.getUpdated()).isEmpty();
    assertThat(response.getRemoved()).isEmpty();
    assertThat(response.getMissing()).isEmpty();
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(created);
  }

  @Test
  void testEmptyDocumentPersistsAcrossPropertyChanges() throws NamespaceMetadataException {
    NamespaceMetadata created = namespacesService.createNamespace("namespace_foo", Map.of());
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(created);
    namespacesService.updateNamespaceProperties("namespace_foo", Map.of("foo", "one"), Set.of());
    assertThat(namespacesService.getNamespace("namespace_foo").getProperties())
        .containsExactlyInAnyOrderEntriesOf(Map.of("foo", "one"));
    namespacesService.updateNamespaceProperties("namespace_foo", Map.of(), Set.of("foo"));
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(created);
  }

  @Test
  void testDeletionRemovesOnlyNamespaceMetadata() throws NamespaceMetadataException {
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
        .isInstanceOf(NamespaceNotFoundException.class);
    assertThatThrownBy(() -> namespacesService.deleteNamespace("namespace_metadata_foo"))
        .isInstanceOf(NamespaceNotFoundException.class);
    assertThatThrownBy(
            () ->
                namespacesService.updateNamespaceProperties(
                    "namespace_metadata_foo", Map.of("foo", "two"), Set.of()))
        .isInstanceOf(NamespaceNotFoundException.class);
    assertThat(
            namespacesService
                .createNamespace("namespace_metadata_foo", Collections.emptyMap())
                .getNamespaceUuid())
        .isNotEqualTo(created.getNamespaceUuid());
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
                            } catch (NamespaceAlreadyExistsException exception) {
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
                      .isInstanceOf(NamespaceConcurrentModificationException.class));
      assertThat(deleteLoaded.await(10, TimeUnit.SECONDS)).isTrue();
      namespacesService.updateNamespaceProperties(
          "namespace_metadata_foo", Map.of("foo", "two", "bar", "three"), Set.of());
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
  void testStorageFailureRollsBackSparseChanges() throws NamespaceMetadataException {
    NamespaceMetadata created =
        namespacesService.createNamespace("namespace_foo", Map.of("foo", "one", "bar", "two"));
    doThrow(new DataAccessResourceFailureException("storage unavailable")).when(repository).flush();
    assertThatThrownBy(
            () ->
                namespacesService.updateNamespaceProperties(
                    "namespace_foo", Map.of("baz", "three"), Set.of("foo")))
        .isInstanceOf(NamespaceMetadataStorageException.class);
    reset(repository);
    assertThat(namespacesService.getNamespace("namespace_foo")).isEqualTo(created);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testConcurrentDocumentMutationsRejectLostUpdate(boolean emptyUpdates) throws Exception {
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
    try {
      List<Callable<Optional<Class<? extends NamespaceMetadataException>>>> mutations =
          Arrays.asList(
              () -> {
                try {
                  namespacesService.updateNamespaceProperties(
                      "namespace_foo", emptyUpdates ? Map.of() : Map.of("foo", "two"), Set.of());
                  return Optional.empty();
                } catch (NamespaceMetadataException exception) {
                  return Optional.of(exception.getClass());
                }
              },
              () -> {
                try {
                  namespacesService.updateNamespaceProperties(
                      "namespace_foo", emptyUpdates ? Map.of() : Map.of("bar", "three"), Set.of());
                  return Optional.empty();
                } catch (NamespaceMetadataException exception) {
                  return Optional.of(exception.getClass());
                }
              });
      List<Future<Optional<Class<? extends NamespaceMetadataException>>>> results =
          executor.invokeAll(mutations, 20, TimeUnit.SECONDS);
      assertThat(Arrays.asList(results.get(0).get(), results.get(1).get()))
          .containsExactlyInAnyOrder(
              Optional.empty(), Optional.of(NamespaceConcurrentModificationException.class));
    } finally {
      executor.shutdownNow();
      assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      reset(repository);
    }
    if (emptyUpdates) {
      assertThat(namespacesService.getNamespace("namespace_foo").getProperties())
          .containsExactlyInAnyOrderEntriesOf(Map.of("keep", "one"));
    } else {
      assertThat(namespacesService.getNamespace("namespace_foo").getProperties())
          .isIn(Map.of("keep", "one", "foo", "two"), Map.of("keep", "one", "bar", "three"));
    }
  }
}
