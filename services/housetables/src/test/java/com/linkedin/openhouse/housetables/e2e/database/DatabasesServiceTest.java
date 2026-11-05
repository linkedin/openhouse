package com.linkedin.openhouse.housetables.e2e.database;

import static com.linkedin.openhouse.housetables.services.DatabaseMetadataException.Reason.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.doAnswer;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.api.spec.model.DatabaseMetadata;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateDatabasePropertiesResponseBody;
import com.linkedin.openhouse.housetables.model.DatabaseRow;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.DatabaseHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.DatabaseMetadataException;
import com.linkedin.openhouse.housetables.services.DatabasesService;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
class DatabasesServiceTest {
  @Autowired private DatabasesService databasesService;
  @Autowired private DataSource dataSource;
  @SpyBean private DatabaseHtsJdbcRepository repository;

  @AfterEach
  void cleanUp() {
    repository.deleteAll();
    new JdbcTemplate(dataSource)
        .update("DELETE FROM user_table_row WHERE database_id = 'database_metadata_foo'");
  }

  @Test
  void testIndependentIdentityAndExactPropertyKeysPersist() throws DatabaseMetadataException {
    Map<String, String> properties =
        Map.of("foo", "one", "FOO", "two", "foo ", "three", "fóo", "four", "", "five");
    DatabaseMetadata created = databasesService.createDatabase("Database_Foo", properties);
    assertThat(UUID.fromString(created.getDatabaseUuid()).toString())
        .isEqualTo(created.getDatabaseUuid());
    assertThat(created.getDatabaseName()).isEqualTo("Database_Foo");
    assertThat(databasesService.getDatabase("DATABASE_FOO")).isEqualTo(created);
    assertThat(databasesService.getDatabase("database_foo").getProperties())
        .containsExactlyInAnyOrderEntriesOf(properties);
    assertThat(repository.findById(created.getDatabaseUuid()))
        .get()
        .extracting(DatabaseRow::getDatabaseName)
        .isEqualTo("Database_Foo");
    assertThat(
            new JdbcTemplate(dataSource)
                .queryForObject(
                    "SELECT COUNT(*) FROM user_table_row WHERE database_id = 'Database_Foo'",
                    Long.class))
        .isZero();
    assertThatThrownBy(() -> databasesService.createDatabase("dAtAbAsE_fOo", Map.of()))
        .isInstanceOf(DatabaseMetadataException.class)
        .extracting("reason")
        .isEqualTo(ALREADY_EXISTS);
    assertThat(repository.count()).isEqualTo(1);
    assertThat(databasesService.getDatabase("database_foo")).isEqualTo(created);
  }

  @Test
  void testSparseUpdatePreservesIdentityAndOmittedProperties() throws DatabaseMetadataException {
    DatabaseMetadata created =
        databasesService.createDatabase(
            "database_foo", Map.of("foo", "one", "bar", "two", "keep", "three", "FOO", "four"));
    long version = repository.findById(created.getDatabaseUuid()).orElseThrow().getVersion();
    UpdateDatabasePropertiesResponseBody response =
        databasesService.updateDatabaseProperties(
            "DATABASE_FOO", Map.of("foo", "five", "baz", "six"), Set.of("bar", "missing"));
    assertThat(response.getUpdated()).containsExactly("baz", "foo");
    assertThat(response.getRemoved()).containsExactly("bar");
    assertThat(response.getMissing()).containsExactly("missing");
    assertThat(databasesService.getDatabase("database_foo").getDatabaseUuid())
        .isEqualTo(created.getDatabaseUuid());
    assertThat(databasesService.getDatabase("database_foo").getProperties())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("foo", "five", "baz", "six", "keep", "three", "FOO", "four"));
    assertThat(repository.findById(created.getDatabaseUuid()).orElseThrow().getVersion())
        .isGreaterThan(version);
    UpdateDatabasePropertiesResponseBody repeated =
        databasesService.updateDatabaseProperties(
            "database_foo", Map.of("foo", "five"), Set.of("bar"));
    assertThat(repeated.getUpdated()).containsExactly("foo");
    assertThat(repeated.getRemoved()).isEmpty();
    assertThat(repeated.getMissing()).containsExactly("bar");
  }

  @Test
  void testOverlapFailsWithoutMutationAndEmptyUpdateSucceeds() throws DatabaseMetadataException {
    DatabaseMetadata created =
        databasesService.createDatabase("database_foo", Map.of("foo", "one"));
    assertThatThrownBy(
            () ->
                databasesService.updateDatabaseProperties(
                    "database_foo", Map.of("foo", "two"), Set.of("foo")))
        .isInstanceOf(DatabaseMetadataException.class)
        .extracting("reason")
        .isEqualTo(INVALID_UPDATE);
    assertThat(databasesService.getDatabase("database_foo")).isEqualTo(created);
    UpdateDatabasePropertiesResponseBody response =
        databasesService.updateDatabaseProperties("database_foo", Map.of(), Set.of());
    assertThat(response.getUpdated()).isEmpty();
    assertThat(response.getRemoved()).isEmpty();
    assertThat(response.getMissing()).isEmpty();
    assertThat(databasesService.getDatabase("database_foo")).isEqualTo(created);
  }

  @Test
  void testDeletionRemovesOnlyDatabaseMetadata() throws DatabaseMetadataException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    jdbcTemplate.update(
        "INSERT INTO user_table_row "
            + "(database_id, table_id, version, metadata_location, storage_type, entity_type) "
            + "VALUES ('database_metadata_foo', 'bar', 7, '/foo/bar', 'hdfs', 'TABLE')");
    Map<String, Object> tableBefore =
        jdbcTemplate.queryForMap(
            "SELECT * FROM user_table_row WHERE database_id = 'database_metadata_foo'");
    DatabaseMetadata created =
        databasesService.createDatabase("database_metadata_foo", Map.of("foo", "one"));
    databasesService.deleteDatabase("DATABASE_METADATA_FOO");
    assertThat(repository.findById(created.getDatabaseUuid())).isEmpty();
    assertThat(
            jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM database_property WHERE database_uuid = ?",
                Long.class,
                created.getDatabaseUuid()))
        .isZero();
    assertThat(
            jdbcTemplate.queryForMap(
                "SELECT * FROM user_table_row WHERE database_id = 'database_metadata_foo'"))
        .isEqualTo(tableBefore);
    assertThatThrownBy(() -> databasesService.getDatabase("database_metadata_foo"))
        .isInstanceOf(DatabaseMetadataException.class)
        .extracting("reason")
        .isEqualTo(NOT_FOUND);
    assertThatThrownBy(() -> databasesService.deleteDatabase("database_metadata_foo"))
        .isInstanceOf(DatabaseMetadataException.class)
        .extracting("reason")
        .isEqualTo(NOT_FOUND);
    assertThatThrownBy(
            () ->
                databasesService.updateDatabaseProperties(
                    "database_metadata_foo", Map.of("foo", "two"), Set.of()))
        .isInstanceOf(DatabaseMetadataException.class)
        .extracting("reason")
        .isEqualTo(NOT_FOUND);
    assertThat(
            databasesService
                .createDatabase("database_metadata_foo", Collections.emptyMap())
                .getDatabaseUuid())
        .isNotEqualTo(created.getDatabaseUuid());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testConcurrentCollectionMutationsRejectLostUpdate(boolean emptyUpdates) throws Exception {
    DatabaseMetadata created =
        databasesService.createDatabase("database_foo", Map.of("keep", "one"));
    CyclicBarrier readers = new CyclicBarrier(2);
    doAnswer(
            invocation -> {
              Optional<DatabaseRow> database = repository.findById(created.getDatabaseUuid());
              database.orElseThrow().getProperties().size();
              readers.await(10, TimeUnit.SECONDS);
              return database;
            })
        .when(repository)
        .findByDatabaseNameKey("database_foo");
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      List<Callable<Optional<DatabaseMetadataException.Reason>>> mutations =
          Arrays.asList(
              () -> {
                try {
                  databasesService.updateDatabaseProperties(
                      "database_foo", emptyUpdates ? Map.of() : Map.of("foo", "two"), Set.of());
                  return Optional.empty();
                } catch (DatabaseMetadataException exception) {
                  return Optional.of(exception.getReason());
                }
              },
              () -> {
                try {
                  databasesService.updateDatabaseProperties(
                      "database_foo", emptyUpdates ? Map.of() : Map.of("bar", "three"), Set.of());
                  return Optional.empty();
                } catch (DatabaseMetadataException exception) {
                  return Optional.of(exception.getReason());
                }
              });
      List<Future<Optional<DatabaseMetadataException.Reason>>> results =
          executor.invokeAll(mutations, 20, TimeUnit.SECONDS);
      assertThat(Arrays.asList(results.get(0).get(), results.get(1).get()))
          .containsExactlyInAnyOrder(Optional.empty(), Optional.of(CONCURRENT_MODIFICATION));
    } finally {
      executor.shutdownNow();
      assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      org.mockito.Mockito.reset(repository);
    }
    if (emptyUpdates) {
      assertThat(databasesService.getDatabase("database_foo").getProperties())
          .containsExactlyInAnyOrderEntriesOf(Map.of("keep", "one"));
    } else {
      assertThat(databasesService.getDatabase("database_foo").getProperties())
          .isIn(Map.of("keep", "one", "foo", "two"), Map.of("keep", "one", "bar", "three"));
    }
  }
}
