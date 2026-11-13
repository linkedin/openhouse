package com.linkedin.openhouse.housetables.e2e.namespace;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.common.exception.checked.NamespaceException;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.NamespacesService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.persistence.EntityManagerFactory;
import javax.persistence.OptimisticLockException;
import javax.sql.DataSource;
import org.hibernate.SessionFactory;
import org.hibernate.resource.jdbc.spi.StatementInspector;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(
    properties = {
      "spring.jpa.properties.hibernate.generate_statistics=true",
      "spring.jpa.properties.hibernate.session_factory.statement_inspector="
          + "com.linkedin.openhouse.housetables.e2e.namespace.NamespaceMysqlWorkloadTest$SqlCapture",
      "logging.level.org.hibernate.engine.internal.StatisticalLoggingSessionEventListener=OFF"
    })
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@EnabledIfSystemProperty(named = "namespace.workload", matches = "true")
class NamespaceMysqlWorkloadTest {
  @Autowired private NamespacesService namespaces;
  @Autowired private UserTableHtsJdbcRepository tables;
  @Autowired private EntityManagerFactory entityManagerFactory;
  @Autowired private DataSource dataSource;

  public static class SqlCapture implements StatementInspector {
    private static final Set<String> STATEMENTS = ConcurrentHashMap.newKeySet();

    @Override
    public String inspect(String sql) {
      STATEMENTS.add(sql);
      return sql;
    }
  }

  @Test
  void sparseUpdatesCommitProgress() throws Exception {
    assertThat(new JdbcTemplate(dataSource).queryForObject("SELECT VERSION()", String.class))
        .startsWith("8.0.");
    Statistics statistics = entityManagerFactory.unwrap(SessionFactory.class).getStatistics();
    // Configurations mutate persistent fixtures and share transaction statistics sequentially.
    for (int propertyCount : new int[] {10, 100, 1000}) {
      namespaces.createNamespace(
          "workload_" + propertyCount,
          IntStream.range(0, propertyCount)
              .boxed()
              .collect(Collectors.toMap(index -> "key_" + index, index -> "value")));
      for (int writers : new int[] {1, 4}) {
        AtomicInteger sequence = new AtomicInteger();
        runWorkload(
            "sparse_" + propertyCount,
            writers,
            statistics,
            () -> {
              namespaces.updateNamespaceProperties(
                  "workload_" + propertyCount,
                  Map.of("key_0", "value_" + sequence.incrementAndGet()),
                  Set.of());
              return true;
            });
        assertThat(namespaces.getNamespace("workload_" + propertyCount).getProperties())
            .hasSize(propertyCount)
            .containsEntry("key_1", "value");
      }
    }
    SqlCapture.STATEMENTS.stream()
        .sorted()
        .forEach(sql -> System.out.println("SQL_CAPTURE " + sql));
  }

  @Test
  void occupancyReads() throws Exception {
    assertThat(new JdbcTemplate(dataSource).queryForObject("SELECT VERSION()", String.class))
        .startsWith("8.0.");
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    jdbcTemplate.batchUpdate(
        "INSERT INTO user_table_row (database_id, table_id, version, storage_type, entity_type) "
            + "VALUES (?, ?, 1, 'hdfs', 'VIEW')",
        IntStream.range(0, 30000)
            .mapToObj(
                index ->
                    new Object[] {
                      index < 10000 ? "workload_views" : "workload_other", "table_" + index
                    })
            .collect(Collectors.toList()));
    jdbcTemplate.execute("ANALYZE TABLE user_table_row");
    Statistics statistics = entityManagerFactory.unwrap(SessionFactory.class).getStatistics();
    for (int readers : new int[] {1, 4}) {
      double measuredP95Millis =
          runWorkload(
              "occupancy_views_10000",
              readers,
              statistics,
              () -> {
                assertThat(
                        tables
                            .findAllTablesByFilters(
                                "workload_views",
                                null,
                                null,
                                null,
                                null,
                                null,
                                PageRequest.of(0, 1, Sort.by("tableId")))
                            .getContent())
                    .isEmpty();
                assertThat(
                        tables
                            .findAllViewsByFilters(
                                "workload_views",
                                null,
                                null,
                                null,
                                null,
                                null,
                                PageRequest.of(0, 1, Sort.by("tableId")))
                            .getContent())
                    .hasSize(1);
                return true;
              });
      System.out.printf("OCCUPANCY_P95,%d,%.3f%n", readers, measuredP95Millis);
      if (Boolean.getBoolean("namespace.workload.regression")) {
        double baselineP95Millis =
            Double.parseDouble(System.getProperty("namespace.occupancy.baseline." + readers));
        double maximumRegressionPercent =
            Double.parseDouble(System.getProperty("namespace.occupancy.maxRegressionPercent"));
        double maximumP95Millis = baselineP95Millis * (1 + maximumRegressionPercent / 100);
        System.out.printf(
            "OCCUPANCY_COMPARISON,%d,%.3f,%.3f,%.3f%n",
            readers, baselineP95Millis, measuredP95Millis, maximumP95Millis);
        assertThat(measuredP95Millis)
            .as("Median warmed p95 latency in milliseconds with %s readers", readers)
            .isLessThanOrEqualTo(maximumP95Millis);
      }
    }
    SqlCapture.STATEMENTS.stream()
        .sorted()
        .forEach(sql -> System.out.println("SQL_CAPTURE " + sql));
  }

  private double runWorkload(
      String name, int concurrency, Statistics statistics, Callable<Boolean> operation)
      throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    List<Double> measuredP95Millis = new ArrayList<>();
    try {
      // Run zero warms the same transaction and query paths used by the measured runs.
      for (int run = 0; run <= 3; run++) {
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        List<Long> durations = Collections.synchronizedList(new ArrayList<>());
        List<Callable<Boolean>> requests =
            IntStream.range(0, 100)
                .mapToObj(
                    index ->
                        (Callable<Boolean>)
                            () -> {
                              long started = System.nanoTime();
                              try {
                                operation.call();
                                successes.incrementAndGet();
                              } catch (NamespaceException exception) {
                                assertThat(exception.getCause())
                                    .isInstanceOfAny(
                                        OptimisticLockException.class,
                                        OptimisticLockingFailureException.class);
                                conflicts.incrementAndGet();
                              } finally {
                                durations.add(System.nanoTime() - started);
                              }
                              return true;
                            })
                .collect(Collectors.toList());
        statistics.clear();
        long started = System.nanoTime();
        List<Future<Boolean>> results = executor.invokeAll(requests, 120, TimeUnit.SECONDS);
        // Resolve every checked task failure before observing aggregate statistics.
        for (Future<Boolean> result : results) {
          assertThat(result.get()).isTrue();
        }
        double seconds = (System.nanoTime() - started) / 1e9;
        List<Long> sorted = durations.stream().sorted().collect(Collectors.toList());
        assertThat(successes.get() + conflicts.get()).isEqualTo(100);
        assertThat(successes.get())
            .as("Successful operations for %s with concurrency %s, run %s", name, concurrency, run)
            .isPositive();
        if (concurrency == 1) {
          assertThat(conflicts.get()).isZero();
          if (name.startsWith("sparse")) {
            // Each request loads the document, writes it, and advances the optimistic version.
            assertThat(statistics.getPrepareStatementCount()).isEqualTo(300);
          }
        }
        if (name.startsWith("occupancy")) {
          assertThat(statistics.getPrepareStatementCount()).isEqualTo(300);
        }
        System.out.printf(
            "WORKLOAD,%s,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f%n",
            name,
            concurrency,
            run,
            successes.get(),
            conflicts.get(),
            statistics.getPrepareStatementCount(),
            sorted.get(49) / 1e6,
            sorted.get(94) / 1e6,
            100 / seconds,
            successes.get() / seconds);
        if (run > 0) {
          measuredP95Millis.add(sorted.get(94) / 1e6);
        }
      }
      return measuredP95Millis.stream().sorted().collect(Collectors.toList()).get(1);
    } finally {
      executor.shutdownNow();
      assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }
}
