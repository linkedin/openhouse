package com.linkedin.openhouse.catalog.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Local Spark-driven repro of the concurrent-insert snapshot loss observed in production. Mirrors
 * {@code TablesConcurrencyTest.concurrentInsertTest} from the LinkedIn acceptance suite but runs
 * fully in-process against the embedded OpenHouse server.
 *
 * <p>Two futures fire {@code INSERT INTO} statements concurrently against the same fresh table. If
 * the bug is present, OpenHouse's subtractive-merge commit path drops one writer's snapshot when
 * the catalog version advances mid-flight, and the row-count assertion at the end fails.
 */
@Slf4j
public class SparkConcurrentInsertFunctionalTest extends OpenHouseSparkITest {

  private static final String TABLE_NAME = "openhouse.db.concurrent_insert";
  private static final int RECORDS_PER_FUTURE = 5;
  private static final int TOTAL_RECORDS = RECORDS_PER_FUTURE * 2;

  private SparkSession spark;
  private ExecutorService executor;

  @BeforeEach
  public void setUp() throws Exception {
    spark = getSparkSession();
    executor = Executors.newFixedThreadPool(2);
    spark.sql("DROP TABLE IF EXISTS " + TABLE_NAME);
    spark.sql("CREATE TABLE " + TABLE_NAME + " (op string, value int)");
  }

  @AfterEach
  public void tearDown() {
    if (executor != null) {
      executor.shutdownNow();
    }
    if (spark != null) {
      spark.sql("DROP TABLE IF EXISTS " + TABLE_NAME);
    }
  }

  @Test
  public void testConcurrentInsertRaceDropsCommit() throws Exception {
    AtomicInteger committedRecord = new AtomicInteger(TOTAL_RECORDS);
    CountDownLatch readyLatch = new CountDownLatch(2);
    CountDownLatch startLatch = new CountDownLatch(1);

    Future<?> insertFuture1 =
        executor.submit(insertTask("insert1", readyLatch, startLatch, committedRecord));
    Future<?> insertFuture2 =
        executor.submit(insertTask("insert2", readyLatch, startLatch, committedRecord));

    readyLatch.await();
    startLatch.countDown();

    insertFuture1.get(5, TimeUnit.MINUTES);
    insertFuture2.get(5, TimeUnit.MINUTES);

    long rows = spark.sql("SELECT * FROM " + TABLE_NAME).count();
    log.info("rows committed (catalog) = {}, expected = {}", rows, committedRecord.get());
    assertThat(rows)
        .as(
            "All %d inserts returned 200 (no client exception decremented counter to %d), "
                + "so the table must contain that many rows. Mismatch => snapshot lost in catalog "
                + "subtractive merge.",
            TOTAL_RECORDS, committedRecord.get())
        .isEqualTo(committedRecord.get());
  }

  private Runnable insertTask(
      String label,
      CountDownLatch readyLatch,
      CountDownLatch startLatch,
      AtomicInteger committedRecord) {
    return () -> {
      readyLatch.countDown();
      try {
        startLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      for (int i = 1; i <= RECORDS_PER_FUTURE; i++) {
        try {
          spark.sql(String.format("INSERT INTO %s VALUES ('%s', %d)", TABLE_NAME, label, i));
        } catch (Exception e) {
          committedRecord.getAndDecrement();
          log.warn("insert failed for {} iter={}", label, i, e);
        }
      }
    };
  }
}
