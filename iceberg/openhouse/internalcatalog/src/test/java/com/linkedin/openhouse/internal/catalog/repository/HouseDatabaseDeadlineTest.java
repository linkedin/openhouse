package com.linkedin.openhouse.internal.catalog.repository;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.housetables.client.api.DatabaseApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException.Reason;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

class HouseDatabaseDeadlineTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void pendingRequestsExpireAtTheirInstalledDeadline(boolean mutation) throws Exception {
    VirtualTimeScheduler clock = VirtualTimeScheduler.getOrSet();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch subscribed = new CountDownLatch(1);
    AtomicInteger subscriptions = new AtomicInteger();
    AtomicInteger cancellations = new AtomicInteger();
    AtomicReference<Thread> worker = new AtomicReference<>();
    DatabaseApi databaseApi = mock(DatabaseApi.class);
    Mono<Void> pending =
        Mono.<Void>never()
            .doOnSubscribe(
                subscription -> {
                  subscriptions.incrementAndGet();
                  subscribed.countDown();
                })
            .doOnCancel(cancellations::incrementAndGet);
    when(databaseApi.deleteDatabaseWithHttpInfo("foo")).thenReturn(pending.then(Mono.empty()));
    when(databaseApi.getDatabase("foo")).thenReturn(pending.then(Mono.empty()));
    HouseDatabaseRepository repository =
        new HouseDatabaseRepositoryImpl(databaseApi, mock(UserTableApi.class));
    try {
      Future<HouseDatabaseException> failure =
          executor.submit(
              () ->
                  assertThrows(
                      HouseDatabaseException.class,
                      () -> {
                        worker.set(Thread.currentThread());
                        if (mutation) {
                          repository.delete("foo");
                        } else {
                          repository.findByName("foo");
                        }
                      }));
      assertTrue(subscribed.await(10, TimeUnit.SECONDS));
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(
              () ->
                  clock.getScheduledTaskCount() == 1
                      && worker.get().getState() == Thread.State.WAITING);
      clock.advanceTimeBy(Duration.ofSeconds(mutation ? 59 : 29));
      assertFalse(failure.isDone());
      clock.advanceTimeBy(Duration.ofSeconds(1));
      assertEquals(
          mutation ? Reason.UNKNOWN_OUTCOME : Reason.UNAVAILABLE,
          failure.get(10, TimeUnit.SECONDS).getReason());
      assertEquals(1, subscriptions.get());
      assertEquals(1, cancellations.get());
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      VirtualTimeScheduler.reset();
    }
  }
}
