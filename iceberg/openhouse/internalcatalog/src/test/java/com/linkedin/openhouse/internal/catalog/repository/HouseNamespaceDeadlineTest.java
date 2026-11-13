package com.linkedin.openhouse.internal.catalog.repository;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.exception.checked.MutationUncertainException;
import com.linkedin.openhouse.common.exception.checked.NamespaceException;
import com.linkedin.openhouse.common.exception.checked.UnavailableException;
import com.linkedin.openhouse.housetables.client.api.NamespaceApi;
import com.linkedin.openhouse.housetables.client.api.UserTableApi;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

class HouseNamespaceDeadlineTest {
  @Test
  void readDeadlineCancelsUnavailableRequest() throws Exception {
    NamespaceApi namespaceApi = mock(NamespaceApi.class);
    HouseNamespaceRepository repository =
        new HouseNamespaceRepositoryImpl(namespaceApi, mock(UserTableApi.class));
    assertDeadline(namespaceApi, () -> repository.getByName("foo"), 30, UnavailableException.class);
  }

  @Test
  void mutationDeadlineCancelsWithUncertainOutcome() throws Exception {
    NamespaceApi namespaceApi = mock(NamespaceApi.class);
    HouseNamespaceRepository repository =
        new HouseNamespaceRepositoryImpl(namespaceApi, mock(UserTableApi.class));
    assertDeadline(
        namespaceApi, () -> repository.delete("foo"), 60, MutationUncertainException.class);
  }

  private void assertDeadline(
      NamespaceApi namespaceApi,
      Executable operation,
      int timeoutSeconds,
      Class<? extends NamespaceException> failureType)
      throws Exception {
    VirtualTimeScheduler clock = VirtualTimeScheduler.getOrSet();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch subscribed = new CountDownLatch(1);
    AtomicInteger subscriptions = new AtomicInteger();
    AtomicInteger cancellations = new AtomicInteger();
    AtomicReference<Thread> worker = new AtomicReference<>();
    Mono<Void> pending =
        Mono.<Void>never()
            .doOnSubscribe(
                subscription -> {
                  subscriptions.incrementAndGet();
                  subscribed.countDown();
                })
            .doOnCancel(cancellations::incrementAndGet);
    when(namespaceApi.deleteNamespaceWithHttpInfo("foo")).thenReturn(pending.then(Mono.empty()));
    when(namespaceApi.getNamespace("foo")).thenReturn(pending.then(Mono.empty()));
    try {
      Future<NamespaceException> failure =
          executor.submit(
              () ->
                  assertThrows(
                      NamespaceException.class,
                      () -> {
                        worker.set(Thread.currentThread());
                        operation.execute();
                      }));
      assertTrue(subscribed.await(10, TimeUnit.SECONDS));
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(
              () ->
                  clock.getScheduledTaskCount() == 1
                      && worker.get().getState() == Thread.State.WAITING);
      clock.advanceTimeBy(Duration.ofSeconds(timeoutSeconds - 1));
      assertFalse(failure.isDone());
      clock.advanceTimeBy(Duration.ofSeconds(1));
      assertEquals(failureType, failure.get(10, TimeUnit.SECONDS).getClass());
      assertEquals(1, subscriptions.get());
      assertEquals(1, cancellations.get());
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      VirtualTimeScheduler.reset();
    }
  }
}
