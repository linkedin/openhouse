package com.linkedin.openhouse.javaclient;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataException;
import com.linkedin.openhouse.relocated.org.springframework.http.ResponseEntity;
import com.linkedin.openhouse.relocated.reactor.core.publisher.Mono;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class NamespaceCancellationTest {
  @Test
  void pendingRequestIsCancelledWhenItsBudgetExpires() {
    AtomicInteger subscriptions = new AtomicInteger();
    AtomicBoolean cancelled = new AtomicBoolean();
    Mono<ResponseEntity<Void>> pending =
        Mono.<ResponseEntity<Void>>never()
            .doOnSubscribe(subscription -> subscriptions.incrementAndGet())
            .doOnCancel(() -> cancelled.set(true));
    NamespaceMetadataException failure =
        assertThrows(
            NamespaceMetadataException.class,
            () -> NamespaceMetadataClient.request(pending, 200, 0));
    assertEquals(0, failure.getStatusCode());
    assertInstanceOf(TimeoutException.class, failure.getCause());
    assertEquals(1, subscriptions.get());
    assertTrue(cancelled.get());
  }

  @Test
  void interruptionCancelsPendingRequestAndRestoresWorkerFlag() throws Exception {
    CountDownLatch subscribed = new CountDownLatch(1);
    AtomicInteger subscriptions = new AtomicInteger();
    AtomicBoolean cancelled = new AtomicBoolean();
    AtomicBoolean interrupted = new AtomicBoolean();
    AtomicReference<NamespaceMetadataException> failure = new AtomicReference<>();
    Mono<ResponseEntity<Void>> pending =
        Mono.<ResponseEntity<Void>>never()
            .doOnSubscribe(
                subscription -> {
                  subscriptions.incrementAndGet();
                  subscribed.countDown();
                })
            .doOnCancel(() -> cancelled.set(true));
    Thread worker =
        new Thread(
            () -> {
              try {
                NamespaceMetadataClient.request(pending, 200, 30);
              } catch (NamespaceMetadataException exception) {
                failure.set(exception);
                interrupted.set(Thread.currentThread().isInterrupted());
              }
            });
    worker.start();
    try {
      assertTrue(subscribed.await(10, TimeUnit.SECONDS));
      worker.interrupt();
      worker.join(10000);
      assertFalse(worker.isAlive());
      assertNotNull(failure.get());
      assertEquals(0, failure.get().getStatusCode());
      assertInstanceOf(InterruptedException.class, failure.get().getCause());
      assertTrue(interrupted.get());
      assertTrue(cancelled.get());
      assertEquals(1, subscriptions.get());
    } finally {
      worker.interrupt();
      worker.join(10000);
    }
  }
}
