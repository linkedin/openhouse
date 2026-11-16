package com.linkedin.openhouse.javaclient;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.relocated.org.springframework.http.HttpStatus;
import com.linkedin.openhouse.relocated.org.springframework.http.ResponseEntity;
import com.linkedin.openhouse.relocated.reactor.core.publisher.Mono;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.exceptions.RESTException;
import org.junit.jupiter.api.Test;

class NamespaceCancellationTest {
  @Test
  void cancelledMutationPreservesCause() {
    CancellationException cause = new CancellationException("foo");
    RESTException failure =
        assertThrows(
            RESTException.class,
            () ->
                OpenHouseCatalog.executeNamespaceRequest(
                    Mono.error(cause),
                    HttpStatus.OK,
                    Duration.ofSeconds(30),
                    OpenHouseCatalog.NamespaceOperation.UPDATE));
    assertSame(cause, failure.getCause());
  }

  @Test
  void pendingRequestIsCancelledWhenItsBudgetExpires() {
    AtomicInteger subscriptions = new AtomicInteger();
    AtomicBoolean cancelled = new AtomicBoolean();
    Mono<ResponseEntity<Void>> pending =
        Mono.<ResponseEntity<Void>>never()
            .doOnSubscribe(subscription -> subscriptions.incrementAndGet())
            .doOnCancel(() -> cancelled.set(true));
    RESTException failure =
        assertThrows(
            RESTException.class,
            () ->
                OpenHouseCatalog.executeNamespaceRequest(
                    pending,
                    HttpStatus.OK,
                    Duration.ZERO,
                    OpenHouseCatalog.NamespaceOperation.LOAD));
    assertInstanceOf(TimeoutException.class, failure.getCause());
    assertEquals(1, subscriptions.get());
    assertTrue(cancelled.get());
  }

  @Test
  void mutationTimeoutCancelsRequestAndPreservesCause() {
    AtomicBoolean cancelled = new AtomicBoolean();
    Mono<ResponseEntity<Void>> pending =
        Mono.<ResponseEntity<Void>>never().doOnCancel(() -> cancelled.set(true));
    RESTException failure =
        assertThrows(
            RESTException.class,
            () ->
                OpenHouseCatalog.executeNamespaceRequest(
                    pending,
                    HttpStatus.OK,
                    Duration.ZERO,
                    OpenHouseCatalog.NamespaceOperation.UPDATE));
    assertInstanceOf(TimeoutException.class, failure.getCause());
    assertTrue(cancelled.get());
  }

  @Test
  void mutationInterruptionCancelsPendingRequestAndRestoresWorkerFlag() throws Exception {
    CountDownLatch subscribed = new CountDownLatch(1);
    AtomicInteger subscriptions = new AtomicInteger();
    AtomicBoolean cancelled = new AtomicBoolean();
    AtomicBoolean interrupted = new AtomicBoolean();
    AtomicReference<RESTException> failure = new AtomicReference<>();
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
                OpenHouseCatalog.executeNamespaceRequest(
                    pending,
                    HttpStatus.OK,
                    Duration.ofSeconds(30),
                    OpenHouseCatalog.NamespaceOperation.UPDATE);
              } catch (RESTException exception) {
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
