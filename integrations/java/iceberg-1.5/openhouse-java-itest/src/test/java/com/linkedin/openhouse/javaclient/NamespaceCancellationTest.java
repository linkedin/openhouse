package com.linkedin.openhouse.javaclient;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.javaclient.exception.CatalogOperationException;
import com.linkedin.openhouse.relocated.org.springframework.http.ResponseEntity;
import com.linkedin.openhouse.relocated.reactor.core.publisher.Mono;
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
  void cancelledMutationPreservesCheckedFailureAndCause() {
    CancellationException cause = new CancellationException("foo");
    CatalogOperationException failure =
        assertThrows(
            CatalogOperationException.class,
            () ->
                NamespaceMetadataClient.request(
                    Mono.error(cause), 200, 30, NamespaceMetadataClient.Operation.UPDATE));
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
    CatalogOperationException failure =
        assertThrows(
            CatalogOperationException.class,
            () ->
                NamespaceMetadataClient.request(
                    pending, 200, 0, NamespaceMetadataClient.Operation.LOAD));
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
                NamespaceCatalogAdapter.execute(
                    () ->
                        NamespaceMetadataClient.request(
                            pending, 200, 0, NamespaceMetadataClient.Operation.UPDATE)));
    assertInstanceOf(CatalogOperationException.class, failure.getCause());
    assertInstanceOf(TimeoutException.class, failure.getCause().getCause());
    assertTrue(cancelled.get());
  }

  @Test
  void mutationInterruptionCancelsPendingRequestAndRestoresWorkerFlag() throws Exception {
    CountDownLatch subscribed = new CountDownLatch(1);
    AtomicInteger subscriptions = new AtomicInteger();
    AtomicBoolean cancelled = new AtomicBoolean();
    AtomicBoolean interrupted = new AtomicBoolean();
    AtomicReference<CatalogOperationException> failure = new AtomicReference<>();
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
                NamespaceMetadataClient.request(
                    pending, 200, 30, NamespaceMetadataClient.Operation.UPDATE);
              } catch (CatalogOperationException exception) {
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
