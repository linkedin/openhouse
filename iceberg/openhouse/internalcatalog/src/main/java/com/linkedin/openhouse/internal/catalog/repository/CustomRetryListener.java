package com.linkedin.openhouse.internal.catalog.repository;

import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;

/** Used as an observer in testing the retry behavior. */
public class CustomRetryListener implements RetryListener {

  private int retryCount = 0;

  @Override
  public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
    return true;
  }

  @Override
  public <T, E extends Throwable> void close(
      RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {}

  @Override
  public <T, E extends Throwable> void onError(
      RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
    retryCount++;
  }

  public int getRetryCount() {
    return retryCount;
  }
}
