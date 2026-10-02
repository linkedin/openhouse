package com.linkedin.openhouse.spark.e2e.extensions;

import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.javaclient.exception.WebClientRequestWithMessageException;
import com.linkedin.openhouse.spark.SparkTestBase;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class UnlockStatementTest {

  @Test
  public void testUnlockRequestFailure() {
    mockTableService.setDispatcher(
        new Dispatcher() {
          @NotNull
          @Override
          public MockResponse dispatch(@NotNull RecordedRequest request) {
            return new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST);
          }
        });
    WebClientRequestWithMessageException failure =
        Assertions.assertThrows(
            WebClientRequestWithMessageException.class,
            () -> spark.sql("ALTER TABLE openhouse.dunlock.t1 UNLOCK"));
    Assertions.assertTrue(failure.getMessage().contains("Connection prematurely closed"));
  }
}
