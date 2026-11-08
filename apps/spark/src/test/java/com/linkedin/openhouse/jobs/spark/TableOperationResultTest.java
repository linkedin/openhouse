package com.linkedin.openhouse.jobs.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TableOperationResultTest {

  @Test
  void success_hasNoFailureReason() {
    TableOperationResult result = TableOperationResult.success();

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.failureReason()).isEmpty();
  }

  @Test
  void failure_capturesExceptionClassAndMessage() {
    TableOperationResult result =
        TableOperationResult.failure(new OutOfMemoryError("Java heap space"));

    assertThat(result.isSuccess()).isFalse();
    assertThat(result.failureReason()).hasValue("java.lang.OutOfMemoryError: Java heap space");
  }

  @Test
  void failure_withoutMessage_capturesClassOnly() {
    TableOperationResult result = TableOperationResult.failure(new IllegalStateException());

    assertThat(result.failureReason()).hasValue("java.lang.IllegalStateException");
  }

  @Test
  void failure_truncatesReasonToColumnBound() {
    String longMessage = "x".repeat(TableOperationResult.MAX_FAILURE_REASON_LENGTH + 100);

    TableOperationResult result = TableOperationResult.failure(new RuntimeException(longMessage));

    assertThat(result.failureReason().orElseThrow().length())
        .isEqualTo(TableOperationResult.MAX_FAILURE_REASON_LENGTH);
  }
}
