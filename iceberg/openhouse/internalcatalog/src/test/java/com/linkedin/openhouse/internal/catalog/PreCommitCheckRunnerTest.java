package com.linkedin.openhouse.internal.catalog;

import com.linkedin.openhouse.common.exception.CommitRejectedException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.annotation.Order;

class PreCommitCheckRunnerTest {

  private static final TableIdentifier TABLE = TableIdentifier.of("db", "tbl");

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
          .withBean(PreCommitCheckRunner.class);

  /**
   * A deployment without checks, such as OSS, still starts, and its runner does nothing: it does
   * not even build the context, which would fail on these null properties.
   */
  @Test
  void withoutChecksTheRunnerStartsAndDoesNothing() {
    contextRunner.run(
        context -> {
          Assertions.assertNull(context.getStartupFailure());
          context.getBean(PreCommitCheckRunner.class).checkAll(TABLE, null, null, null);
        });
  }

  /**
   * Checks run in {@code @Order}, not registration order. The first rejection reaches the caller
   * unchanged and stops the rest; each outcome is counted under the check's name.
   */
  @Test
  void checksRunInOrderAndTheFirstRejectionStopsTheRest() {
    contextRunner
        .withBean(Calls.class)
        .withBean(Third.class)
        .withBean(Rejecting.class)
        .withBean(First.class)
        .run(
            context -> {
              Assertions.assertNull(context.getStartupFailure());
              CommitRejectedException thrown =
                  Assertions.assertThrows(
                      CommitRejectedException.class,
                      () ->
                          context
                              .getBean(PreCommitCheckRunner.class)
                              .checkAll(TABLE, null, null, Collections.emptyMap()));
              Assertions.assertSame(Rejecting.REJECTION, thrown);
              Assertions.assertEquals(
                  Arrays.asList("first", "rejecting"), context.getBean(Calls.class).names);

              MeterRegistry meters = context.getBean(MeterRegistry.class);
              Assertions.assertEquals(1, count(meters, "first", "passed"));
              Assertions.assertEquals(1, count(meters, "rejecting", "rejected"));
              Assertions.assertNull(
                  meters.find(PreCommitCheckRunner.METRIC_PREFIX).tag("name", "third").counter());
            });
  }

  /**
   * A check that throws {@link CommitFailedException} would make Iceberg retry a rejection that
   * fails the same way every time; the runner reports it as a bug instead.
   */
  @Test
  void aCheckCannotMakeIcebergRetry() {
    CommitFailedException misuse = new CommitFailedException("not a rejection");
    MeterRegistry meters = new SimpleMeterRegistry();
    PreCommitCheckRunner runner =
        new PreCommitCheckRunner(
            Collections.singletonList(
                new Named("retrying") {
                  @Override
                  public void check(PreCommitContext context) {
                    throw misuse;
                  }
                }),
            meters);

    IllegalStateException thrown =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> runner.checkAll(TABLE, null, null, Collections.emptyMap()));
    Assertions.assertSame(misuse, thrown.getCause());
    Assertions.assertEquals(1, count(meters, "retrying", "error"));
  }

  private static double count(MeterRegistry meters, String name, String result) {
    return meters
        .get(PreCommitCheckRunner.METRIC_PREFIX)
        .tag(PreCommitCheckRunner.TAG_NAME, name)
        .tag(PreCommitCheckRunner.TAG_RESULT, result)
        .counter()
        .count();
  }

  /** The names of the checks that ran, in the order they ran. */
  static class Calls {
    final List<String> names = new ArrayList<>();
  }

  abstract static class Named implements PreCommitCheck {
    private final String name;

    Named(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }
  }

  abstract static class Recording extends Named {
    private final Calls calls;

    Recording(String name, Calls calls) {
      super(name);
      this.calls = calls;
    }

    @Override
    public void check(PreCommitContext context) {
      calls.names.add(getName());
    }
  }

  @Order(1)
  static class First extends Recording {
    First(Calls calls) {
      super("first", calls);
    }
  }

  @Order(2)
  static class Rejecting extends Recording {
    static final CommitRejectedException REJECTION = new CommitRejectedException("rejected");

    Rejecting(Calls calls) {
      super("rejecting", calls);
    }

    @Override
    public void check(PreCommitContext context) {
      super.check(context);
      throw REJECTION;
    }
  }

  @Order(3)
  static class Third extends Recording {
    Third(Calls calls) {
      super("third", calls);
    }
  }
}
