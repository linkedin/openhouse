package com.linkedin.openhouse.housetables.mock.repository;

import static com.linkedin.openhouse.housetables.repository.impl.jdbc.CorruptEntityTypeTranslation.findCorruptEntityTypeCause;
import static com.linkedin.openhouse.housetables.repository.impl.jdbc.CorruptEntityTypeTranslation.translating;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linkedin.openhouse.common.exception.CorruptEntityTypeException;
import javax.persistence.PersistenceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.orm.jpa.JpaSystemException;

/**
 * The whole of the ORM containment. Corruption raised by the converter must reach the shared advice
 * as itself, and nothing else may be disturbed on the way past.
 */
public class CorruptEntityTypeTranslationTest {

  private static final CorruptEntityTypeException CORRUPTION =
      new CorruptEntityTypeException(
          "Column user_table_row.entity_type holds unrecognized value ['UNKNOWN']",
          new IllegalArgumentException("UNKNOWN"));

  @Test
  public void testAResultPassesThroughUntouched() {
    Assertions.assertEquals("read", translating(() -> "read"));
  }

  /** Unwrapped, so the advice renders the converter's diagnostic rather than the wrapper's. */
  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  public void testCorruptionIsUnwrappedFromAnyDepthInTheChain(int depth) {
    Throwable wrapped = CORRUPTION;
    for (int i = 1; i < depth; i++) {
      wrapped = new PersistenceException("layer " + i, wrapped);
    }

    Throwable failure = new JpaSystemException(new PersistenceException("outer", wrapped));

    assertThatThrownBy(
            () ->
                translating(
                    () -> {
                      throw (RuntimeException) failure;
                    }))
        .isSameAs(CORRUPTION);
  }

  /** The other wrapper the translator can meet, given the converter escape's ancestry. */
  @Test
  public void testInvalidDataAccessApiUsageWrapperIsAlsoUnwrapped() {
    InvalidDataAccessApiUsageException wrapper =
        new InvalidDataAccessApiUsageException("usage", CORRUPTION);

    assertThatThrownBy(
            () ->
                translating(
                    () -> {
                      throw wrapper;
                    }))
        .isSameAs(CORRUPTION);
  }

  /**
   * A dependency outage must stay distinguishable from bad data. Rethrown as the very same
   * instance, so it reaches the advice exactly as it does on any untouched path.
   */
  @Test
  public void testAnUnrelatedFailureIsRethrownAsTheSameInstance() {
    DataAccessResourceFailureException failure =
        new DataAccessResourceFailureException("datasource down");

    assertThatThrownBy(
            () ->
                translating(
                    () -> {
                      throw failure;
                    }))
        .isSameAs(failure)
        .isNotInstanceOf(CorruptEntityTypeException.class);
  }

  @Test
  public void testANonDataAccessRuntimeFailureIsRethrownAsTheSameInstance() {
    IllegalStateException failure = new IllegalStateException("unrelated");

    assertThatThrownBy(
            () ->
                translating(
                    () -> {
                      throw failure;
                    }))
        .isSameAs(failure);
  }

  @Test
  public void testNullChainYieldsNoCorruption() {
    assertThat(findCorruptEntityTypeCause(null)).isEmpty();
  }

  @Test
  public void testUnrelatedChainYieldsNoCorruption() {
    assertThat(findCorruptEntityTypeCause(new JpaSystemException(new PersistenceException("x"))))
        .isEmpty();
  }

  /** A cause that is its own cause must terminate rather than spin. */
  @Test
  public void testSelfReferentialChainTerminates() {
    Throwable selfReferential =
        new RuntimeException("loop") {
          @Override
          public synchronized Throwable getCause() {
            return this;
          }
        };

    assertThat(findCorruptEntityTypeCause(selfReferential)).isEmpty();
  }

  /** Two exceptions each naming the other: identity tracking, not depth alone, ends this one. */
  @Test
  public void testMutuallyReferentialChainTerminates() {
    Throwable[] pair = new Throwable[2];
    pair[0] =
        new RuntimeException("a") {
          @Override
          public synchronized Throwable getCause() {
            return pair[1];
          }
        };
    pair[1] =
        new RuntimeException("b") {
          @Override
          public synchronized Throwable getCause() {
            return pair[0];
          }
        };

    assertThat(findCorruptEntityTypeCause(pair[0])).isEmpty();
  }

  /**
   * The depth bound is a stop, not a search: corruption buried deeper than it is reported absent,
   * which is the deliberate trade for terminating on a hostile chain.
   */
  @Test
  public void testCorruptionBeyondTheDepthBoundIsNotFound() {
    Throwable chain = CORRUPTION;
    for (int i = 0; i < 25; i++) {
      chain = new RuntimeException("layer " + i, chain);
    }

    assertThat(findCorruptEntityTypeCause(chain)).isEmpty();
  }

  @Test
  public void testCorruptionAtTheDeepestReachableLinkIsStillFound() {
    Throwable chain = CORRUPTION;
    for (int i = 0; i < 19; i++) {
      chain = new RuntimeException("layer " + i, chain);
    }

    assertThat(findCorruptEntityTypeCause(chain)).hasValue(CORRUPTION);
  }
}
