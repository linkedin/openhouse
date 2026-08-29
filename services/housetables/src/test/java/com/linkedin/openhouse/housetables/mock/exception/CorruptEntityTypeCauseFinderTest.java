package com.linkedin.openhouse.housetables.mock.exception;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.housetables.exception.CorruptEntityTypeCauseFinder;
import com.linkedin.openhouse.housetables.exception.CorruptEntityTypeConversionException;
import com.linkedin.openhouse.housetables.exception.CorruptUserTableDataException;
import java.util.Optional;
import javax.persistence.PersistenceException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.orm.jpa.JpaSystemException;

/**
 * The single cause search the adapter and the advice share. It returns an {@link Optional} rather
 * than a nullable sentinel, so every assertion here states presence or absence explicitly.
 */
public class CorruptEntityTypeCauseFinderTest {

  private static final String CORRUPT_MSG =
      "Column user_table_row.entity_type holds unrecognized value ['UNKNOWN']; "
          + "only TABLE, VIEW (in any case) and NULL are valid";

  private static CorruptEntityTypeConversionException corruption() {
    return new CorruptEntityTypeConversionException(
        CORRUPT_MSG, new IllegalArgumentException("UNKNOWN"));
  }

  @Test
  public void testTheExceptionItselfIsFound() {
    CorruptEntityTypeConversionException corruption = corruption();

    Optional<CorruptEntityTypeConversionException> found =
        CorruptEntityTypeCauseFinder.find(corruption);

    assertThat(found).isPresent();
    assertThat(found).containsSame(corruption);
  }

  /** The shape Hibernate produces when the attribute converter fails mid-result-set. */
  @Test
  public void testCorruptionInsideAJpaSystemExceptionIsFound() {
    CorruptEntityTypeConversionException corruption = corruption();

    Optional<CorruptEntityTypeConversionException> found =
        CorruptEntityTypeCauseFinder.find(
            new JpaSystemException(
                new PersistenceException(
                    "Error attempting to apply AttributeConverter", corruption)));

    assertThat(found).containsSame(corruption);
  }

  /** The other wrapper the translator can pick, and the module wrapper the adapter produces. */
  @Test
  public void testCorruptionInsideOtherWrappersIsFound() {
    CorruptEntityTypeConversionException corruption = corruption();

    assertThat(
            CorruptEntityTypeCauseFinder.find(
                new InvalidDataAccessApiUsageException("converter failed", corruption)))
        .containsSame(corruption);
    assertThat(
            CorruptEntityTypeCauseFinder.find(
                new CorruptUserTableDataException(
                    "read failed", new JpaSystemException(new PersistenceException(corruption)))))
        .containsSame(corruption);
  }

  @Test
  public void testDeeplyNestedCorruptionIsFound() {
    CorruptEntityTypeConversionException corruption = corruption();

    Optional<CorruptEntityTypeConversionException> found =
        CorruptEntityTypeCauseFinder.find(
            new JpaSystemException(
                new PersistenceException(
                    "outer",
                    new IllegalStateException(
                        "middle", new RuntimeException("inner", corruption)))));

    assertThat(found).containsSame(corruption);
  }

  @Test
  public void testUnrelatedChainYieldsAbsence() {
    Optional<CorruptEntityTypeConversionException> found =
        CorruptEntityTypeCauseFinder.find(
            new JpaSystemException(new PersistenceException("connection reset")));

    assertThat(found).isEmpty();
  }

  /** Absence must also be the answer for a null input, not a NullPointerException. */
  @Test
  public void testNullYieldsAbsence() {
    assertThat(CorruptEntityTypeCauseFinder.find(null)).isEmpty();
  }

  /** Identity tracking, not just depth, is what makes a cycle terminate. */
  @Test
  public void testCyclicChainTerminatesWithAbsence() {
    assertThat(CorruptEntityTypeCauseFinder.find(new SelfCausedException("cycle"))).isEmpty();
  }

  /**
   * A chain longer than the bound stops early. The corruption is placed past the bound, so a finder
   * that ignored the bound would find it and this assertion would fail.
   */
  @Test
  public void testChainBeyondTheDepthBoundIsNotSearched() {
    Throwable chain = corruption();
    for (int level = 0; level < CorruptEntityTypeCauseFinder.CAUSE_CHAIN_MAX_DEPTH + 5; level++) {
      chain = new RuntimeException("level-" + level, chain);
    }

    assertThat(CorruptEntityTypeCauseFinder.find(chain)).isEmpty();
  }

  /**
   * {@link Throwable#initCause} forbids a self-referential cause, so the cycle is expressed by
   * overriding the accessor.
   */
  private static class SelfCausedException extends RuntimeException {
    SelfCausedException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable getCause() {
      return this;
    }
  }
}
