package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.common.exception.CorruptEntityTypeException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import org.springframework.dao.DataAccessException;

/**
 * Containment for the one ORM detail housetables cannot ignore: {@code EntityTypeConverter} throws
 * on a corrupt discriminator, and Spring's persistence exception translation buries that inside a
 * {@link DataAccessException} whose message is the wrapper's rather than the diagnostic.
 */
public final class CorruptEntityTypeTranslation {

  /** Bounds the cause walk, so a cyclic chain terminates instead of spinning. */
  private static final int CAUSE_CHAIN_MAX_DEPTH = 20;

  private CorruptEntityTypeTranslation() {}

  /**
   * Runs immediately after {@code PersistenceExceptionTranslationInterceptor}. Corruption is
   * rethrown unwrapped so the advice can render its diagnostic; everything else is rethrown exactly
   * as it arrived, so an infrastructure failure behaves as it always has.
   */
  public static <T> T translating(Callable<T> read) {
    try {
      return read.call();
    } catch (DataAccessException dataAccessException) {
      throw findCorruptEntityTypeCause(dataAccessException)
          .map(corruption -> (RuntimeException) corruption)
          .orElse(dataAccessException);
    } catch (RuntimeException runtimeException) {
      throw runtimeException;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /** Bounded by depth and by visited identity, so a cyclic cause chain terminates. */
  public static Optional<CorruptEntityTypeException> findCorruptEntityTypeCause(
      Throwable exception) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Throwable current = exception;
    for (int depth = 0; current != null && depth < CAUSE_CHAIN_MAX_DEPTH; depth++) {
      if (!visited.add(current)) {
        break;
      }
      if (current instanceof CorruptEntityTypeException) {
        return Optional.of((CorruptEntityTypeException) current);
      }
      current = current.getCause();
    }
    return Optional.empty();
  }
}
