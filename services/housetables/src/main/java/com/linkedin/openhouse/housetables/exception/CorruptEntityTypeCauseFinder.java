package com.linkedin.openhouse.housetables.exception;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Optional;
import java.util.Set;

/**
 * The single cause-chain search this module performs: the adapter that translates and the advice
 * that renders must agree on what "carries corruption" means.
 *
 * <p>Bounded by both depth and visited identity, so a cyclic cause chain terminates.
 */
public final class CorruptEntityTypeCauseFinder {

  public static final int CAUSE_CHAIN_MAX_DEPTH = 20;

  private CorruptEntityTypeCauseFinder() {
    // Utility class, constructor does nothing
  }

  /** @param exception the exception to search, which may itself be the corruption */
  public static Optional<CorruptEntityTypeConversionException> find(Throwable exception) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Throwable current = exception;
    for (int depth = 0; current != null && depth < CAUSE_CHAIN_MAX_DEPTH; depth++) {
      if (!visited.add(current)) {
        break;
      }
      if (current instanceof CorruptEntityTypeConversionException) {
        return Optional.of((CorruptEntityTypeConversionException) current);
      }
      current = current.getCause();
    }
    return Optional.empty();
  }
}
