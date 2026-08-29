package com.linkedin.openhouse.housetables.exception;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Optional;
import java.util.Set;

/**
 * The single cause-chain search this module performs. Shared by the persistence adapter, which
 * translates, and by the scoped advice, which renders; both must agree on what "carries corruption"
 * means.
 *
 * <p>Returns an {@link Optional} rather than a nullable sentinel, and walks the chain bounded by
 * both depth and visited identity so a self- or mutually-referential cause terminates.
 */
public final class CorruptEntityTypeCauseFinder {

  public static final int CAUSE_CHAIN_MAX_DEPTH = 20;

  private CorruptEntityTypeCauseFinder() {
    // Utility class, constructor does nothing
  }

  /**
   * @param exception the exception to search, which may itself be the corruption
   * @return the first {@link CorruptEntityTypeConversionException} in the chain, or empty
   */
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
