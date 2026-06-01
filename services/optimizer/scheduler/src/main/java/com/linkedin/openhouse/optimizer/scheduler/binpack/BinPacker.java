package com.linkedin.openhouse.optimizer.scheduler.binpack;

import java.util.List;

/**
 * Strategy interface for grouping a flat list of {@link BinItem}s into one or more {@link Bin}s.
 * Implementations encode the per-bin caps (file count, byte size, item count, etc.) and the
 * placement algorithm; callers iterate the returned bins and dispatch one batch per bin.
 *
 * <p>The interface does not reference any optimizer-specific types (operations, statuses,
 * repositories). Adapter code in the scheduler maps its domain objects into {@code BinItem}s before
 * calling and maps results back to operation ids after.
 */
public interface BinPacker {
  /** Pack {@code items} into one or more bins. Each returned bin is non-empty. */
  List<Bin> pack(List<BinItem> items);
}
