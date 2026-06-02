package com.linkedin.openhouse.optimizer.scheduler.binpack;

import java.util.List;

/**
 * Strategy interface for grouping a flat list of {@link BinItem}s into one or more {@link Bin}s.
 * Implementations encode the per-bin caps and the placement algorithm; callers iterate the returned
 * bins and dispatch one batch per bin.
 *
 * <p>The packer sees items only as {@link BinItem}; per-op-type dispatchers narrow to their
 * concrete impl at access time.
 */
public interface BinPacker {
  /** Pack {@code items} into one or more bins. Each returned bin is non-empty. */
  List<Bin> pack(List<BinItem> items);
}
