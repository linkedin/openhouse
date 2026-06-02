package com.linkedin.openhouse.optimizer.scheduler.binpack;

import java.util.List;

/**
 * Strategy interface for grouping a flat list of {@link BinItem}s into one or more {@link Bin}s.
 * Implementations encode the per-bin caps and the placement algorithm; callers iterate the returned
 * bins and dispatch one batch per bin.
 *
 * <p>The input parameter uses {@code ? extends BinItem} so callers can pass a typed list of a
 * concrete impl (e.g. {@code List<OfdBinItem>}) without fighting Java's invariance. The packer sees
 * the items only as {@link BinItem}s; the per-op-type dispatcher downcasts at access time.
 */
public interface BinPacker {
  /** Pack {@code items} into one or more bins. Each returned bin is non-empty. */
  List<Bin> pack(List<? extends BinItem> items);
}
