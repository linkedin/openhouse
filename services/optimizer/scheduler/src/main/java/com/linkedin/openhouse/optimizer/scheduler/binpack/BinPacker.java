package com.linkedin.openhouse.optimizer.scheduler.binpack;

import java.util.List;

/**
 * Strategy interface for grouping a flat list of {@link BinItem}s into one or more {@link Bin}s.
 * Implementations encode the per-bin caps (weight, items, etc.) and the placement algorithm;
 * callers iterate the returned bins and dispatch one batch per bin.
 *
 * <p>Parametric on the {@link BinItem} impl so the packer, bins, and items are all type-consistent
 * end-to-end — the dispatch site receives {@code List<Bin<T>>} and never has to downcast.
 *
 * @param <T> concrete {@link BinItem} implementation packed by this packer
 */
public interface BinPacker<T extends BinItem> {
  /** Pack {@code items} into one or more bins. Each returned bin is non-empty. */
  List<Bin<T>> pack(List<T> items);
}
