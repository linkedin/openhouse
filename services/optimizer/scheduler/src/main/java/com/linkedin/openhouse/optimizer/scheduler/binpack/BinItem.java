package com.linkedin.openhouse.optimizer.scheduler.binpack;

/**
 * Smallest contract a {@link BinPacker} needs from each unit it packs: a single non-negative
 * weight. Implementations are operation-specific (see {@code
 * com.linkedin.openhouse.optimizer.operations.ofd.OfdBinItem}) and encode their own cost model in
 * {@link #getWeight()}. They also carry whatever identity the downstream dispatcher needs (table
 * name, operation id, etc.); those getters live on the impl, not on this interface, so the packer
 * stays a pure utility.
 */
public interface BinItem {
  long getWeight();
}
