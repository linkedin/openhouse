package com.linkedin.openhouse.optimizer.scheduler.binpack;

import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;

/**
 * One packable unit. Exposes the weight a packer keys on, plus the identity the scheduler reads
 * when it launches a Spark job (fully-qualified table name, operation id).
 *
 * <p>Implementations have a public no-arg "seat" constructor — instantiated transiently inside
 * {@link FirstFitBinPacker#pack} via a {@code Supplier<T extends BinItem>} (typically a {@code
 * MyItem::new} method reference) — on which {@link #fromOpAndStats} is called to return the
 * populated item. Getters on a seat are not meaningful; the seat exists for the lifetime of a
 * single projection call.
 */
public interface BinItem {
  long getWeight();

  String getFullyQualifiedTableName();

  String getOperationId();

  BinItem fromOpAndStats(TableOperationDto op, TableStatsDto stats);
}
