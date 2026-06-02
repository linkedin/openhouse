package com.linkedin.openhouse.optimizer.scheduler.binpack;

import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;

/**
 * One packable unit. Exposes the weight a packer keys on, plus the identity the scheduler reads
 * when it launches a Spark job (fully-qualified table name, operation id).
 *
 * <p>{@link #withOpAndStats(TableOperationDto, TableStatsDto)} returns a new populated instance
 * from a (pending operation, current stats) pair. Implementations have a no-arg constructor that
 * makes a "seat" prototype suitable for calling {@code withOpAndStats(...)} on; getters on a seat
 * are not meaningful.
 */
public interface BinItem {
  long getWeight();

  String getFullyQualifiedTableName();

  String getOperationId();

  BinItem withOpAndStats(TableOperationDto op, TableStatsDto stats);
}
