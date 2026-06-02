package com.linkedin.openhouse.optimizer.scheduler.binpack;

/**
 * One packable unit. Exposes the weight a packer keys on, plus the identity the scheduler reads
 * when it launches a Spark job (fully-qualified table name, operation id). Implementations are
 * immutable data — projection from {@code (operation, stats)} to a concrete {@link BinItem} subtype
 * is the bin packer's responsibility.
 */
public interface BinItem {
  long getWeight();

  String getFullyQualifiedTableName();

  String getOperationId();
}
