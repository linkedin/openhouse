package com.linkedin.openhouse.housetables.repository;

import com.linkedin.openhouse.housetables.api.spec.model.ReplicationCheckpoint;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationCheckpointUpdate;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationDestination;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationEdgeState;
import java.util.List;

/** Persistence contract for replication destination associations and per-edge progress. */
public interface ReplicationStateStore {
  List<ReplicationEdgeState> findDestinations(
      String sourceClusterId, String sourceTableUUID, long sourceCreationTime);

  ReplicationDestination putDestination(ReplicationDestination destination);

  ReplicationCheckpoint findCheckpoint(
      String sourceClusterId,
      String sourceTableUUID,
      long sourceCreationTime,
      String destinationClusterId,
      String destinationTableUUID,
      long destinationCreationTime);

  ReplicationCheckpoint advanceCheckpoint(ReplicationCheckpointUpdate update);
}
