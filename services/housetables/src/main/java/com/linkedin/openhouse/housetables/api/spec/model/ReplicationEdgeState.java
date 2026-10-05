package com.linkedin.openhouse.housetables.api.spec.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

/** Joined read view of one stable destination association and its independent checkpoint. */
@Builder(toBuilder = true)
@Value
public class ReplicationEdgeState {
  @JsonProperty("destination")
  ReplicationDestination destination;

  @JsonProperty("checkpoint")
  ReplicationCheckpoint checkpoint;
}
