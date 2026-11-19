package com.linkedin.openhouse.tables.services;

import java.util.List;
import org.springframework.security.access.AuthorizationServiceException;

/** Indicates a replication DDL failure that may leave source and destination state out of sync. */
public class ReplicationCascadeException extends AuthorizationServiceException {
  public ReplicationCascadeException(String message, Throwable cause) {
    super(message, cause);
  }

  public static ReplicationCascadeException localCommitFailure(
      String operation, List<String> completedPeers, RuntimeException cause) {
    return new ReplicationCascadeException(
        String.format(
            "Remote %s succeeded at replication destinations %s, but the local commit failed; "
                + "destination state may be ahead. Check local state and retry the operation.",
            operation, completedPeers),
        cause);
  }
}
