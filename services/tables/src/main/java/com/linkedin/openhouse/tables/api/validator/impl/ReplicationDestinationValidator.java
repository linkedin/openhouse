package com.linkedin.openhouse.tables.api.validator.impl;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import org.springframework.stereotype.Component;

/**
 * ReplicationDestinationValidator is a custom validator to validate the destination cluster name
 * for replication policy. The destination cluster must be different from the source cluster.
 */
@Component
public class ReplicationDestinationValidator {
  public void validate(Policies policies, TableUri tableUri) {
    if (policies != null
        && policies.getReplication() != null
        && policies.getReplication().getConfig() != null) {
      policies
          .getReplication()
          .getConfig()
          .forEach(
              replicationConfig -> {
                if (!validateReplicationDestination(replicationConfig, tableUri)) {
                  throw new RequestValidationFailureException(
                      String.format(
                          "Replication destination cluster for the table [%s] must be different from the source cluster",
                          tableUri));
                }
              });
    }
  }

  /**
   * Validate that the destination cluster provided by users is not the same as the source cluster
   */
  protected boolean validateReplicationDestination(
      ReplicationConfig replicationConfig, TableUri tableUri) {
    return !replicationConfig.getDestination().equals(tableUri.getClusterId());
  }
}
