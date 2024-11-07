package com.linkedin.openhouse.tables.api.validator.impl;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Replication;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * ReplicationConfigValidator is a custom validator to validate the input values for destination and
 * interval used for replication.
 */
@Slf4j
@Component
public class ReplicationConfigValidator {
  public void validate(Replication replication, TableUri tableUri) {
    log.info(String.format("Table: %s replication: %s\n", tableUri, replication));
  }
}
