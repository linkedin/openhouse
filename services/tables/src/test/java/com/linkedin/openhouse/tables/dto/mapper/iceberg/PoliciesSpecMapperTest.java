package com.linkedin.openhouse.tables.dto.mapper.iceberg;

import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Replication;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.core.util.CronExpression;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PoliciesSpecMapperTest {

  @Test
  void mapPoliciesWithReplicationWithValidIntervalTest() {
    PoliciesSpecMapper mapper = new PoliciesSpecMapper();
    List<Integer> validIntervals = Arrays.asList(1, 3, 6, 12);
    for (int validInterval : validIntervals) {
      Policies policy1 = mapper.mapPolicies(getPolicies(validInterval));
      Assertions.assertNotNull(policy1.getReplication().getConfig().get(0).getCronSchedule());
      Assertions.assertTrue(
          isValidCronExpression(policy1.getReplication().getConfig().get(0).getCronSchedule()));
    }
  }

  private boolean isValidCronExpression(String cronExpression) {
    try {
      // Attempt to create a CronExpression object
      // If no exception is thrown, the expression is valid
      new CronExpression(cronExpression);
      return true;
    } catch (Exception e) {
      // Invalid cron expression
      return false;
    }
  }

  private Policies getPolicies(int interval) {
    List<ReplicationConfig> configs = new ArrayList<>();
    configs.add(
        ReplicationConfig.builder()
            .destination("War")
            .interval(String.format("%dH", interval))
            .build());
    Replication repl = Replication.builder().config(configs).build();
    return Policies.builder().replication(repl).build();
  }
}
