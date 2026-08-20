package com.linkedin.openhouse.tables.repository.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tables.api.spec.v0.request.components.History;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.PolicyTag;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Replication;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Tripwire tests guarding the RTAS policy merge ({@link
 * OpenHouseInternalRepositoryImpl#mergePolicies}) against a new policy plane being added to {@link
 * Policies} without teaching the merge to carry it forward. If either test fails, a policy plane
 * was added or changed — update {@code mergePolicies} (and the expected set below) so CREATE OR
 * REPLACE (RTAS) does not silently drop the new plane.
 */
public class RtasPolicyMergeTripwireTest {

  /**
   * Behavioral guard: merging an existing table that has every policy plane populated with a
   * request that omits them must carry every plane forward. The reflective loop covers nullable
   * object planes; the sharing assertion covers the primitive flag.
   */
  @Test
  public void mergeCarriesForwardEveryPolicyPlane() throws Exception {
    Policies existing =
        Policies.builder()
            .retention(
                Retention.builder()
                    .count(3)
                    .granularity(TimePartitionSpec.Granularity.HOUR)
                    .build())
            .columnTags(
                Collections.singletonMap(
                    "col",
                    PolicyTag.builder().tags(Collections.singleton(PolicyTag.Tag.PII)).build()))
            .replication(
                Replication.builder()
                    .config(
                        Collections.singletonList(
                            ReplicationConfig.builder()
                                .destination("clusterA")
                                .interval("12H")
                                .build()))
                    .build())
            .history(
                History.builder()
                    .maxAge(3)
                    .granularity(TimePartitionSpec.Granularity.DAY)
                    .versions(5)
                    .build())
            .lockState(LockState.builder().locked(true).build())
            .sharingEnabled(true)
            .build();
    // Request omits every policy plane, matching the sparse policies payload Spark RTAS sends.
    Policies requested = Policies.builder().build();

    Policies merged = OpenHouseInternalRepositoryImpl.mergePolicies(existing, requested);

    for (Field field : Policies.class.getDeclaredFields()) {
      if (field.isSynthetic()
          || Modifier.isStatic(field.getModifiers())
          || field.getType().isPrimitive()) {
        continue;
      }
      field.setAccessible(true);
      assertNotNull(
          field.get(merged),
          "RTAS policy merge dropped Policies."
              + field.getName()
              + " — a new policy plane was likely added. Update "
              + "OpenHouseInternalRepositoryImpl.mergePolicies to carry it forward across CREATE OR REPLACE.");
    }
    assertTrue(
        merged.isSharingEnabled(),
        "RTAS policy merge dropped Policies.sharingEnabled — update "
            + "OpenHouseInternalRepositoryImpl.mergePolicies to carry it forward across CREATE OR REPLACE.");
  }

  /**
   * Structural guard: the set of policy planes is fixed and known. Any field added, removed, or
   * renamed on {@link Policies} (including a primitive the behavioral test cannot observe) trips
   * this test, forcing a conscious update of the RTAS merge.
   */
  @Test
  public void policiesFieldSetIsUnchanged() {
    Set<String> expected =
        new TreeSet<>(
            Set.of(
                "retention",
                "sharingEnabled",
                "columnTags",
                "replication",
                "history",
                "lockState"));

    Set<String> actual =
        java.util.Arrays.stream(Policies.class.getDeclaredFields())
            .filter(f -> !f.isSynthetic() && !Modifier.isStatic(f.getModifiers()))
            .map(Field::getName)
            .collect(Collectors.toCollection(TreeSet::new));

    assertEquals(
        expected,
        actual,
        "Policies fields changed. A policy plane was added/removed/renamed — update "
            + "OpenHouseInternalRepositoryImpl.mergePolicies (RTAS merge) so it is not dropped on "
            + "CREATE OR REPLACE, then update this expected set.");
  }
}
