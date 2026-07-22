package com.linkedin.openhouse.tables.repository.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tables.api.spec.v0.request.components.History;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.PolicyTag;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Replication;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OpenHouseInternalRepositoryImpl#mergePolicies}, the per-plane policy merge
 * used by CREATE OR REPLACE (RTAS). Captures the merge contract: a plane the request provides wins;
 * a plane the request omits is carried forward from the existing table; new planes can be added via
 * RTAS; and {@code sharingEnabled} (a primitive boolean) takes the request's value.
 */
public class OpenHouseInternalRepositoryImplMergePoliciesTest {

  private static final Retention RETENTION_A =
      Retention.builder().count(3).granularity(TimePartitionSpec.Granularity.HOUR).build();
  private static final Retention RETENTION_B =
      Retention.builder().count(9).granularity(TimePartitionSpec.Granularity.DAY).build();
  private static final History HISTORY_A =
      History.builder()
          .maxAge(3)
          .granularity(TimePartitionSpec.Granularity.DAY)
          .versions(5)
          .build();
  private static final Replication REPLICATION_A =
      Replication.builder()
          .config(
              List.of(ReplicationConfig.builder().destination("clusterA").interval("1D").build()))
          .build();
  private static final Map<String, PolicyTag> COLUMN_TAGS_A =
      Map.of("col1", PolicyTag.builder().tags(Set.of(PolicyTag.Tag.PII)).build());
  private static final Map<String, PolicyTag> COLUMN_TAGS_B =
      Map.of("col2", PolicyTag.builder().tags(Set.of(PolicyTag.Tag.HC)).build());

  private static Policies allPlanes() {
    return Policies.builder()
        .retention(RETENTION_A)
        .history(HISTORY_A)
        .replication(REPLICATION_A)
        .columnTags(COLUMN_TAGS_A)
        .lockState(LockState.builder().locked(true).build())
        .sharingEnabled(true)
        .build();
  }

  @Test
  void existingNull_returnsRequestedUnchanged() {
    Policies requested = Policies.builder().retention(RETENTION_B).build();
    assertSame(requested, OpenHouseInternalRepositoryImpl.mergePolicies(null, requested));
  }

  @Test
  void requestedNull_carriesForwardEveryPlane() {
    Policies existing = allPlanes();
    assertSame(existing, OpenHouseInternalRepositoryImpl.mergePolicies(existing, null));
  }

  @Test
  void everyOmittedPlaneCarriedForward_providedPlaneWins() {
    Policies existing = allPlanes();
    // Request provides ONLY retention (a different value); all other planes omitted.
    Policies requested = Policies.builder().retention(RETENTION_B).build();

    Policies merged = OpenHouseInternalRepositoryImpl.mergePolicies(existing, requested);

    // The provided plane wins (partial merge changes only that plane).
    assertEquals(RETENTION_B, merged.getRetention());
    assertEquals(9, merged.getRetention().getCount());
    // Every omitted plane is carried forward from the existing table.
    assertEquals(HISTORY_A, merged.getHistory());
    assertEquals(REPLICATION_A, merged.getReplication());
    assertEquals(COLUMN_TAGS_A, merged.getColumnTags());
    assertTrue(merged.getLockState().isLocked());
  }

  @Test
  void canAddPolicyPlanesViaRtas() {
    // Existing has retention only; RTAS request adds history and PII column tags.
    Policies existing = Policies.builder().retention(RETENTION_A).build();
    Policies requested = Policies.builder().history(HISTORY_A).columnTags(COLUMN_TAGS_B).build();

    Policies merged = OpenHouseInternalRepositoryImpl.mergePolicies(existing, requested);

    assertEquals(RETENTION_A, merged.getRetention()); // carried forward
    assertEquals(HISTORY_A, merged.getHistory()); // added by RTAS
    assertEquals(COLUMN_TAGS_B, merged.getColumnTags()); // added by RTAS
    assertNull(merged.getReplication()); // never set on either side
  }

  @Test
  void columnTags_emptyRequestCarriesForward_nonEmptyRequestWins() {
    Policies existing = Policies.builder().columnTags(COLUMN_TAGS_A).build();

    // Absent/empty column tags on the request -> existing tags carried forward.
    Policies mergedEmpty =
        OpenHouseInternalRepositoryImpl.mergePolicies(existing, Policies.builder().build());
    assertEquals(COLUMN_TAGS_A, mergedEmpty.getColumnTags());

    // Non-empty column tags on the request -> the request's tags win.
    Policies mergedOverride =
        OpenHouseInternalRepositoryImpl.mergePolicies(
            existing, Policies.builder().columnTags(COLUMN_TAGS_B).build());
    assertEquals(COLUMN_TAGS_B, mergedOverride.getColumnTags());
  }

  @Test
  void sharingEnabled_takesRequestValue() {
    Policies existing = Policies.builder().sharingEnabled(true).build();
    Policies requested = Policies.builder().retention(RETENTION_A).sharingEnabled(false).build();

    Policies merged = OpenHouseInternalRepositoryImpl.mergePolicies(existing, requested);

    assertFalse(merged.isSharingEnabled());
  }
}
