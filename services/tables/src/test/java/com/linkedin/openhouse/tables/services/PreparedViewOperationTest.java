package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import org.junit.jupiter.api.Test;

public class PreparedViewOperationTest {

  private static final String POINTER = "file:/tmp/openhouse/db/v/captured.metadata.json";

  private static HouseTable row(String entityType, String location) {
    return HouseTable.builder()
        .databaseId("db")
        .tableId("v")
        .tableLocation(location)
        .storageType("local")
        .entityType(entityType)
        .build();
  }

  @Test
  public void observedAbsenceHasNoOccupantAndNoViewBase() {
    PreparedViewOperation prepared = PreparedViewOperation.observedAbsence();

    assertFalse(prepared.getOccupantRow().isPresent());
    assertFalse(prepared.getViewBaseRow().isPresent());
    assertNull(prepared.getCapturedTableLocation());
  }

  @Test
  public void tableOccupantIsCapturedButNeverProjectedAsAViewBase() {
    HouseTable table = row("TABLE", POINTER);

    PreparedViewOperation prepared = PreparedViewOperation.tableOccupant(table);

    assertSame(table, prepared.getOccupantRow().get());
    assertFalse(prepared.getViewBaseRow().isPresent());
    assertNull(prepared.getCapturedTableLocation());
  }

  @Test
  public void viewIsBothTheOccupantAndTheSameReferenceAsTheViewBase() {
    HouseTable view = row("VIEW", POINTER);

    PreparedViewOperation prepared = PreparedViewOperation.view(view);

    assertSame(view, prepared.getOccupantRow().get());
    assertSame(view, prepared.getViewBaseRow().get());
    assertEquals(POINTER, prepared.getCapturedTableLocation());
  }

  @Test
  public void viewBaseIsDerivedFromTheCanonicalCaseSensitiveViewType() {
    assertFalse(
        PreparedViewOperation.tableOccupant(row("view", POINTER)).getViewBaseRow().isPresent());
    assertFalse(
        PreparedViewOperation.tableOccupant(row(null, POINTER)).getViewBaseRow().isPresent());
  }

  @Test
  public void capturedPointerIsAnImmutableSnapshotWhileTheSameRowReferenceStaysTheCasToken() {
    HouseTable managed = mock(HouseTable.class);
    when(managed.getEntityType()).thenReturn("VIEW");
    when(managed.getTableLocation())
        .thenReturn(POINTER, "file:/tmp/openhouse/db/v/new.metadata.json");

    PreparedViewOperation prepared = PreparedViewOperation.view(managed);

    // The persistence context later changes the managed row in place after a publish.
    assertEquals("file:/tmp/openhouse/db/v/new.metadata.json", managed.getTableLocation());
    assertEquals(POINTER, prepared.getCapturedTableLocation());
    assertSame(managed, prepared.getViewBaseRow().get());
    assertSame(managed, prepared.getOccupantRow().get());
  }

  @Test
  public void equalityFollowsTheCapturedRowAndPointerAndIgnoresRequestedIdentity() {
    PreparedViewOperation view = PreparedViewOperation.view(row("VIEW", POINTER));

    assertEquals(view, PreparedViewOperation.view(row("VIEW", POINTER)));
    assertEquals(view.hashCode(), PreparedViewOperation.view(row("VIEW", POINTER)).hashCode());
    assertNotEquals(view, PreparedViewOperation.view(row("VIEW", POINTER + ".other")));
    assertNotEquals(view, PreparedViewOperation.tableOccupant(row("TABLE", POINTER)));
    assertNotEquals(view, PreparedViewOperation.observedAbsence());

    PreparedViewOperation absent = PreparedViewOperation.observedAbsence();
    PreparedViewOperation enriched = absent.withRequestedIdentity("db", "v");
    assertEquals(absent, enriched);
    assertEquals(absent.hashCode(), enriched.hashCode());
    assertEquals("db", enriched.auditDatabaseId());
    assertEquals("v", enriched.auditViewId());
    assertSame(view, view.withRequestedIdentity("other", "other"));
    assertTrue(view.getViewBaseRow().isPresent());
  }

  @Test
  public void factoriesStillRejectNullRows() {
    assertThrows(NullPointerException.class, () -> PreparedViewOperation.view(null));
    assertThrows(NullPointerException.class, () -> PreparedViewOperation.tableOccupant(null));
  }
}
