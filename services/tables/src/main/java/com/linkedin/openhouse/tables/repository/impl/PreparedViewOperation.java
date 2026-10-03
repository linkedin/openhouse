package com.linkedin.openhouse.tables.repository.impl;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import java.util.Objects;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The single pre-admission snapshot capture: the one HTS read for a view operation, taken once and
 * reused through authorization, base-version checking, admission, the engine commit, and audit; no
 * later step re-reads.
 *
 * <p>Three states, matching the three repository capture shapes:
 *
 * <ul>
 *   <li>{@link #observedAbsence()}: the key held nothing at capture time (a create candidate, or a
 *       read/delete target that does not exist).
 *   <li>{@link #view(HouseTable)}: the key held a VIEW row; this is both the neutral occupant and
 *       the typed view base row (the REPLACE CAS token and audit identity source).
 *   <li>{@link #tableOccupant(HouseTable)}: the neutral POST/PUT capture found a TABLE row at this
 *       key: a collision, never revealed as a view before service authorization resolves (the
 *       capture is neutral, not typed, precisely so a table occupant is not hidden behind
 *       "absent").
 * </ul>
 */
@Getter
@EqualsAndHashCode
@ToString
public final class PreparedViewOperation {

  /** Any occupant of the key (any entity type), from the neutral POST/PUT capture. */
  private final Optional<HouseTable> occupantRow;

  /** The occupant only when it is a VIEW row: the REPLACE CAS token and audit identity source. */
  private final Optional<HouseTable> viewBaseRow;

  /**
   * The view row's {@code tableLocation} at the moment of capture, snapshotted as a plain,
   * immutable string rather than re-read from {@link #viewBaseRow} later. {@link #viewBaseRow}
   * keeps the exact JPA-managed entity reference the repository loaded (required so the CAS token
   * it also serves as stays the identical object the engine commits against), and that same
   * persistence context's save/merge during commit can mutate that managed instance's fields
   * in-place once the operation publishes a new pointer, so a caller reading {@link #viewBaseRow}'s
   * field after the commit (as audit emission does) would otherwise observe the new value instead
   * of the one actually captured before the write.
   */
  private final String capturedTableLocation;

  /**
   * The requested identifiers, carried only so audit emission can name an operation whose capture
   * found no row at all (a create from observed absence). Excluded from equality/hashing/toString
   * so a caller that constructs this value without them (as every pre-existing unit test's {@link
   * #observedAbsence()} call does) still compares equal to an instance the service later enriches
   * via {@link #withRequestedIdentity(String, String)} purely for audit emission.
   */
  @EqualsAndHashCode.Exclude private final String requestedDatabaseId;

  @EqualsAndHashCode.Exclude private final String requestedViewId;

  private PreparedViewOperation(
      Optional<HouseTable> occupantRow,
      Optional<HouseTable> viewBaseRow,
      String capturedTableLocation,
      String requestedDatabaseId,
      String requestedViewId) {
    this.occupantRow = occupantRow;
    this.viewBaseRow = viewBaseRow;
    this.capturedTableLocation = capturedTableLocation;
    this.requestedDatabaseId = requestedDatabaseId;
    this.requestedViewId = requestedViewId;
  }

  public static PreparedViewOperation observedAbsence() {
    return new PreparedViewOperation(Optional.empty(), Optional.empty(), null, null, null);
  }

  public static PreparedViewOperation view(HouseTable viewRow) {
    Objects.requireNonNull(viewRow, "viewRow must not be null; use observedAbsence() instead");
    return new PreparedViewOperation(
        Optional.of(viewRow), Optional.of(viewRow), viewRow.getTableLocation(), null, null);
  }

  public static PreparedViewOperation tableOccupant(HouseTable tableRow) {
    Objects.requireNonNull(tableRow, "tableRow must not be null; use observedAbsence() instead");
    return new PreparedViewOperation(Optional.of(tableRow), Optional.empty(), null, null, null);
  }

  /**
   * Returns a value-equal copy carrying the requested database/view id for audit emission when this
   * capture found no row (so neither {@link #occupantRow} nor {@link #viewBaseRow} can supply one).
   * A capture that already found a row is returned unchanged: the row is always the authoritative
   * identity source.
   */
  public PreparedViewOperation withRequestedIdentity(String databaseId, String viewId) {
    if (occupantRow.isPresent()) {
      return this;
    }
    return new PreparedViewOperation(
        occupantRow, viewBaseRow, capturedTableLocation, databaseId, viewId);
  }

  /** The database id for audit naming: from the captured row if present, else the request. */
  public String auditDatabaseId() {
    return occupantRow.map(HouseTable::getDatabaseId).orElse(requestedDatabaseId);
  }

  /** The view id for audit naming: from the captured row if present, else the request. */
  public String auditViewId() {
    return occupantRow.map(HouseTable::getTableId).orElse(requestedViewId);
  }
}
