package com.linkedin.openhouse.tables.repository.impl;

import static com.linkedin.openhouse.internal.catalog.CatalogConstants.ENTITY_TYPE_VIEW;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import java.util.Objects;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The single pre-admission HTS capture for a view operation, reused through authorization,
 * admission, commit and audit; no later step re-reads.
 *
 * <ul>
 *   <li>{@link #observedAbsence()}: no row at the key.
 *   <li>{@link #view(HouseTable)}: a VIEW row, which is also the REPLACE CAS token.
 *   <li>{@link #tableOccupant(HouseTable)}: a TABLE row (a name collision), not exposed as a view
 *       base.
 * </ul>
 */
@Getter
@EqualsAndHashCode
@ToString
public final class PreparedViewOperation {

  /** The row at the key, of any entity type: the exact reference loaded, never a copy. */
  private final Optional<HouseTable> occupantRow;

  /**
   * The view row's {@code tableLocation} at capture. The managed row can be mutated in place when
   * commit publishes a new pointer, so audit reads this snapshot instead of the row.
   */
  private final String capturedTableLocation;

  /**
   * Requested ids, for audit naming when the capture found no row. Excluded from equality so an
   * instance enriched via {@link #withRequestedIdentity(String, String)} equals the original.
   */
  @EqualsAndHashCode.Exclude private final String requestedDatabaseId;

  @EqualsAndHashCode.Exclude private final String requestedViewId;

  private PreparedViewOperation(
      Optional<HouseTable> occupantRow,
      String capturedTableLocation,
      String requestedDatabaseId,
      String requestedViewId) {
    this.occupantRow = occupantRow;
    this.capturedTableLocation = capturedTableLocation;
    this.requestedDatabaseId = requestedDatabaseId;
    this.requestedViewId = requestedViewId;
  }

  public static PreparedViewOperation observedAbsence() {
    return new PreparedViewOperation(Optional.empty(), null, null, null);
  }

  public static PreparedViewOperation view(HouseTable viewRow) {
    Objects.requireNonNull(viewRow, "viewRow must not be null; use observedAbsence() instead");
    return new PreparedViewOperation(Optional.of(viewRow), viewRow.getTableLocation(), null, null);
  }

  public static PreparedViewOperation tableOccupant(HouseTable tableRow) {
    Objects.requireNonNull(tableRow, "tableRow must not be null; use observedAbsence() instead");
    return new PreparedViewOperation(Optional.of(tableRow), null, null, null);
  }

  /** A value-equal copy carrying the requested ids when no row was found; otherwise this. */
  public PreparedViewOperation withRequestedIdentity(String databaseId, String viewId) {
    if (occupantRow.isPresent()) {
      return this;
    }
    return new PreparedViewOperation(occupantRow, capturedTableLocation, databaseId, viewId);
  }

  /** The occupant (same reference) only when it is a VIEW row: the REPLACE CAS token. */
  public Optional<HouseTable> getViewBaseRow() {
    return occupantRow.filter(row -> ENTITY_TYPE_VIEW.equals(row.getEntityType()));
  }

  /** Audit database id: the captured row's, else the requested one. */
  public String auditDatabaseId() {
    return occupantRow.map(HouseTable::getDatabaseId).orElse(requestedDatabaseId);
  }

  /** Audit view id: the captured row's, else the requested one. */
  public String auditViewId() {
    return occupantRow.map(HouseTable::getTableId).orElse(requestedViewId);
  }
}
