package com.linkedin.openhouse.tables.services;

import static com.linkedin.openhouse.internal.catalog.CatalogConstants.ENTITY_TYPE_VIEW;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import java.util.Objects;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The single pre-admission HTS capture for a view operation, reused through authorization,
 * admission and commit; no later step re-reads.
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

  private PreparedViewOperation(Optional<HouseTable> occupantRow) {
    this.occupantRow = occupantRow;
  }

  public static PreparedViewOperation observedAbsence() {
    return new PreparedViewOperation(Optional.empty());
  }

  public static PreparedViewOperation view(HouseTable viewRow) {
    Objects.requireNonNull(viewRow, "viewRow must not be null; use observedAbsence() instead");
    return new PreparedViewOperation(Optional.of(viewRow));
  }

  public static PreparedViewOperation tableOccupant(HouseTable tableRow) {
    Objects.requireNonNull(tableRow, "tableRow must not be null; use observedAbsence() instead");
    return new PreparedViewOperation(Optional.of(tableRow));
  }

  /** The occupant (same reference) only when it is a VIEW row: the REPLACE CAS token. */
  public Optional<HouseTable> getViewBaseRow() {
    return occupantRow.filter(row -> ENTITY_TYPE_VIEW.equals(row.getEntityType()));
  }
}
