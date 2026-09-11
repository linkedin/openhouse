package com.linkedin.openhouse.internal.catalog.view;

import lombok.Getter;

/** A view create collided with a House Table row that is not a view. */
@Getter
public class ViewNameOccupiedException extends RuntimeException {

  /** Never null: House Table resolves a legacy row with no discriminator to {@code TABLE}. */
  private final String occupantEntityType;

  private final String databaseId;

  private final String viewId;

  public ViewNameOccupiedException(String databaseId, String viewId, String occupantEntityType) {
    super(
        String.format(
            "Name %s.%s is already occupied by an entity of type %s",
            databaseId, viewId, occupantEntityType));
    this.databaseId = databaseId;
    this.viewId = viewId;
    this.occupantEntityType = occupantEntityType;
  }
}
