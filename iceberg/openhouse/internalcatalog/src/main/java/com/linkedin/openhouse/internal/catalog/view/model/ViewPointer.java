package com.linkedin.openhouse.internal.catalog.view.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** View-facing HTS projection; UUID requires a metadata read and is returned separately. */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class ViewPointer {

  private final String databaseId;

  private final String viewId;

  private final String metadataLocation;

  private final String storageType;

  private final long creationTime;
}
