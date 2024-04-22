package com.linkedin.openhouse.internal.catalog.model;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Data Model for persisting Table Object in the HTS-Repository. */
@Entity
@IdClass(HouseTablePrimaryKey.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class HouseTable {

  @Id private String tableId;

  @Id private String databaseId;

  private String clusterId;

  private String tableUri;

  private String tableUUID;

  private String tableLocation;

  private String tableVersion;

  private String tableCreator;

  private long lastModifiedTime;

  private long creationTime;

  private String storageType;
}
