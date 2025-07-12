package com.linkedin.openhouse.housetables.model;

import java.sql.Timestamp;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Version;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Data Model for persisting a Soft Deleted User Table Object in the HouseTable. */
@Entity
@IdClass(SoftDeletedUserTableRowPrimaryKey.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class SoftDeletedUserTableRow {

  @Id String tableId;

  @Id String databaseId;

  @Id Long deletedAtMs;

  @Version Long version;

  String metadataLocation;

  String storageType;

  Long creationTime;

  Timestamp timeToLive;
}
