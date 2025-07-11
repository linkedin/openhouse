package com.linkedin.openhouse.housetables.model;

import java.sql.Timestamp;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Version;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/** Data Model for persisting a Soft Deleted User Table Object in the HouseTable. */
@Entity
@IdClass(SoftDeletedUserTableRowPrimaryKey.class)
@SuperBuilder(toBuilder = true)
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

  @Setter Timestamp timeToLive;
}
