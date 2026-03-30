package com.linkedin.openhouse.housetables.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** JPA entity for the table_storage_location join table. */
@Entity
@Table(name = "table_storage_location")
@IdClass(TableStorageLocationRowPrimaryKey.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class TableStorageLocationRow {

  @Id
  @Column(name = "table_uuid")
  private String tableUuid;

  @Id
  @Column(name = "storage_location_id")
  private String storageLocationId;
}
