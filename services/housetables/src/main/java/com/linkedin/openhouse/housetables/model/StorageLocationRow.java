package com.linkedin.openhouse.housetables.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** JPA entity for the storage_location table. */
@Entity
@Table(name = "storage_location")
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class StorageLocationRow {

  @Id
  @Column(name = "storage_location_id")
  private String storageLocationId;

  @Column(name = "uri", nullable = false)
  private String uri;
}
