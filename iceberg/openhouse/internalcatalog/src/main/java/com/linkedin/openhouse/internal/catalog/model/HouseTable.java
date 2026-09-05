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

  private long deletedAtMs;

  private long purgeAfterMs;

  /**
   * This column indicates the storage type used by this table. See {@link
   * com.linkedin.openhouse.cluster.storage.StorageType}. A storage type indicates the {@link
   * com.linkedin.openhouse.cluster.storage.StorageClient} implementation that is used to interact
   * with this table.
   */
  private String storageType;

  /**
   * Entity-type discriminator ({@code TABLE}/{@code VIEW}) for the row at this key.
   *
   * <p>Non-null on every fully hydrated House Table row: the server resolves a legacy null to
   * {@code TABLE} at its own parse boundary, and every test double reproduces that. Consumers
   * therefore never null-check it. A locally built, pre-mapping pointer may still omit it until a
   * write mapper stamps the route's canonical type.
   */
  private String entityType;
}
