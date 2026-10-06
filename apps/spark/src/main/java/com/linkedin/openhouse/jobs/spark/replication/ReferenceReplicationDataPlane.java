package com.linkedin.openhouse.jobs.spark.replication;

import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

/** Spark data-plane operations for the OpenHouse reference replicator. */
public class ReferenceReplicationDataPlane {
  private static final String CATALOG_CONF_PREFIX = "spark.sql.catalog.";

  private final SparkSession spark;
  private final TableMetadataProvider tableMetadataProvider;

  public ReferenceReplicationDataPlane(
      SparkSession spark, TableMetadataProvider tableMetadataProvider) {
    this.spark = spark;
    this.tableMetadataProvider = tableMetadataProvider;
  }

  public TableGeneration getGeneration(String catalogName, TableIdentifier identifier) {
    return getGeneration(getTableMetadata(catalogName, identifier));
  }

  public long getCurrentSnapshotId(String catalogName, TableIdentifier identifier) {
    return currentSnapshotId(loadCatalog(catalogName).loadTable(identifier), "table");
  }

  public OptionalLong findCurrentSnapshotId(String catalogName, TableIdentifier identifier) {
    Table table = loadCatalog(catalogName).loadTable(identifier);
    return table.currentSnapshot() == null
        ? OptionalLong.empty()
        : OptionalLong.of(table.currentSnapshot().snapshotId());
  }

  /**
   * Resolves the current locator for an immutable OpenHouse table generation. This is deliberately
   * based on catalog responses rather than Iceberg table properties used as rename linkage.
   */
  public Optional<TableIdentifier> findTableByGeneration(
      String catalogName, TableGeneration generation) {
    Catalog catalog = loadCatalog(catalogName);
    if (!(catalog instanceof SupportsNamespaces)) {
      throw new IllegalStateException(
          "Spark catalog does not support namespace listing: " + catalogName);
    }
    SupportsNamespaces namespaceCatalog = (SupportsNamespaces) catalog;
    List<TableIdentifier> matches = new ArrayList<>();
    for (Namespace namespace : namespaceCatalog.listNamespaces()) {
      for (TableIdentifier identifier : catalog.listTables(namespace)) {
        if (generation.equals(getGeneration(getTableMetadata(catalogName, identifier)))) {
          matches.add(identifier);
        }
      }
    }
    if (matches.size() > 1) {
      throw new IllegalStateException(
          "Multiple catalog tables match OpenHouse generation " + generation);
    }
    return matches.stream().findFirst();
  }

  /** Renames an existing replica using the OpenHouse catalog rename operation. */
  public void renameReplica(
      String catalogName,
      TableIdentifier from,
      TableIdentifier to,
      TableGeneration expectedGeneration) {
    Catalog catalog = loadCatalog(catalogName);
    GetTableResponseBody source = getTableMetadata(catalogName, from);
    requireGeneration(source, expectedGeneration, "rename source");
    requireReplica(source, from);
    if (from.equals(to)) {
      return;
    }
    if (catalog.tableExists(to)) {
      throw new IllegalStateException(
          "Cannot rename replica; destination table already exists: " + to);
    }
    catalog.renameTable(from, to);
    GetTableResponseBody renamed = getTableMetadata(catalogName, to);
    requireGeneration(renamed, expectedGeneration, "renamed destination");
    requireReplica(renamed, to);
    if (catalog.tableExists(from)) {
      throw new IllegalStateException("Old replica locator still exists after rename: " + from);
    }
  }

  /**
   * Replaces the destination contents with the current source contents through Spark SQL and the
   * OpenHouse catalog. The destination must already be a REPLICA_TABLE.
   */
  public CopyResult copyLatestSnapshot(
      String sourceCatalogName,
      TableIdentifier sourceIdentifier,
      String destinationCatalogName,
      TableIdentifier destinationIdentifier,
      TableGeneration expectedSourceGeneration,
      TableGeneration expectedDestinationGeneration) {
    if (sourceCatalogName.equals(destinationCatalogName)
        && sourceIdentifier.equals(destinationIdentifier)) {
      throw new IllegalArgumentException("Source and destination catalog locators must differ");
    }
    Catalog sourceCatalog = loadCatalog(sourceCatalogName);
    Catalog destinationCatalog = loadCatalog(destinationCatalogName);
    Table source = sourceCatalog.loadTable(sourceIdentifier);
    GetTableResponseBody sourceMetadata = getTableMetadata(sourceCatalogName, sourceIdentifier);
    GetTableResponseBody destinationMetadata =
        getTableMetadata(destinationCatalogName, destinationIdentifier);
    requireGeneration(sourceMetadata, expectedSourceGeneration, "source");
    requireGeneration(destinationMetadata, expectedDestinationGeneration, "destination");
    requireReplica(destinationMetadata, destinationIdentifier);

    long sourceSnapshotId = currentSnapshotId(source, "source");
    String sourceSqlIdentifier = sqlIdentifier(sourceCatalogName, sourceIdentifier);
    String destinationSqlIdentifier = sqlIdentifier(destinationCatalogName, destinationIdentifier);
    spark.sql(
        String.format(
            "INSERT OVERWRITE TABLE %s SELECT * FROM %s",
            destinationSqlIdentifier, sourceSqlIdentifier));

    Table committedDestination = destinationCatalog.loadTable(destinationIdentifier);
    GetTableResponseBody committedDestinationMetadata =
        getTableMetadata(destinationCatalogName, destinationIdentifier);
    requireGeneration(
        committedDestinationMetadata, expectedDestinationGeneration, "destination after copy");
    requireReplica(committedDestinationMetadata, destinationIdentifier);
    long destinationSnapshotId = currentSnapshotId(committedDestination, "destination");
    return new CopyResult(sourceSnapshotId, destinationSnapshotId);
  }

  private Catalog loadCatalog(String catalogName) {
    Map<String, String> sparkProperties = JavaConverters.mapAsJavaMap(spark.conf().getAll());
    String catalogProperty = CATALOG_CONF_PREFIX + catalogName;
    String implementation = sparkProperties.get(catalogProperty + ".catalog-impl");
    if (implementation == null || implementation.trim().isEmpty()) {
      throw new IllegalArgumentException("Spark catalog is not configured: " + catalogName);
    }
    Map<String, String> catalogProperties = new java.util.HashMap<>();
    String propertyPrefix = catalogProperty + ".";
    sparkProperties.forEach(
        (key, value) -> {
          if (key.startsWith(propertyPrefix)) {
            catalogProperties.put(key.substring(propertyPrefix.length()), value);
          }
        });
    return CatalogUtil.loadCatalog(
        implementation, catalogName, catalogProperties, spark.sparkContext().hadoopConfiguration());
  }

  private GetTableResponseBody getTableMetadata(String catalogName, TableIdentifier identifier) {
    GetTableResponseBody metadata = tableMetadataProvider.get(catalogName, identifier);
    if (metadata == null) {
      throw new IllegalStateException(
          "Tables API returned no metadata for " + catalogName + "." + identifier);
    }
    return metadata;
  }

  private static TableGeneration getGeneration(GetTableResponseBody table) {
    if (table.getTableUUID() == null || table.getCreationTime() == null) {
      throw new IllegalStateException(
          "OpenHouse Tables API response is missing stable table identity: "
              + table.getDatabaseId()
              + "."
              + table.getTableId());
    }
    return new TableGeneration(table.getTableUUID(), table.getCreationTime());
  }

  private static void requireGeneration(
      GetTableResponseBody table, TableGeneration expected, String operation) {
    TableGeneration actual = getGeneration(table);
    if (!expected.equals(actual)) {
      throw new IllegalStateException(
          String.format(
              "OpenHouse table generation changed during %s: expected %s, found %s",
              operation, expected, actual));
    }
  }

  private static void requireReplica(GetTableResponseBody table, TableIdentifier identifier) {
    if (table.getTableType() != GetTableResponseBody.TableTypeEnum.REPLICA_TABLE) {
      throw new IllegalArgumentException(
          "Replication destination is not a REPLICA_TABLE: " + identifier);
    }
  }

  private static long currentSnapshotId(Table table, String role) {
    if (table.currentSnapshot() == null) {
      throw new IllegalStateException(
          "Cannot replicate "
              + role
              + " table without a current Iceberg snapshot: "
              + table.name());
    }
    return table.currentSnapshot().snapshotId();
  }

  private static String sqlIdentifier(String catalogName, TableIdentifier identifier) {
    if (identifier.namespace().levels().length != 1) {
      throw new IllegalArgumentException(
          "OpenHouse replication supports one-level database namespaces: " + identifier);
    }
    return quote(catalogName)
        + "."
        + quote(identifier.namespace().level(0))
        + "."
        + quote(identifier.name());
  }

  private static String quote(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  @FunctionalInterface
  public interface TableMetadataProvider {
    GetTableResponseBody get(String catalogName, TableIdentifier identifier);
  }

  public static final class TableGeneration {
    private final String tableUuid;
    private final long creationTime;

    public TableGeneration(String tableUuid, long creationTime) {
      this.tableUuid = tableUuid;
      this.creationTime = creationTime;
    }

    public String getTableUuid() {
      return tableUuid;
    }

    public long getCreationTime() {
      return creationTime;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof TableGeneration)) {
        return false;
      }
      TableGeneration that = (TableGeneration) other;
      return creationTime == that.creationTime && tableUuid.equals(that.tableUuid);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(tableUuid, creationTime);
    }

    @Override
    public String toString() {
      return tableUuid + "@" + creationTime;
    }
  }

  public static final class CopyResult {
    private final long sourceSnapshotId;
    private final long destinationSnapshotId;

    public CopyResult(long sourceSnapshotId, long destinationSnapshotId) {
      this.sourceSnapshotId = sourceSnapshotId;
      this.destinationSnapshotId = destinationSnapshotId;
    }

    public long getSourceSnapshotId() {
      return sourceSnapshotId;
    }

    public long getDestinationSnapshotId() {
      return destinationSnapshotId;
    }
  }
}
