package com.linkedin.openhouse.internal.catalog.view.model;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;

/**
 * Caller-supplied description of one view commit.
 *
 * <p>{@code baseViewVersion} discriminates the operation: {@code null} means CREATE, and a non-null
 * value means REPLACE against that exact current metadata path.
 *
 * <p>{@code viewUuid}, {@code viewLocation}, and {@code storageType} are <b>required for CREATE and
 * ignored for REPLACE</b>. Stable identity generation, storage selection, and root allocation
 * belong to the service layer above this contract, exactly as {@code tableUUID} and the allocated
 * table root are prepared above the internal table catalog. A replace deliberately takes its
 * physical identity from the published row and its current metadata instead, so the three fields
 * are never read on that path and a conflicting value cannot cause reallocation. The commit engine
 * provides no fallback for a missing create-side value; it fails instead of allocating, mirroring
 * {@code OpenHouseInternalCatalog.defaultWarehouseLocation}.
 *
 * <p>Version-neutral by construction: {@link Schema} and {@link Namespace} exist in Iceberg 1.2,
 * and no {@code org.apache.iceberg.view.*} type appears here.
 */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class ViewCommitIntent {

  private final String databaseId;

  private final String viewId;

  private final Schema schema;

  private final List<SqlViewRepresentationIntent> representations;

  /** Dialect the caller authored the view in; recorded in the resulting version summary. */
  private final String sourceDialect;

  private final String defaultCatalog;

  /** Null and {@link Namespace#empty()} are the same value on both build and comparison. */
  private final Namespace defaultNamespace;

  private final Map<String, String> viewProperties;

  /**
   * Null means CREATE. Non-null is the exact current metadata path this replace is based on, and is
   * passed unchanged as the compare-and-swap token.
   */
  private final String baseViewVersion;

  /**
   * Acting principal recorded on the entity. This engine records identity only; authorization
   * belongs to the later service layer.
   */
  private final String creator;

  /**
   * Stable entity UUID generated or retained by the caller. Required for CREATE, ignored for
   * REPLACE, which preserves the published {@code ViewMetadata.uuid()}.
   */
  private final String viewUuid;

  /**
   * Already allocated view root directory supplied by the caller. Required for CREATE, ignored for
   * REPLACE, which writes under the published {@code ViewMetadata.location()}.
   */
  private final String viewLocation;

  /**
   * Type of the storage the caller already selected, used only to obtain the CREATE {@code FileIO}.
   * Required for CREATE, ignored for REPLACE, which resolves it from the pointer row.
   */
  private final String storageType;
}
