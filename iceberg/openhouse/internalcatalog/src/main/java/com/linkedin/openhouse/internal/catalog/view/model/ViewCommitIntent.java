package com.linkedin.openhouse.internal.catalog.view.model;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
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
 * <p>{@code operation} is required and chosen explicitly; the engine never infers it. {@code
 * baseRow} is the trusted House Table snapshot the future views repository read for this logical
 * target with a single neutral lookup: a non-null row is that hydrated snapshot, and {@code null}
 * means the lookup completed and found absence — never "not loaded". The engine classifies and
 * swaps against this snapshot and performs no House Table read of its own.
 *
 * <p>{@code viewUuid}, {@code viewLocation}, and {@code storageType} are required for CREATE and
 * ignored for REPLACE, which takes identity, root, and storage from the captured row and its
 * metadata. There is no fallback: a missing create-side value fails rather than being allocated
 * here.
 *
 * <p>Version-neutral by construction, so it loads under Iceberg 1.2.
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

  private final String sourceDialect;

  private final String defaultCatalog;

  /** Null and {@link Namespace#empty()} are the same value on both build and comparison. */
  private final Namespace defaultNamespace;

  private final Map<String, String> viewProperties;

  /** Required CREATE or REPLACE; the engine rejects a null value at commit, before any effect. */
  private final ViewCommitOperation operation;

  /**
   * The server-read House Table snapshot for this target: a hydrated row, or {@code null} when the
   * lookup completed and found absence. Never re-read by the engine.
   */
  private final HouseTable baseRow;

  private final String creator;

  /** Required for CREATE; a REPLACE preserves the published {@code ViewMetadata.uuid()}. */
  private final String viewUuid;

  /** Required for CREATE; a REPLACE writes under the published {@code ViewMetadata.location()}. */
  private final String viewLocation;

  /** Required for CREATE; a REPLACE resolves storage from the captured row. */
  private final String storageType;
}
