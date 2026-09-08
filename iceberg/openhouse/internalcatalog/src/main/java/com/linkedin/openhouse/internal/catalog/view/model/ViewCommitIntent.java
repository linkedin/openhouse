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
 * <p>{@code viewUuid}, {@code viewLocation}, and {@code storageType} are required for CREATE and
 * ignored for REPLACE, which takes them from the published row. There is no fallback: a missing
 * create-side value fails rather than being allocated here.
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

  /** Null means CREATE; non-null is the metadata path used unchanged as the swap token. */
  private final String baseViewVersion;

  private final String creator;

  /** Required for CREATE; a REPLACE preserves the published {@code ViewMetadata.uuid()}. */
  private final String viewUuid;

  /** Required for CREATE; a REPLACE writes under the published {@code ViewMetadata.location()}. */
  private final String viewLocation;

  /** Required for CREATE; a REPLACE resolves storage from the pointer row. */
  private final String storageType;
}
