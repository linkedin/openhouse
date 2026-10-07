package com.linkedin.openhouse.internal.catalog.view.model;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;

/** The complete definition of a published view, as parsed back out of its metadata file. */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class LoadedView {

  /** HTS reference identifying the published metadata file and its storage type. */
  private final ViewPointer pointer;

  /** Stable view identity read from metadata, preserved across replacements. */
  private final String viewUuid;

  /** Output schema of the current view version. */
  private final Schema schema;

  /** SQL definitions for each dialect supported by the current version. */
  private final List<SqlViewRepresentationIntent> representations;

  /** Original source dialect recorded in the current version summary. */
  private final String sourceDialect;

  /** Default catalog for resolving unqualified references in the SQL definitions. */
  private final String defaultCatalog;

  /** Default namespace for resolving unqualified references in the SQL definitions. */
  private final Namespace defaultNamespace;

  /** Metadata properties, including user properties and OpenHouse-managed fields. */
  private final Map<String, String> properties;

  /** Last modification time in epoch milliseconds, read from metadata properties. */
  private final long lastModifiedTime;

  /** Assigned by Iceberg, never computed by OpenHouse. */
  private final int currentVersionId;
}
