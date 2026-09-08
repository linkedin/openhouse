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

  private final ViewPointer pointer;

  private final String viewUuid;

  private final Schema schema;

  private final List<SqlViewRepresentationIntent> representations;

  private final String sourceDialect;

  private final String defaultCatalog;

  private final Namespace defaultNamespace;

  private final Map<String, String> properties;

  private final long lastModifiedTime;

  /** Assigned by Iceberg, never computed by OpenHouse. */
  private final int currentVersionId;
}
