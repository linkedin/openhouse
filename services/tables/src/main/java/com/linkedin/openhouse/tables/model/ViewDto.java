package com.linkedin.openhouse.tables.model;

import com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Internal representation of a view as it moves between the API handler and the view service.
 *
 * <p>This DTO is not a persistence entity. Views are not a {@code TableType} variant.
 *
 * <p>Pointer fields populate the read response. Definition fields are write inputs and are omitted
 * from the pointer-only response.
 */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ViewDto {

  private String viewId;

  private String databaseId;

  private String clusterId;

  private String viewUri;

  private String metadataLocation;

  /**
   * On a read this is the view's current version pointer. On a write it carries the caller's
   * supplied {@code baseMetadataLocation} for the service's concurrency check.
   */
  private String viewVersion;

  private String viewCreator;

  private long creationTime;

  private long lastModifiedTime;

  private String schema;

  private List<ViewRepresentation> representations;

  private String sourceDialect;

  private String defaultCatalog;

  private List<String> defaultNamespace;

  private Map<String, String> viewProperties;
}
