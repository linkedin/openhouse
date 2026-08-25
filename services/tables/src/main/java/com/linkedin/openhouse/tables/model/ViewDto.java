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
 * <p>Deliberately <b>not</b> a JPA entity: it carries no {@code @Entity}, no {@code @IdClass} and
 * no primary-key companion class, because view persistence does not exist yet and M2 must not
 * retrofit this DTO as a separate persisted namespace. It likewise carries no UUID and no {@code
 * TableType} — views are not a table variant.
 *
 * <p>Fields split into two groups. The pointer group ({@code viewUri}, {@code metadataLocation},
 * {@code viewVersion}, {@code creationTime}, {@code lastModifiedTime}, {@code viewCreator}) is what
 * a read returns. The definition group ({@code schema}, {@code representations}, {@code
 * sourceDialect}, {@code defaultCatalog}, {@code defaultNamespace}, {@code viewProperties}) is
 * write-only input and never appears in a response.
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
   * supplied {@code baseViewVersion} so the service can compare it later.
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
