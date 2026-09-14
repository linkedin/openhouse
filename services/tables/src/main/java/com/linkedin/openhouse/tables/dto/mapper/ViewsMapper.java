package com.linkedin.openhouse.tables.dto.mapper;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewListResult;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;

/** Mapper between the /v1 views wire models and {@link ViewDto}. */
@Mapper(componentModel = "spring")
public interface ViewsMapper {

  /**
   * Transform a create/update request into a {@link ViewDto} for the service layer.
   *
   * <p>The caller-supplied {@code baseMetadataLocation} is stored as {@code viewVersion} so the
   * service can compare it against the current pointer. Server-owned pointer fields are left unset:
   * only the service can populate them.
   *
   * @param requestBody source request
   * @return a new immutable {@link ViewDto}
   */
  @Mappings({
    @Mapping(source = "viewId", target = "viewId"),
    @Mapping(source = "databaseId", target = "databaseId"),
    @Mapping(source = "clusterId", target = "clusterId"),
    @Mapping(source = "schema", target = "schema"),
    @Mapping(source = "representations", target = "representations"),
    @Mapping(source = "sourceDialect", target = "sourceDialect"),
    @Mapping(source = "defaultCatalog", target = "defaultCatalog"),
    @Mapping(source = "defaultNamespace", target = "defaultNamespace"),
    @Mapping(source = "viewProperties", target = "viewProperties"),
    @Mapping(source = "baseMetadataLocation", target = "viewVersion"),
    @Mapping(target = "viewUri", ignore = true),
    @Mapping(target = "metadataLocation", ignore = true),
    @Mapping(target = "viewCreator", ignore = true),
    @Mapping(target = "creationTime", ignore = true),
    @Mapping(target = "lastModifiedTime", ignore = true)
  })
  ViewDto toViewDto(CreateUpdateViewRequestBody requestBody);

  /**
   * Transform a {@link ViewDto} into the pointer-only read contract. Definition fields on the DTO
   * have no counterpart on the response by design and are dropped here.
   *
   * @param viewDto source dto
   * @return the response body forwarded to the client
   */
  GetViewResponseBody toGetViewResponseBody(ViewDto viewDto);

  /**
   * Transform a service list result into the client response.
   *
   * <p>Order, element count and the continuation token are the service's, and this copies them. It
   * does not sort, filter, truncate, refill a short page, derive a token from the number of results
   * or decode one.
   *
   * <p>Output the service is not allowed to produce is rejected rather than mapped: a missing
   * result, a missing or partly null list, or a blank token would otherwise reach the client as a
   * successful last page and end a traversal early. The messages are fixed and carry no returned
   * value, because they reach the error body and service audit events.
   *
   * @param result the service's page of identifier-only dtos and its optional continuation token
   * @return the response body forwarded to the client
   */
  default GetAllViewsResponseBody toGetAllViewsResponseBody(ViewListResult result) {
    if (result == null) {
      throw new IllegalStateException("viewsService returned no result");
    }
    List<ViewDto> results = result.getResults();
    if (results == null || results.stream().anyMatch(Objects::isNull)) {
      throw new IllegalStateException("viewsService returned an invalid results list");
    }
    String nextPageToken = result.getNextPageToken();
    if (nextPageToken != null && StringUtils.isBlank(nextPageToken)) {
      throw new IllegalStateException("viewsService returned a blank continuation token");
    }
    return GetAllViewsResponseBody.builder()
        .results(results.stream().map(this::toGetViewResponseBody).collect(Collectors.toList()))
        .nextPageToken(nextPageToken)
        .build();
  }
}
