package com.linkedin.openhouse.tables.dto.mapper;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
import com.linkedin.openhouse.tables.model.ViewDto;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;
import org.springframework.data.domain.Page;

/** Mapper between the /v1 views wire models and {@link ViewDto}. */
@Mapper(componentModel = "spring")
public interface ViewsMapper {

  /**
   * Transform a create/update request into a {@link ViewDto} for the service layer.
   *
   * <p>The caller-supplied {@code baseViewVersion} is stored as {@code viewVersion} so the service
   * can compare it against the current pointer later, mirroring how {@code TablesMapper} stores
   * {@code baseTableVersion} as {@code tableVersion}. Server-owned pointer fields are left unset:
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
    @Mapping(
        source = "baseViewVersion",
        target = "viewVersion"), /* store base version to check later */
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
   * Transform a page of {@link ViewDto} into a page of response bodies, preserving the page
   * metadata. List DTOs carry identifiers only, so the resulting response bodies are intentionally
   * sparse.
   *
   * @param viewDtoPage source page
   * @return a page of sparse response bodies
   */
  default Page<GetViewResponseBody> toGetViewResponseBodyPage(Page<ViewDto> viewDtoPage) {
    return viewDtoPage.map(this::toGetViewResponseBody);
  }
}
