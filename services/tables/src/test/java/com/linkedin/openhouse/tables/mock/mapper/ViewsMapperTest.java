package com.linkedin.openhouse.tables.mock.mapper;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
import com.linkedin.openhouse.tables.dto.mapper.ViewsMapper;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

/** Golden-path mapping coverage for {@link ViewsMapper}. */
@SpringBootTest
public class ViewsMapperTest {

  @Autowired private ViewsMapper viewsMapper;

  @Test
  public void testRequestMapsToViewDtoStoringBaseVersionAsViewVersion() {
    CreateUpdateViewRequestBody requestBody = ViewModelConstants.fullyPopulatedRequest();

    ViewDto viewDto = viewsMapper.toViewDto(requestBody);

    Assertions.assertEquals(requestBody.getViewId(), viewDto.getViewId());
    Assertions.assertEquals(requestBody.getDatabaseId(), viewDto.getDatabaseId());
    Assertions.assertEquals(requestBody.getClusterId(), viewDto.getClusterId());
    Assertions.assertEquals(requestBody.getSchema(), viewDto.getSchema());
    Assertions.assertEquals(requestBody.getRepresentations(), viewDto.getRepresentations());
    Assertions.assertEquals(requestBody.getSourceDialect(), viewDto.getSourceDialect());
    Assertions.assertEquals(requestBody.getDefaultCatalog(), viewDto.getDefaultCatalog());
    Assertions.assertEquals(requestBody.getDefaultNamespace(), viewDto.getDefaultNamespace());
    Assertions.assertEquals(requestBody.getViewProperties(), viewDto.getViewProperties());
    Assertions.assertEquals(
        requestBody.getBaseViewVersion(),
        viewDto.getViewVersion(),
        "The caller's base version is stored as viewVersion so the service can compare it against"
            + " the current pointer, mirroring how baseTableVersion maps to tableVersion.");

    // Pointer fields are server-owned and must not be populated from a request.
    Assertions.assertNull(viewDto.getViewUri());
    Assertions.assertNull(viewDto.getMetadataLocation());
    Assertions.assertNull(viewDto.getViewCreator());
    Assertions.assertEquals(0L, viewDto.getCreationTime());
    Assertions.assertEquals(0L, viewDto.getLastModifiedTime());
  }

  /**
   * Uses distinct sentinels for {@code metadataLocation} and {@code viewVersion}. In production the
   * two hold the same value, which would let a swapped mapping pass unnoticed.
   */
  @Test
  public void testViewDtoMapsToPointerResponseBody() {
    ViewDto viewDto =
        ViewDto.builder()
            .viewId(ViewModelConstants.VIEW_ID)
            .databaseId(ViewModelConstants.DATABASE_ID)
            .clusterId(ViewModelConstants.CLUSTER_ID)
            .viewUri(ViewModelConstants.VIEW_URI)
            .metadataLocation(ViewModelConstants.DISTINCT_METADATA_LOCATION)
            .viewVersion(ViewModelConstants.DISTINCT_VIEW_VERSION)
            .creationTime(ViewModelConstants.CREATION_TIME)
            // Definition fields have no counterpart on the pointer-only read contract.
            .schema(ViewModelConstants.VIEW_SCHEMA_LITERAL)
            .sourceDialect(ViewModelConstants.SOURCE_DIALECT)
            .build();

    GetViewResponseBody responseBody = viewsMapper.toGetViewResponseBody(viewDto);

    Assertions.assertEquals(ViewModelConstants.pointerResponseWithDistinctPointers(), responseBody);
    Assertions.assertEquals(
        ViewModelConstants.DISTINCT_METADATA_LOCATION, responseBody.getMetadataLocation());
    Assertions.assertEquals(
        ViewModelConstants.DISTINCT_VIEW_VERSION, responseBody.getViewVersion());
  }

  @Test
  public void testViewDtoPageMapsToSparseResponsePagePreservingMetadata() {
    List<ViewDto> content =
        Arrays.asList(
            ViewDto.builder().viewId("my_view").databaseId(ViewModelConstants.DATABASE_ID).build(),
            ViewDto.builder()
                .viewId("my_other_view")
                .databaseId(ViewModelConstants.DATABASE_ID)
                .build());
    Page<ViewDto> dtoPage = new PageImpl<>(content, PageRequest.of(1, 2), 7);

    Page<GetViewResponseBody> responsePage = viewsMapper.toGetViewResponseBodyPage(dtoPage);

    Assertions.assertEquals(
        ViewModelConstants.sparseListPage().getContent(), responsePage.getContent());
    Assertions.assertEquals(1, responsePage.getNumber());
    Assertions.assertEquals(2, responsePage.getSize());
    Assertions.assertEquals(7, responsePage.getTotalElements());
    Assertions.assertEquals(4, responsePage.getTotalPages());
    Assertions.assertNull(
        responsePage.getContent().get(0).getMetadataLocation(),
        "List elements stay sparse: only identifiers are populated.");
  }
}
