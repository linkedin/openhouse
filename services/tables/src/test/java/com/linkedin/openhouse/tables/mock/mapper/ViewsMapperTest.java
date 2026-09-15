package com.linkedin.openhouse.tables.mock.mapper;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
import com.linkedin.openhouse.tables.dto.mapper.ViewsMapper;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewListResult;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

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
    Assertions.assertEquals(requestBody.getSchema(), viewDto.getSchema());
    Assertions.assertEquals(requestBody.getRepresentations(), viewDto.getRepresentations());
    Assertions.assertEquals(requestBody.getSourceDialect(), viewDto.getSourceDialect());
    Assertions.assertEquals(requestBody.getDefaultCatalog(), viewDto.getDefaultCatalog());
    Assertions.assertEquals(requestBody.getDefaultNamespace(), viewDto.getDefaultNamespace());
    Assertions.assertEquals(requestBody.getViewProperties(), viewDto.getViewProperties());
    Assertions.assertEquals(
        requestBody.getBaseMetadataLocation(),
        viewDto.getViewVersion(),
        "The caller's base version is stored as viewVersion so the service can compare it against"
            + " the current pointer, mirroring how baseTableVersion maps to tableVersion.");

    // Pointer fields are server-owned and must not be populated from a request.
    Assertions.assertNull(
        viewDto.getClusterId(),
        "The serving cluster is the service's own identity, so the mapper never takes it from a"
            + " request, whatever the caller sent.");
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
  public void testViewListResultMapsToTheResponseEnvelopePreservingOrderAndToken() {
    List<ViewDto> results =
        Arrays.asList(
            ViewModelConstants.sparseListDto("my_view"),
            ViewModelConstants.sparseListDto("my_other_view"));
    ViewListResult serviceResult =
        ViewListResult.builder()
            .results(results)
            .nextPageToken(ViewModelConstants.NEXT_PAGE_TOKEN)
            .build();

    GetAllViewsResponseBody responseBody = viewsMapper.toGetAllViewsResponseBody(serviceResult);

    Assertions.assertEquals(ViewModelConstants.sparseListElements(), responseBody.getResults());
    Assertions.assertEquals("my_view", responseBody.getResults().get(0).getViewId());
    Assertions.assertEquals(
        "my_other_view",
        responseBody.getResults().get(1).getViewId(),
        "Order is the service's; the mapper does not sort.");
    Assertions.assertEquals(ViewModelConstants.NEXT_PAGE_TOKEN, responseBody.getNextPageToken());
    Assertions.assertNull(
        responseBody.getResults().get(0).getMetadataLocation(),
        "List elements stay sparse: only identifiers are populated.");
  }

  /** Neither exhaustion nor continuation may be inferred from the number of elements. */
  @Test
  public void testTokenPresenceFollowsTheServiceRatherThanTheResultCount() {
    Assertions.assertNull(
        viewsMapper
            .toGetAllViewsResponseBody(ViewModelConstants.viewListResult())
            .getNextPageToken(),
        "A full terminal page has no token.");

    GetAllViewsResponseBody emptyTerminal =
        viewsMapper.toGetAllViewsResponseBody(
            ViewListResult.builder().results(Collections.emptyList()).build());
    Assertions.assertEquals(Collections.emptyList(), emptyTerminal.getResults());
    Assertions.assertNull(emptyTerminal.getNextPageToken());

    GetAllViewsResponseBody emptyContinuing =
        viewsMapper.toGetAllViewsResponseBody(
            ViewModelConstants.emptyViewListResultWithNextPageToken());
    Assertions.assertEquals(Collections.emptyList(), emptyContinuing.getResults());
    Assertions.assertEquals(
        ViewModelConstants.NEXT_PAGE_TOKEN,
        emptyContinuing.getNextPageToken(),
        "An empty page keeps the service's token: emptiness is not exhaustion.");
  }

  @Test
  public void testServiceResultCannotOmitItsResults() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> ViewListResult.builder().nextPageToken(ViewModelConstants.NEXT_PAGE_TOKEN).build(),
        "A missing list is a construction error, not an accidental successful empty page.");
  }

  @ParameterizedTest(name = "nextPageToken={0}")
  @ValueSource(strings = {"  padded  ", "a+b/c=%", "null", "\tleading-tab"})
  public void testOutgoingTokenIsPreservedVerbatim(String token) {
    GetAllViewsResponseBody responseBody =
        viewsMapper.toGetAllViewsResponseBody(
            ViewListResult.builder()
                .results(ViewModelConstants.sparseListDtos())
                .nextPageToken(token)
                .build());

    Assertions.assertEquals(token, responseBody.getNextPageToken());
    Assertions.assertEquals(
        2, responseBody.getResults().size(), "The results are unaffected by the token's form.");
  }

  private static Stream<Arguments> invalidServiceResults() {
    return Stream.of(
        Arguments.of("no result at all", null, "viewsService returned no result"),
        Arguments.of(
            "null results list",
            ViewModelConstants.invalidResultWithNullResults(),
            "viewsService returned an invalid results list"),
        Arguments.of(
            "null element",
            ViewModelConstants.invalidResultWithNullElement(),
            "viewsService returned an invalid results list"),
        Arguments.of(
            "whitespace-only continuation token",
            ViewModelConstants.invalidResultWithBlankNextPageToken(),
            "viewsService returned a blank continuation token"),
        Arguments.of(
            "empty continuation token",
            ViewModelConstants.invalidResultWithEmptyNextPageToken(),
            "viewsService returned a blank continuation token"));
  }

  /** Invalid service output must not become a terminal response. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidServiceResults")
  public void testInvalidServiceOutputIsRejectedRatherThanMapped(
      String name, ViewListResult invalidResult, String expectedMessage) {
    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> viewsMapper.toGetAllViewsResponseBody(invalidResult));

    Assertions.assertEquals(
        expectedMessage,
        exception.getMessage(),
        "The message is fixed and carries no returned identifier or token, because it reaches the"
            + " error body and the service audit event.");
  }
}
