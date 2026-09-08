package com.linkedin.openhouse.tables.mock.api;

import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseViewsApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
import com.linkedin.openhouse.tables.api.validator.ViewsApiValidator;
import com.linkedin.openhouse.tables.dto.mapper.ViewsMapper;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.services.ViewsService;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpStatus;

/**
 * Plain-Mockito coverage of {@link OpenHouseViewsApiHandler}.
 *
 * <p>Intentionally no Spring context: {@code MockTablesApplication} does not component-scan {@code
 * tables.api.handler.impl}, and the {@code @Primary} mock handler would shadow the real one anyway,
 * so a Spring test could not obtain this bean.
 */
@ExtendWith(MockitoExtension.class)
public class OpenHouseViewsApiHandlerTest {

  private static final String SERVING_CLUSTER = "local-cluster";
  private static final String ACTING_PRINCIPAL = "DUMMY_ANONYMOUS_USER";

  @Mock private ViewsApiValidator viewsApiValidator;

  @Mock private ViewsService viewsService;

  @Mock private ViewsMapper viewsMapper;

  @Mock private ClusterProperties clusterProperties;

  @InjectMocks private OpenHouseViewsApiHandler handler;

  private ViewDto viewDto;

  private GetViewResponseBody responseBody;

  @BeforeEach
  public void setup() {
    viewDto =
        ViewDto.builder()
            .viewId(ViewModelConstants.VIEW_ID)
            .databaseId(ViewModelConstants.DATABASE_ID)
            .build();
    responseBody = ViewModelConstants.pointerResponse();
  }

  @Test
  public void getViewValidatesBeforeCallingTheServiceAndReturns200() {
    when(viewsService.getView(
            ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL))
        .thenReturn(viewDto);
    when(viewsMapper.toGetViewResponseBody(viewDto)).thenReturn(responseBody);

    ApiResponse<GetViewResponseBody> apiResponse =
        handler.getView(
            ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL);

    InOrder inOrder = Mockito.inOrder(viewsApiValidator, viewsService);
    inOrder
        .verify(viewsApiValidator)
        .validateGetView(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    inOrder
        .verify(viewsService)
        .getView(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL);

    Assertions.assertEquals(HttpStatus.OK, apiResponse.getHttpStatus());
    Assertions.assertSame(
        responseBody,
        apiResponse.getResponseBody(),
        "The handler must forward the mapper's result untouched; it does no serialization or"
            + " enrichment of its own.");
  }

  @Test
  public void getAllViewsPassesThroughTheMappedPageAndReturns200() {
    Page<ViewDto> servicePage =
        new PageImpl<>(Collections.singletonList(viewDto), PageRequest.of(0, 50), 1);
    Page<GetViewResponseBody> mappedPage = ViewModelConstants.sparseListPage();

    when(viewsService.getAllViews(ViewModelConstants.DATABASE_ID, 0, 50, null, ACTING_PRINCIPAL))
        .thenReturn(servicePage);
    when(viewsMapper.toGetViewResponseBodyPage(servicePage)).thenReturn(mappedPage);

    ApiResponse<GetAllViewsResponseBody> apiResponse =
        handler.getAllViews(ViewModelConstants.DATABASE_ID, 0, 50, null, ACTING_PRINCIPAL);

    InOrder inOrder = Mockito.inOrder(viewsApiValidator, viewsService);
    inOrder
        .verify(viewsApiValidator)
        .validateGetAllViews(ViewModelConstants.DATABASE_ID, 0, 50, null);
    inOrder
        .verify(viewsService)
        .getAllViews(ViewModelConstants.DATABASE_ID, 0, 50, null, ACTING_PRINCIPAL);

    Assertions.assertEquals(HttpStatus.OK, apiResponse.getHttpStatus());
    Assertions.assertSame(mappedPage, apiResponse.getResponseBody().getPageResults());
  }

  @Test
  public void createViewValidatesAgainstTheServingClusterAndReturns201() {
    CreateUpdateViewRequestBody requestBody = ViewModelConstants.createRequestWithoutBaseVersion();
    when(clusterProperties.getClusterName()).thenReturn(SERVING_CLUSTER);
    when(viewsService.putView(requestBody, ACTING_PRINCIPAL, true))
        .thenReturn(Pair.of(viewDto, true));
    when(viewsMapper.toGetViewResponseBody(viewDto)).thenReturn(responseBody);

    ApiResponse<GetViewResponseBody> apiResponse =
        handler.createView(ViewModelConstants.DATABASE_ID, requestBody, ACTING_PRINCIPAL);

    InOrder inOrder = Mockito.inOrder(viewsApiValidator, viewsService);
    inOrder
        .verify(viewsApiValidator)
        .validateCreateView(SERVING_CLUSTER, ViewModelConstants.DATABASE_ID, requestBody);
    // failOnExist is true on POST: a POST must never silently replace an existing view.
    inOrder.verify(viewsService).putView(requestBody, ACTING_PRINCIPAL, true);

    Assertions.assertEquals(HttpStatus.CREATED, apiResponse.getHttpStatus());
    Assertions.assertSame(responseBody, apiResponse.getResponseBody());
  }

  @Test
  public void updateViewSelectsStatusFromTheServiceCreatedFlag() {
    CreateUpdateViewRequestBody requestBody = ViewModelConstants.fullyPopulatedRequest();
    when(clusterProperties.getClusterName()).thenReturn(SERVING_CLUSTER);
    when(viewsMapper.toGetViewResponseBody(viewDto)).thenReturn(responseBody);

    when(viewsService.putView(requestBody, ACTING_PRINCIPAL, false))
        .thenReturn(Pair.of(viewDto, false));
    Assertions.assertEquals(
        HttpStatus.OK,
        handler
            .updateView(
                ViewModelConstants.DATABASE_ID,
                ViewModelConstants.VIEW_ID,
                requestBody,
                ACTING_PRINCIPAL)
            .getHttpStatus(),
        "A PUT that replaced an existing view reports 200.");

    when(viewsService.putView(requestBody, ACTING_PRINCIPAL, false))
        .thenReturn(Pair.of(viewDto, true));
    Assertions.assertEquals(
        HttpStatus.CREATED,
        handler
            .updateView(
                ViewModelConstants.DATABASE_ID,
                ViewModelConstants.VIEW_ID,
                requestBody,
                ACTING_PRINCIPAL)
            .getHttpStatus(),
        "A PUT that created the view reports 201.");

    // Strict alternation proves the validator ran before the service on *each* invocation, not
    // merely that both collaborators were touched. failOnExist is false on PUT: a PUT may create.
    InOrder inOrder = Mockito.inOrder(viewsApiValidator, viewsService);
    for (int invocation = 0; invocation < 2; invocation++) {
      inOrder
          .verify(viewsApiValidator)
          .validateUpdateView(
              SERVING_CLUSTER,
              ViewModelConstants.DATABASE_ID,
              ViewModelConstants.VIEW_ID,
              requestBody);
      inOrder.verify(viewsService).putView(requestBody, ACTING_PRINCIPAL, false);
    }
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void deleteViewValidatesBeforeCallingTheServiceAndReturns204() {
    ApiResponse<Void> apiResponse =
        handler.deleteView(
            ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL);

    InOrder inOrder = Mockito.inOrder(viewsApiValidator, viewsService);
    inOrder
        .verify(viewsApiValidator)
        .validateDeleteView(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    inOrder
        .verify(viewsService)
        .deleteView(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL);

    Assertions.assertEquals(HttpStatus.NO_CONTENT, apiResponse.getHttpStatus());
    Assertions.assertNull(apiResponse.getResponseBody());
  }
}
