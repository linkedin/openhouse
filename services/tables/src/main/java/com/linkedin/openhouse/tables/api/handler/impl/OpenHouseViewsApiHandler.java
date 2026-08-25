package com.linkedin.openhouse.tables.api.handler.impl;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.handler.ViewsApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
import com.linkedin.openhouse.tables.api.validator.ViewsApiValidator;
import com.linkedin.openhouse.tables.dto.mapper.ViewsMapper;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.services.ViewsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Default /v2 views API handler. The flow is strictly validate, map, delegate to the service, map
 * back, and pick a status: no business logic, no response serialization and no feature gating live
 * here.
 */
@Component
public class OpenHouseViewsApiHandler implements ViewsApiHandler {

  @Autowired private ViewsApiValidator viewsApiValidator;

  @Autowired private ViewsService viewsService;

  @Autowired private ViewsMapper viewsMapper;

  @Autowired private ClusterProperties clusterProperties;

  @Override
  public ApiResponse<GetViewResponseBody> getView(
      String databaseId, String viewId, String actingPrincipal) {
    viewsApiValidator.validateGetView(databaseId, viewId);
    ViewDto viewDto = viewsService.getView(databaseId, viewId, actingPrincipal);
    return ApiResponse.<GetViewResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(viewsMapper.toGetViewResponseBody(viewDto))
        .build();
  }

  @Override
  public ApiResponse<GetAllViewsResponseBody> getAllViews(
      String databaseId, int page, int size, String sortBy, String actingPrincipal) {
    viewsApiValidator.validateGetAllViews(databaseId, page, size, sortBy);
    return ApiResponse.<GetAllViewsResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAllViewsResponseBody.builder()
                .pageResults(
                    viewsMapper.toGetViewResponseBodyPage(
                        viewsService.getAllViews(databaseId, page, size, sortBy, actingPrincipal)))
                .build())
        .build();
  }

  @Override
  public ApiResponse<GetViewResponseBody> createView(
      String databaseId, CreateUpdateViewRequestBody requestBody, String actingPrincipal) {
    viewsApiValidator.validateCreateView(
        clusterProperties.getClusterName(), databaseId, requestBody);
    Pair<ViewDto, Boolean> putResult = viewsService.putView(requestBody, actingPrincipal, true);
    return ApiResponse.<GetViewResponseBody>builder()
        .httpStatus(HttpStatus.CREATED)
        .responseBody(viewsMapper.toGetViewResponseBody(putResult.getFirst()))
        .build();
  }

  @Override
  public ApiResponse<GetViewResponseBody> updateView(
      String databaseId,
      String viewId,
      CreateUpdateViewRequestBody requestBody,
      String actingPrincipal) {
    viewsApiValidator.validateUpdateView(
        clusterProperties.getClusterName(), databaseId, viewId, requestBody);
    Pair<ViewDto, Boolean> putResult = viewsService.putView(requestBody, actingPrincipal, false);
    HttpStatus httpStatus = putResult.getSecond() ? HttpStatus.CREATED : HttpStatus.OK;
    return ApiResponse.<GetViewResponseBody>builder()
        .httpStatus(httpStatus)
        .responseBody(viewsMapper.toGetViewResponseBody(putResult.getFirst()))
        .build();
  }

  @Override
  public ApiResponse<Void> deleteView(String databaseId, String viewId, String actingPrincipal) {
    viewsApiValidator.validateDeleteView(databaseId, viewId);
    viewsService.deleteView(databaseId, viewId, actingPrincipal);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }
}
