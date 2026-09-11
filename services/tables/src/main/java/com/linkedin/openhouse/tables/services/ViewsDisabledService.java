package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.model.ViewDto;
import org.springframework.data.domain.Page;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

/**
 * Disabled {@link ViewsService} implementation. Every operation reports {@link
 * ViewErrorCode#VIEWS_DISABLED}, producing HTTP 404 without reading or writing view state.
 */
@Component
public class ViewsDisabledService implements ViewsService {

  /**
   * Fixed and redacted. The message is copied into the error body and into service audit events, so
   * it must never echo request content.
   */
  static final String VIEWS_DISABLED_MESSAGE = "Views are disabled";

  @Override
  public ViewDto getView(String databaseId, String viewId, String actingPrincipal) {
    throw viewsDisabled();
  }

  @Override
  public Page<ViewDto> getAllViews(
      String databaseId, int page, int size, String sortBy, String actingPrincipal) {
    throw viewsDisabled();
  }

  @Override
  public Pair<ViewDto, Boolean> putView(
      CreateUpdateViewRequestBody requestBody, String actingPrincipal, boolean failOnExist) {
    throw viewsDisabled();
  }

  @Override
  public void deleteView(String databaseId, String viewId, String actingPrincipal) {
    throw viewsDisabled();
  }

  private ViewApiException viewsDisabled() {
    return new ViewApiException(ViewErrorCode.VIEWS_DISABLED, VIEWS_DISABLED_MESSAGE);
  }
}
