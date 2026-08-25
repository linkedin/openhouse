package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.model.ViewDto;
import org.springframework.data.domain.Page;
import org.springframework.data.util.Pair;

/** Service interface backing the /v2 views endpoints. */
public interface ViewsService {

  /**
   * Given a databaseId and viewId, prepare a {@link ViewDto} if actingPrincipal has the right
   * privilege.
   *
   * @param databaseId database identifier
   * @param viewId view identifier
   * @param actingPrincipal authenticated user
   * @return the view pointer
   */
  ViewDto getView(String databaseId, String viewId, String actingPrincipal);

  /**
   * Given a databaseId, prepare a page of identifier-only {@link ViewDto}s.
   *
   * @param databaseId database identifier
   * @param page zero-based page index
   * @param size page size
   * @param sortBy optional single sort field
   * @param actingPrincipal authenticated user
   * @return a page of identifier-only dtos
   */
  Page<ViewDto> getAllViews(
      String databaseId, int page, int size, String sortBy, String actingPrincipal);

  /**
   * Create or replace a view.
   *
   * @param requestBody the create/update request
   * @param actingPrincipal authenticated user performing the write
   * @param failOnExist true for POST create, false for PUT create-or-replace
   * @return a pair whose first element is the saved view and whose second element is true iff the
   *     call created the view rather than replacing it
   */
  Pair<ViewDto, Boolean> putView(
      CreateUpdateViewRequestBody requestBody, String actingPrincipal, boolean failOnExist);

  /**
   * Delete the view identified by databaseId and viewId if actingPrincipal has the right privilege.
   *
   * @param databaseId database identifier
   * @param viewId view identifier
   * @param actingPrincipal authenticated user
   */
  void deleteView(String databaseId, String viewId, String actingPrincipal);
}
