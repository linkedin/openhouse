package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewListResult;
import org.springframework.data.util.Pair;

/** Service interface backing the /v1 views endpoints. */
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
   * Return at most {@code size} identifier-only views, with non-null results and elements.
   *
   * <p>A null next token marks completion; non-null tokens must be nonblank. The service owns token
   * validation, ordering, and traversal consistency.
   *
   * @param databaseId database identifier
   * @param pageToken opaque continuation token from a previous result, or null for the first page
   * @param size maximum number of results to return
   * @param sortBy optional single sort field
   * @param actingPrincipal authenticated user
   * @return one page of identifier-only dtos and its optional continuation token
   */
  ViewListResult getAllViews(
      String databaseId, String pageToken, int size, String sortBy, String actingPrincipal);

  // TODO: give view properties the reserved-key lifecycle tables already have, centralized in a
  // view
  // analog of PreservedKeyChecker instead of split across layers. For openhouse.view.sourceDialect
  // (the view counterpart of a client-supplied, then frozen, reserved table key like
  // openhouse.tableUUID) mirror tables:
  //   1. identity: openhouse.-prefixed keys are reserved (reuse
  // HouseTableSerdeUtils.IS_OH_PREFIXED);
  //   2. create: allow this one key to be set once, still rejecting every other reserved key
  //      (allowKeyInCreation analog);
  //   3. replace: reject any add/alter/drop of a reserved key (checkIfPreservedViewPropsModified,
  //      mirroring OpenHouseInternalRepositoryImpl.checkIfPreservedTblPropsModified ->
  //      ALTER_RESERVED_TBLPROPS);
  //   4. persist it in the view's standard Iceberg properties map and strip reserved keys from user
  //      properties on read (getUserTblProps analog).
  // The create-time allow currently lives as a carve-out in OpenHouseViewsApiValidator because
  // there
  // is no view repository yet; move it here once persistence exists so the whole lifecycle is in
  // one
  // place, as it is for tables.
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
