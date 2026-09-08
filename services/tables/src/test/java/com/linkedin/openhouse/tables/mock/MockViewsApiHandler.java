package com.linkedin.openhouse.tables.mock;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.handler.ViewsApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.stereotype.Component;

/**
 * {@code @Primary} stand-in for {@link ViewsApiHandler} used by the views controller tests,
 * mirroring {@link MockTablesApiHandler}. It exists so the controller tests exercise routing,
 * status codes and response serialization without depending on a view service that does not exist
 * yet.
 *
 * <p><b>Error signal:</b> every route first runs the request's {@code databaseId} through a
 * deterministic switch, mirroring {@link MockTablesApiHandler}'s {@code "d200"}/{@code "d404"}
 * convention. {@link #databaseIdFor(ViewErrorCode)} yields the database id that makes a route throw
 * a {@link ViewApiException} carrying that code, and two further ids cover the uncoded paths: an
 * {@link AccessDeniedException} and a generic infrastructure {@link AuthorizationServiceException}.
 * Any other database id, including the {@code "d200"} the tests use for success, responds normally.
 *
 * <p><b>PUT signal:</b> PUT has two success statuses and the handler picks between them from the
 * service's created flag, which does not exist here. The mock therefore uses a deterministic,
 * documented identifier signal instead: a PUT for {@link #PUT_CREATES_VIEW_ID} reports 201 CREATED
 * and every other view id reports 200 OK. Keep this signal on the view id rather than the database
 * id so a later negative-path slice is free to use the database id for error selection, matching
 * how {@link MockTablesApiHandler} switches on {@code databaseId}.
 */
@Component
@Primary
public class MockViewsApiHandler implements ViewsApiHandler {

  /** A PUT to this view id reports 201 CREATED; any other view id reports 200 OK. */
  public static final String PUT_CREATES_VIEW_ID = "v201";

  /**
   * Fixed message carried by every thrown {@link ViewApiException}. Deliberately identical across
   * codes: the controller contract is status plus message, and the code itself never reaches the
   * wire, so tests must not be able to recover it from the body by accident.
   */
  public static final String VIEW_FAILURE_MESSAGE = "Mock view handler failure";

  /** Fixed message for the access-denied path. */
  public static final String ACCESS_DENIED_MESSAGE = "Mock view handler denied access";

  /** Fixed message for the generic, uncoded infrastructure failure path. */
  public static final String UNAVAILABLE_MESSAGE = "Mock view handler dependency unavailable";

  /** Database id that makes any route throw {@link AccessDeniedException} (403). */
  public static final String ACCESS_DENIED_DATABASE_ID = "d403";

  /**
   * Database id that makes any route throw {@link AuthorizationServiceException}, the generic
   * uncoded infrastructure failure that the shared handler maps to 503.
   */
  public static final String UNAVAILABLE_DATABASE_ID = "d503";

  private static final Map<String, ViewErrorCode> ERROR_CODE_BY_DATABASE_ID;

  static {
    Map<String, ViewErrorCode> byDatabaseId = new LinkedHashMap<>();
    for (ViewErrorCode errorCode : ViewErrorCode.values()) {
      byDatabaseId.put(databaseIdFor(errorCode), errorCode);
    }
    ERROR_CODE_BY_DATABASE_ID = Collections.unmodifiableMap(byDatabaseId);
  }

  /**
   * @return the database id that makes every route throw a {@link ViewApiException} carrying {@code
   *     errorCode}. Derived from the enum constant so a new code is covered automatically rather
   *     than needing a hand-maintained switch arm.
   */
  public static String databaseIdFor(ViewErrorCode errorCode) {
    return "derr_" + errorCode.name();
  }

  private static void throwIfErrorDatabaseId(String databaseId) {
    if (ACCESS_DENIED_DATABASE_ID.equals(databaseId)) {
      throw new AccessDeniedException(ACCESS_DENIED_MESSAGE);
    }
    if (UNAVAILABLE_DATABASE_ID.equals(databaseId)) {
      throw new AuthorizationServiceException(UNAVAILABLE_MESSAGE);
    }
    ViewErrorCode errorCode = ERROR_CODE_BY_DATABASE_ID.get(databaseId);
    if (errorCode != null) {
      throw new ViewApiException(errorCode, VIEW_FAILURE_MESSAGE);
    }
  }

  @Override
  public ApiResponse<GetViewResponseBody> getView(
      String databaseId, String viewId, String actingPrincipal) {
    throwIfErrorDatabaseId(databaseId);
    return ApiResponse.<GetViewResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(ViewModelConstants.pointerResponse())
        .build();
  }

  @Override
  public ApiResponse<GetAllViewsResponseBody> getAllViews(
      String databaseId, int page, int size, String sortBy, String actingPrincipal) {
    throwIfErrorDatabaseId(databaseId);
    return ApiResponse.<GetAllViewsResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(ViewModelConstants.listResponse())
        .build();
  }

  @Override
  public ApiResponse<GetViewResponseBody> createView(
      String databaseId, CreateUpdateViewRequestBody requestBody, String actingPrincipal) {
    throwIfErrorDatabaseId(databaseId);
    return ApiResponse.<GetViewResponseBody>builder()
        .httpStatus(HttpStatus.CREATED)
        .responseBody(ViewModelConstants.pointerResponse())
        .build();
  }

  @Override
  public ApiResponse<GetViewResponseBody> updateView(
      String databaseId,
      String viewId,
      CreateUpdateViewRequestBody requestBody,
      String actingPrincipal) {
    throwIfErrorDatabaseId(databaseId);
    HttpStatus httpStatus = PUT_CREATES_VIEW_ID.equals(viewId) ? HttpStatus.CREATED : HttpStatus.OK;
    return ApiResponse.<GetViewResponseBody>builder()
        .httpStatus(httpStatus)
        .responseBody(ViewModelConstants.pointerResponse())
        .build();
  }

  @Override
  public ApiResponse<Void> deleteView(String databaseId, String viewId, String actingPrincipal) {
    throwIfErrorDatabaseId(databaseId);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }
}
