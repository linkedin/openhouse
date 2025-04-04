package com.linkedin.openhouse.tables.mock;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.InvalidSchemaEvolutionException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.common.exception.OpenHouseCommitStateUnknownException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.UnprocessableEntityException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.tables.api.handler.TablesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.stereotype.Component;

@Component
@Primary
public class MockTablesApiHandler implements TablesApiHandler {
  @Override
  public ApiResponse<GetTableResponseBody> getTable(
      String databaseId, String tableId, String actingPrincipal) {
    switch (databaseId) {
      case "d200":
        return ApiResponse.<GetTableResponseBody>builder()
            .httpStatus(HttpStatus.OK)
            .responseBody(RequestConstants.TEST_GET_TABLE_RESPONSE_BODY)
            .build();
      case "d404":
        throw new NoSuchUserTableException(databaseId, tableId);
      case "dnullpointer":
        throw new NullPointerException(); // test for exception handler
      default:
        return null;
    }
  }

  @Override
  public ApiResponse<GetAllTablesResponseBody> searchTables(String databaseId) {
    switch (databaseId) {
      case "d200":
        return ApiResponse.<GetAllTablesResponseBody>builder()
            .httpStatus(HttpStatus.OK)
            .responseBody(RequestConstants.TEST_GET_ALL_TABLES_RESPONSE_BODY)
            .build();
      case "d404":
        throw new NoSuchUserTableException(databaseId, "");
      default:
        return null;
    }
  }

  @Override
  public ApiResponse<GetTableResponseBody> createTable(
      String databaseId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody,
      String tableCreator) {
    // Mock responses for different databaseIds to emulate different responses.
    switch (databaseId) {
      case "d200":
        return ApiResponse.<GetTableResponseBody>builder()
            .httpStatus(HttpStatus.CREATED)
            .responseBody(RequestConstants.TEST_GET_TABLE_RESPONSE_BODY)
            .build();
      case "d400":
        throw new RequestValidationFailureException();
      case "d404":
        throw new NoSuchUserTableException(databaseId, createUpdateTableRequestBody.getTableId());
      case "d409":
        throw new AlreadyExistsException(
            "Table", String.format("%s.%s", databaseId, createUpdateTableRequestBody.getTableId()));
      case "dException":
        throwTableException(createUpdateTableRequestBody.getTableId());
      default:
        return null;
    }
  }

  @Override
  public ApiResponse<GetTableResponseBody> updateTable(
      String databaseId,
      String tableId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody,
      String tableCreatorUpdator) {
    switch (databaseId) {
      case "d200":
        return ApiResponse.<GetTableResponseBody>builder()
            .httpStatus(HttpStatus.OK)
            .responseBody(RequestConstants.TEST_GET_TABLE_RESPONSE_BODY)
            .build();
      case "d201":
        return ApiResponse.<GetTableResponseBody>builder()
            .httpStatus(HttpStatus.CREATED)
            .responseBody(RequestConstants.TEST_GET_TABLE_RESPONSE_BODY)
            .build();
      case "d400":
        throw new RequestValidationFailureException();
      case "d404":
        throw new NoSuchUserTableException(databaseId, tableId);
      case "dException":
        throwTableException(createUpdateTableRequestBody.getTableId());
      default:
        return null;
    }
  }

  @Override
  public ApiResponse<Void> deleteTable(String databaseId, String tableId, String actingPrincipal) {
    switch (databaseId) {
      case "d204":
        return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
      case "d400":
        throw new RequestValidationFailureException();
      case "d404":
        throw new NoSuchUserTableException(databaseId, tableId);
      default:
        return null;
    }
  }

  @Override
  public ApiResponse<Void> updateAclPolicies(
      String databaseId,
      String tableId,
      UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody,
      String actingPrincipal) {
    switch (databaseId) {
      case "d204":
        return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
      case "d400":
        throw new RequestValidationFailureException();
      case "d404":
        throw new NoSuchUserTableException(databaseId, tableId);
      case "d503":
        throw new AuthorizationServiceException("Internal authz service not available");
      case "d422":
        throw new UnprocessableEntityException("Unprocessable entity");
      default:
        return null;
    }
  }

  @Override
  public ApiResponse<GetAclPoliciesResponseBody> getAclPolicies(
      String databaseId, String tableId, String actingPrincipal) {
    switch (databaseId) {
      case "d200":
        return ApiResponse.<GetAclPoliciesResponseBody>builder()
            .httpStatus(HttpStatus.OK)
            .responseBody(RequestConstants.TEST_GET_ACL_POLICIES_RESPONSE_BODY)
            .build();
      case "d400":
        throw new RequestValidationFailureException();
      case "d404":
        throw new NoSuchUserTableException(databaseId, "");
      case "d503":
        throw new AuthorizationServiceException("Internal authz service not available");
      default:
        return null;
    }
  }

  @Override
  public ApiResponse<GetAclPoliciesResponseBody> getAclPoliciesForUserPrincipal(
      String databaseId, String tableId, String actingPrincipal, String userPrincipal) {
    switch (databaseId) {
      case "d200":
        return ApiResponse.<GetAclPoliciesResponseBody>builder()
            .httpStatus(HttpStatus.OK)
            .responseBody(RequestConstants.TEST_GET_ACL_POLICIES_RESPONSE_BODY)
            .build();
      case "d400":
        throw new RequestValidationFailureException();
      case "d404":
        throw new NoSuchUserTableException(databaseId, "");
      case "d503":
        throw new AuthorizationServiceException("Internal authz service not available");
      default:
        return null;
    }
  }

  @Override
  public ApiResponse<Void> createLock(
      String databaseId,
      String tableId,
      CreateUpdateLockRequestBody updateLockRequestBody,
      String tableCreatorUpdator) {
    switch (databaseId) {
      case "d201":
        return ApiResponse.<Void>builder().httpStatus(HttpStatus.CREATED).build();
      case "d400":
        throw new RequestValidationFailureException();
      case "d404":
        throw new NoSuchUserTableException(databaseId, tableId);
      case "d503":
        throw new AuthorizationServiceException("Internal authz service not available");
      case "d422":
        throw new UnprocessableEntityException("Unprocessable entity");
      default:
        return null;
    }
  }

  @Override
  public ApiResponse<Void> deleteLock(
      String databaseId, String tableId, String extractAuthenticatedUserPrincipal) {
    switch (databaseId) {
      case "d204":
        return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
      case "d400":
        throw new RequestValidationFailureException();
      case "d404":
        throw new NoSuchUserTableException(databaseId, tableId);
      default:
        return null;
    }
  }

  private void throwTableException(String tableId) {
    switch (tableId) {
      case "entityconcurrentmodificationexception":
        throw new EntityConcurrentModificationException(
            tableId, "Entity Concurrent Modification Exception", new RuntimeException());
      case "openhousecommitstateunknownexception":
        throw new OpenHouseCommitStateUnknownException(
            tableId, "Open House Commit State Unknown Exception", new RuntimeException());
      case "requestvalidationfailureexception":
        throw new RequestValidationFailureException("Request Validation Failure Exception");
      case "unprocessableentityexception":
        throw new UnprocessableEntityException("Unprocessable Entity Exception");
      case "alreadyexistsexception":
        throw new AlreadyExistsException("tableType", "tableId", new RuntimeException());
      case "orgapacheicebergexceptionsalreadyexistsexception":
        throw new org.apache.iceberg.exceptions.AlreadyExistsException("Already Exists Exception");
      case "invalidschemaevolutionexception":
        throw new InvalidSchemaEvolutionException(
            "Invalid Schema Evolution Exception", "oldSchema", "newSchema");
      case "unsupportedclientoperationexception":
        throw new UnsupportedClientOperationException(
            UnsupportedClientOperationException.Operation.ALTER_TABLE_TYPE,
            "Unsupported Client Operation Exception");
      case "accessdeniedexception":
        throw new AccessDeniedException("Access Denied Exception");
      case "illegalstateexception":
        throw new IllegalStateException("Illegal State Exception");
      case "authorizationserviceexception":
        throw new AuthorizationServiceException("Authorization Service Exception");
      case "exception":
        throw new RuntimeException(tableId + "Exception");
      default:
        throw new RuntimeException("Unknown tableId: " + tableId);
    }
  }
}
