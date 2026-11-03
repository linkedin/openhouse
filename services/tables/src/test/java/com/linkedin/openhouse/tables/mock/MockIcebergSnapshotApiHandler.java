package com.linkedin.openhouse.tables.mock;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.handler.IcebergSnapshotsApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
@Primary
public class MockIcebergSnapshotApiHandler implements IcebergSnapshotsApiHandler {

  @Override
  public ApiResponse<GetTableResponseBody> putIcebergSnapshots(
      String databaseId,
      String tableId,
      IcebergSnapshotsRequestBody icebergSnapshotRequestBody,
      String tableCreator) {
    switch (databaseId) {
      case "d201":
        return ApiResponse.<GetTableResponseBody>builder()
            .httpStatus(HttpStatus.CREATED)
            .responseBody(RequestConstants.TEST_GET_TABLE_RESPONSE_BODY)
            .build();
      case "d200":
        // Echo the request's table properties on the response so the audit aspect can read
        // committed state from result.getResponseBody().getTableProperties().
        return ApiResponse.<GetTableResponseBody>builder()
            .httpStatus(HttpStatus.OK)
            .responseBody(
                RequestConstants.TEST_GET_TABLE_RESPONSE_BODY
                    .toBuilder()
                    .tableProperties(
                        icebergSnapshotRequestBody.getCreateUpdateTableRequestBody() == null
                            ? null
                            : icebergSnapshotRequestBody
                                .getCreateUpdateTableRequestBody()
                                .getTableProperties())
                    .build())
            .build();
      case "d400":
        throw new RequestValidationFailureException();
      default:
        return null;
    }
  }
}
