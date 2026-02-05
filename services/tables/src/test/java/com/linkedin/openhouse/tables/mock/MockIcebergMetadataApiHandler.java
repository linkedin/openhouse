package com.linkedin.openhouse.tables.mock;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.tables.api.handler.IcebergMetadataApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetIcebergMetadataResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetMetadataDiffResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableDataResponseBody;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
@Primary
public class MockIcebergMetadataApiHandler implements IcebergMetadataApiHandler {

  @Override
  public ApiResponse<GetIcebergMetadataResponseBody> getIcebergMetadata(
      String databaseId, String tableId, String actingPrincipal) {
    switch (databaseId) {
      case "d200":
        return ApiResponse.<GetIcebergMetadataResponseBody>builder()
            .httpStatus(HttpStatus.OK)
            .responseBody(
                GetIcebergMetadataResponseBody.builder()
                    .tableId(tableId)
                    .databaseId(databaseId)
                    .currentMetadata("{\"format-version\":2,\"table-uuid\":\"test-uuid\"}")
                    .metadataHistory(
                        Arrays.asList(
                            GetIcebergMetadataResponseBody.MetadataVersion.builder()
                                .version(1)
                                .file("v1.metadata.json")
                                .timestamp(1651002318265L)
                                .location("s3://bucket/metadata/v1.metadata.json")
                                .build()))
                    .metadataLocation("s3://bucket/metadata/v2.metadata.json")
                    .snapshots("[{\"snapshot-id\":1,\"timestamp-ms\":1651002318265}]")
                    .partitions("[]")
                    .currentSnapshotId("1")
                    .build())
            .build();
      case "d404":
        throw new NoSuchUserTableException(databaseId, tableId);
      default:
        return null;
    }
  }

  @Override
  public ApiResponse<GetMetadataDiffResponseBody> getMetadataDiff(
      String databaseId, String tableId, String metadataFile, String actingPrincipal) {
    switch (databaseId) {
      case "d200":
        return ApiResponse.<GetMetadataDiffResponseBody>builder()
            .httpStatus(HttpStatus.OK)
            .responseBody(
                GetMetadataDiffResponseBody.builder()
                    .tableId(tableId)
                    .databaseId(databaseId)
                    .currentMetadata(
                        "{\"format-version\":2,\"table-uuid\":\"test-uuid\",\"current-snapshot-id\":2}")
                    .currentTimestamp(1651002318265L)
                    .currentMetadataLocation(metadataFile)
                    .previousMetadata(
                        "{\"format-version\":2,\"table-uuid\":\"test-uuid\",\"current-snapshot-id\":1}")
                    .previousTimestamp(1651002218265L)
                    .previousMetadataLocation("s3://bucket/metadata/v1.metadata.json")
                    .isFirstCommit(false)
                    .build())
            .build();
      case "d404":
        throw new NoSuchUserTableException(databaseId, tableId);
      default:
        return null;
    }
  }

  @Override
  public ApiResponse<GetTableDataResponseBody> getTableData(
      String databaseId, String tableId, int limit, String actingPrincipal) {
    switch (databaseId) {
      case "d200":
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1);
        row1.put("name", "test1");
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 2);
        row2.put("name", "test2");
        return ApiResponse.<GetTableDataResponseBody>builder()
            .httpStatus(HttpStatus.OK)
            .responseBody(
                GetTableDataResponseBody.builder()
                    .tableId(tableId)
                    .databaseId(databaseId)
                    .schema("{\"type\":\"struct\",\"fields\":[]}")
                    .rows(Arrays.asList(row1, row2))
                    .totalRowsFetched(2)
                    .hasMore(false)
                    .build())
            .build();
      case "d404":
        throw new NoSuchUserTableException(databaseId, tableId);
      default:
        return null;
    }
  }
}
