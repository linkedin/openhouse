package com.linkedin.openhouse.tables.mock.api.handler.impl;

import static com.linkedin.openhouse.tables.api.handler.IcebergRestApiHandler.ICEBERG_REST_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.tables.api.handler.TablesApiHandler;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseIcebergRestApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.generated.iceberg.model.ListTablesResponse;
import java.util.Collections;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
public class OpenHouseIcebergRestApiHandlerTest {

  @Mock private TablesApiHandler tablesApiHandler;
  @Mock private OpenHouseInternalCatalog openHouseInternalCatalog;

  private OpenHouseIcebergRestApiHandler handler;

  @BeforeEach
  void setUp() {
    handler = new OpenHouseIcebergRestApiHandler(tablesApiHandler, openHouseInternalCatalog);
  }

  @Test
  void configAdvertisesOnlyImplementedEndpoints() {
    assertThat(handler.getConfig("openhouse").getOverrides())
        .containsEntry("prefix", ICEBERG_REST_PREFIX);
    assertThat(handler.getConfig("openhouse").getEndpoints())
        .containsExactly(
            "GET /v1/{prefix}/namespaces/{namespace}/tables",
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}");
  }

  @Test
  void listTablesUsesOpaquePaginationToken() {
    when(tablesApiHandler.searchTables("db", 0, 1, "tableId"))
        .thenReturn(pageResponse("db", "t1", 0, 1, 2));

    ListTablesResponse firstPage = handler.listTables(ICEBERG_REST_PREFIX, "db", null, 1);

    assertThat(firstPage.getIdentifiers()).containsExactly(TableIdentifier.of("db", "t1"));
    assertThat(firstPage.getNextPageToken()).isNotBlank();

    when(tablesApiHandler.searchTables("db", 1, 1, "tableId"))
        .thenReturn(pageResponse("db", "t2", 1, 1, 2));
    ListTablesResponse secondPage =
        handler.listTables(ICEBERG_REST_PREFIX, "db", firstPage.getNextPageToken(), null);

    assertThat(secondPage.getIdentifiers()).containsExactly(TableIdentifier.of("db", "t2"));
    assertThat(secondPage.getNextPageToken()).isNull();
  }

  @Test
  void rejectsInvalidPrefixAndPageToken() {
    assertThatThrownBy(() -> handler.listTables("other", "db", null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("prefix");
    assertThatThrownBy(() -> handler.listTables(ICEBERG_REST_PREFIX, "db", "invalid", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("page token");
  }

  @Test
  void rejectsUnsupportedSnapshotProjection() {
    assertThatThrownBy(
            () -> handler.loadTable(ICEBERG_REST_PREFIX, "db", "t1", null, null, "refs", null))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("snapshots=refs");
  }

  @Test
  void loadTableReusesExistingReadHandlerBeforeCatalogLoad() {
    when(tablesApiHandler.getTable(eq("db"), eq("t1"), eq("undefined")))
        .thenReturn(
            ApiResponse.<GetTableResponseBody>builder()
                .httpStatus(HttpStatus.OK)
                .responseBody(GetTableResponseBody.builder().databaseId("db").tableId("t1").build())
                .build());
    TableMetadata metadata = testMetadata("hdfs://warehouse/db/t1");
    TableOperations operations = org.mockito.Mockito.mock(TableOperations.class);
    when(operations.current()).thenReturn(metadata);
    when(openHouseInternalCatalog.loadTable(TableIdentifier.of("db", "t1")))
        .thenReturn(new BaseTable(operations, "openhouse.db.t1"));

    assertThat(
            handler
                .loadTable(ICEBERG_REST_PREFIX, "db", "t1", null, null, "all", null)
                .tableMetadata()
                .location())
        .isEqualTo(metadata.location());
    verify(tablesApiHandler).getTable("db", "t1", "undefined");
  }

  private static ApiResponse<GetAllTablesResponseBody> pageResponse(
      String databaseId, String tableId, int page, int size, int total) {
    GetTableResponseBody table =
        GetTableResponseBody.builder().databaseId(databaseId).tableId(tableId).build();
    return ApiResponse.<GetAllTablesResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAllTablesResponseBody.builder()
                .pageResults(
                    new PageImpl<>(
                        Collections.singletonList(table), PageRequest.of(page, size), total))
                .build())
        .build();
  }

  private static TableMetadata testMetadata(String location) {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    return TableMetadata.newTableMetadata(
        schema,
        PartitionSpec.unpartitioned(),
        SortOrder.unsorted(),
        location,
        Collections.emptyMap());
  }
}
