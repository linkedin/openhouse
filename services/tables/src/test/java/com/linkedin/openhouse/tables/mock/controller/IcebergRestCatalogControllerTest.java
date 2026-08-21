package com.linkedin.openhouse.tables.mock.controller;

import static com.linkedin.openhouse.tables.api.handler.IcebergRestApiHandler.ICEBERG_REST_PREFIX;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.tables.api.handler.IcebergRestApiHandler;
import com.linkedin.openhouse.tables.controller.IcebergRestCatalogController;
import com.linkedin.openhouse.tables.controller.IcebergRestExceptionHandler;
import com.linkedin.openhouse.tables.controller.IcebergRestHttpMessageConverter;
import com.linkedin.openhouse.tables.generated.iceberg.model.CatalogConfig;
import com.linkedin.openhouse.tables.generated.iceberg.model.ListTablesResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@ExtendWith(MockitoExtension.class)
public class IcebergRestCatalogControllerTest {

  private MockMvc mvc;

  @Mock private IcebergRestApiHandler icebergRestApiHandler;

  @BeforeEach
  public void setup() {
    mvc =
        MockMvcBuilders.standaloneSetup(new IcebergRestCatalogController(icebergRestApiHandler))
            .setControllerAdvice(new IcebergRestExceptionHandler())
            .setMessageConverters(
                new IcebergRestHttpMessageConverter(),
                new MappingJackson2HttpMessageConverter(),
                new StringHttpMessageConverter())
            .build();
  }

  @Test
  public void testConfigAdvertisesSupportedEndpoints() throws Exception {
    when(icebergRestApiHandler.getConfig(nullable(String.class)))
        .thenReturn(
            new CatalogConfig(
                    Collections.singletonMap("prefix", ICEBERG_REST_PREFIX), Collections.emptyMap())
                .endpoints(
                    Collections.singletonList("GET /v1/{prefix}/namespaces/{namespace}/tables")));

    mvc.perform(MockMvcRequestBuilders.get("/v1/config"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.overrides.prefix").value(ICEBERG_REST_PREFIX))
        .andExpect(jsonPath("$.endpoints[0]").exists());
  }

  @Test
  public void testListTablesDelegatesTypedResponse() throws Exception {
    when(icebergRestApiHandler.listTables(
            eq(ICEBERG_REST_PREFIX), eq("db"), nullable(String.class), nullable(Integer.class)))
        .thenReturn(
            new ListTablesResponse()
                .identifiers(
                    new LinkedHashSet<>(
                        Arrays.asList(
                            TableIdentifier.of("db", "tb1"), TableIdentifier.of("db", "tb2"))))
                .nextPageToken("next"));

    mvc.perform(
            MockMvcRequestBuilders.get("/v1/{prefix}/namespaces/db/tables", ICEBERG_REST_PREFIX)
                .param("pageSize", "2"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.identifiers[0].namespace[0]").value("db"))
        .andExpect(jsonPath("$.identifiers[1].name").value("tb2"))
        .andExpect(jsonPath("$.next-page-token").value("next"));
  }

  @Test
  public void testLoadTableDelegatesTypedResponse() throws Exception {
    TableMetadata metadata = testMetadata("hdfs://warehouse/db/tb1");
    when(icebergRestApiHandler.loadTable(
            eq(ICEBERG_REST_PREFIX),
            eq("db"),
            eq("tb1"),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class)))
        .thenReturn(LoadTableResponse.builder().withTableMetadata(metadata).build());

    mvc.perform(
            MockMvcRequestBuilders.get(
                "/v1/{prefix}/namespaces/db/tables/tb1", ICEBERG_REST_PREFIX))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.metadata-location").value(metadata.metadataFileLocation()))
        .andExpect(jsonPath("$.metadata").exists());
  }

  @Test
  public void testTypedNotFoundError() throws Exception {
    when(icebergRestApiHandler.loadTable(
            eq(ICEBERG_REST_PREFIX),
            eq("db"),
            eq("missing"),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class)))
        .thenThrow(new NoSuchTableException("Table does not exist"));

    mvc.perform(
            MockMvcRequestBuilders.get(
                "/v1/{prefix}/namespaces/db/tables/missing", ICEBERG_REST_PREFIX))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.error.code").value(404))
        .andExpect(jsonPath("$.error.type").value("NoSuchTableException"));
  }

  @Test
  public void testForbiddenErrorIsSanitized() throws Exception {
    when(icebergRestApiHandler.loadTable(
            eq(ICEBERG_REST_PREFIX),
            eq("db"),
            eq("private"),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class)))
        .thenThrow(new AccessDeniedException("sensitive policy details"));

    mvc.perform(
            MockMvcRequestBuilders.get(
                "/v1/{prefix}/namespaces/db/tables/private", ICEBERG_REST_PREFIX))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.error.message").value("Access denied"))
        .andExpect(jsonPath("$.error.type").value("ForbiddenException"));
  }

  @Test
  public void testHeadDelegates() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.head(
                "/v1/{prefix}/namespaces/db/tables/tb1", ICEBERG_REST_PREFIX))
        .andExpect(status().isNoContent());

    verify(icebergRestApiHandler).tableExists(ICEBERG_REST_PREFIX, "db", "tb1");
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
