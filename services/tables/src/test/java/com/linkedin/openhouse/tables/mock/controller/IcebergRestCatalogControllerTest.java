package com.linkedin.openhouse.tables.mock.controller;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.tables.api.validator.TablesApiValidator;
import com.linkedin.openhouse.tables.controller.IcebergRestCatalogController;
import com.linkedin.openhouse.tables.controller.IcebergRestExceptionHandler;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.services.TablesService;
import java.util.Arrays;
import java.util.Collections;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@ExtendWith(MockitoExtension.class)
public class IcebergRestCatalogControllerTest {

  private MockMvc mvc;

  @Mock private OpenHouseInternalCatalog openHouseInternalCatalog;

  @Mock private TablesService tablesService;

  @Mock private TablesApiValidator tablesApiValidator;

  @InjectMocks private IcebergRestCatalogController icebergRestCatalogController;

  @BeforeEach
  public void setup() {
    mvc =
        MockMvcBuilders.standaloneSetup(icebergRestCatalogController)
            .setControllerAdvice(new IcebergRestExceptionHandler())
            .build();
  }

  @Test
  public void testConfig() throws Exception {
    mvc.perform(MockMvcRequestBuilders.get("/v1/config"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.defaults").exists())
        .andExpect(jsonPath("$.overrides").exists());
  }

  @Test
  public void testListTables() throws Exception {
    when(openHouseInternalCatalog.listTables(Namespace.of("db")))
        .thenReturn(
            Arrays.asList(
                TableIdentifier.of("db", "tb1"), TableIdentifier.of("db", "tb2")));

    mvc.perform(MockMvcRequestBuilders.get("/v1/namespaces/db/tables"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.identifiers[0].namespace[0]").value("db"))
        .andExpect(jsonPath("$.identifiers[0].name").value("tb1"))
        .andExpect(jsonPath("$.identifiers[1].name").value("tb2"));
  }

  @Test
  public void testLoadTable() throws Exception {
    when(tablesService.getTable(eq("db"), eq("tb1"), anyString()))
        .thenReturn(TableDto.builder().databaseId("db").tableId("tb1").build());

    TableMetadata metadata = testMetadata("hdfs://warehouse/db/tb1");
    TableOperations tableOperations = org.mockito.Mockito.mock(TableOperations.class);
    when(tableOperations.current()).thenReturn(metadata);
    BaseTable baseTable = new BaseTable(tableOperations, "openhouse.db.tb1");
    when(openHouseInternalCatalog.loadTable(TableIdentifier.of("db", "tb1")))
        .thenReturn(baseTable);

    mvc.perform(MockMvcRequestBuilders.get("/v1/namespaces/db/tables/tb1"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.metadata-location").value(metadata.metadataFileLocation()))
        .andExpect(jsonPath("$.metadata").exists());
  }

  @Test
  public void testLoadTableNotFound() throws Exception {
    when(tablesService.getTable(eq("db"), eq("missing"), anyString()))
        .thenThrow(new NoSuchUserTableException("db", "missing"));

    mvc.perform(MockMvcRequestBuilders.get("/v1/namespaces/db/tables/missing"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.error.code").value(404))
        .andExpect(jsonPath("$.error.type").value("NoSuchTableException"));
  }

  @Test
  public void testHeadTableExists() throws Exception {
    when(tablesService.getTable(eq("db"), eq("tb1"), anyString()))
        .thenReturn(TableDto.builder().databaseId("db").tableId("tb1").build());

    mvc.perform(MockMvcRequestBuilders.head("/v1/namespaces/db/tables/tb1"))
        .andExpect(status().isNoContent());
  }

  @Test
  public void testHeadTableNotFound() throws Exception {
    when(tablesService.getTable(eq("db"), eq("missing"), anyString()))
        .thenThrow(new NoSuchUserTableException("db", "missing"));

    mvc.perform(MockMvcRequestBuilders.head("/v1/namespaces/db/tables/missing"))
        .andExpect(status().isNotFound());
  }

  private static TableMetadata testMetadata(String location) {
    Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    return TableMetadata.newTableMetadata(
        schema,
        PartitionSpec.unpartitioned(),
        SortOrder.unsorted(),
        location,
        Collections.emptyMap());
  }
}
