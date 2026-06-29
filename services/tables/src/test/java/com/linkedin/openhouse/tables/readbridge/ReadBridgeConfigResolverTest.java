package com.linkedin.openhouse.tables.readbridge;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseTablesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.api.validator.TablesApiValidator;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.services.TablesService;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class ReadBridgeConfigResolverTest {

  /** Open-source default source: supplies nothing, so the feature is inert. */
  private static final ColumnDefaultsSource NONE = tableDto -> Collections.emptyMap();

  private static final String PREFIX = ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX;

  @Test
  public void testEmptyWhenNoColumnDefaults() {
    Assertions.assertTrue(
        new ReadBridgeConfigResolver(NONE).resolve("db", "tbl", mock(TableDto.class)).isEmpty());
  }

  @Test
  public void testStampsColumnDefaultEntry() {
    ColumnDefaultsSource source = tableDto -> Collections.singletonMap(5, TextNode.valueOf("US"));
    Map<String, String> config =
        new ReadBridgeConfigResolver(source).resolve("db", "tbl", mock(TableDto.class));
    // value is the single-value JSON for the default ("US" -> "\"US\"").
    Assertions.assertEquals("\"US\"", config.get(PREFIX + "5"));
  }

  @Test
  public void testStampsAllColumnDefaultsAsSeparateEntries() {
    ColumnDefaultsSource source =
        tableDto -> {
          Map<Integer, JsonNode> defaults = new LinkedHashMap<>();
          defaults.put(5, TextNode.valueOf("US"));
          defaults.put(7, IntNode.valueOf(0));
          return defaults;
        };
    Map<String, String> config =
        new ReadBridgeConfigResolver(source).resolve("db", "tbl", mock(TableDto.class));
    Assertions.assertEquals(2, config.size());
    Assertions.assertEquals("\"US\"", config.get(PREFIX + "5"));
    Assertions.assertEquals("0", config.get(PREFIX + "7"));
  }

  /** getTable stamps the resolver's config onto the response body. */
  @Test
  public void testGetTableStampsResolvedConfig() {
    TablesService tableService = mock(TablesService.class);
    TablesMapper tablesMapper = mock(TablesMapper.class);
    ReadBridgeConfigResolver resolver = mock(ReadBridgeConfigResolver.class);

    TableDto tableDto = mock(TableDto.class);
    when(tableService.getTable("db", "tbl", "principal")).thenReturn(tableDto);
    when(tablesMapper.toGetTableResponseBody(tableDto))
        .thenReturn(GetTableResponseBody.builder().tableId("tbl").databaseId("db").build());

    Map<String, String> resolved = Collections.singletonMap(PREFIX + "5", "\"US\"");
    when(resolver.resolve(eq("db"), eq("tbl"), eq(tableDto))).thenReturn(resolved);

    OpenHouseTablesApiHandler handler = handlerWith(tableService, tablesMapper, resolver);

    ApiResponse<GetTableResponseBody> response = handler.getTable("db", "tbl", "principal");

    Assertions.assertSame(resolved, response.getResponseBody().getConfig());
  }

  /** With the behaviorless open-source source wired in, getTable leaves config empty. */
  @Test
  public void testGetTableLeavesConfigEmptyWithNoColumnDefaults() {
    TablesService tableService = mock(TablesService.class);
    TablesMapper tablesMapper = mock(TablesMapper.class);

    TableDto tableDto = mock(TableDto.class);
    when(tableService.getTable(anyString(), anyString(), anyString())).thenReturn(tableDto);
    when(tablesMapper.toGetTableResponseBody(any()))
        .thenReturn(GetTableResponseBody.builder().tableId("tbl").databaseId("db").build());

    OpenHouseTablesApiHandler handler =
        handlerWith(tableService, tablesMapper, new ReadBridgeConfigResolver(NONE));

    ApiResponse<GetTableResponseBody> response = handler.getTable("db", "tbl", "principal");

    Assertions.assertTrue(response.getResponseBody().getConfig().isEmpty());
  }

  private OpenHouseTablesApiHandler handlerWith(
      TablesService tableService, TablesMapper tablesMapper, ReadBridgeConfigResolver resolver) {
    OpenHouseTablesApiHandler handler = new OpenHouseTablesApiHandler();
    ReflectionTestUtils.setField(handler, "tablesApiValidator", mock(TablesApiValidator.class));
    ReflectionTestUtils.setField(handler, "tableService", tableService);
    ReflectionTestUtils.setField(handler, "tablesMapper", tablesMapper);
    ReflectionTestUtils.setField(handler, "readBridgeConfigResolver", resolver);
    return handler;
  }
}
