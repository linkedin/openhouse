package com.linkedin.openhouse.tables.readbridge;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
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

  /** The tables service's default source: no column defaults. */
  private static final ColumnDefaultsSource NO_DEFAULTS = tableDto -> Collections.emptyMap();

  private static final String PREFIX = ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX;

  private static ReadBridgeConfigResolver resolverFor(ColumnDefaultsSource source) {
    return new ReadBridgeConfigResolver(source);
  }

  private static ColumnDefaultsSource oneDefault() {
    return tableDto -> Collections.singletonMap(5, TextNode.valueOf("US"));
  }

  /** A table carrying an explicit {@code required-capability.column-defaults} value. */
  private static TableDto tableWithOptIn(String value) {
    return TableDto.builder()
        .databaseId("db")
        .tableId("tbl")
        .tableProperties(
            Collections.singletonMap(
                ReadBridgeConfigResolver.COLUMN_DEFAULTS_CAPABILITY_PROP, value))
        .build();
  }

  /**
   * A buggy deployment source fails reads and writes rather than dropping every default, and as
   * itself: a server bug is neither the table's fault nor the request's.
   */
  @Test
  public void testSourceBugFailsReadsAndWritesAsItself() {
    IllegalStateException bug = new IllegalStateException("encoder exploded");
    ReadBridgeConfigResolver resolver =
        resolverFor(
            tableDto -> {
              throw bug;
            });
    TableDto table = tableWithOptIn("true");

    Assertions.assertSame(
        bug, Assertions.assertThrows(IllegalStateException.class, () -> resolver.resolve(table)));
    Assertions.assertSame(
        bug,
        Assertions.assertThrows(
            IllegalStateException.class, () -> resolver.incomingColumnDefaults(table)));
  }

  /**
   * A default the source cannot apply makes a stored table's config unavailable on GET. For a table
   * a write sends, the source's own exception reaches the caller, which decides what it means.
   */
  @Test
  public void testUnusableDefaultIsBlamedOnTheTableThatDeclaresIt() {
    TableDto table = tableWithOptIn("true");
    ColumnDefaultException reported =
        new ColumnDefaultException("column country: default is not a long");
    ReadBridgeConfigResolver resolver =
        resolverFor(
            tableDto -> {
              throw reported;
            });

    TableConfigUnavailableException onGet =
        Assertions.assertThrows(
            TableConfigUnavailableException.class, () -> resolver.resolve(table));
    Assertions.assertSame(reported, onGet.getCause());
    Assertions.assertEquals(reported.getMessage(), onGet.getMessage());
    Assertions.assertSame(
        reported,
        Assertions.assertThrows(
            ColumnDefaultException.class, () -> resolver.incomingColumnDefaults(table)));
  }

  /**
   * Gate 2: a table is bridged only when it carries {@code true}, and the source of a table that is
   * not opted in is never asked.
   */
  @Test
  public void testOnlyTheTablePropertyTurnsColumnDefaultsOn() {
    ColumnDefaultsSource source = mock(ColumnDefaultsSource.class);
    ReadBridgeConfigResolver resolver = resolverFor(source);

    Assertions.assertTrue(
        resolver
            .resolve(
                TableDto.builder()
                    .databaseId("db")
                    .tableId("tbl")
                    .tableProperties(Collections.emptyMap())
                    .build())
            .isEmpty());
    Assertions.assertTrue(resolver.resolve(tableWithOptIn("false")).isEmpty());
    Assertions.assertTrue(resolver.resolve(tableWithOptIn("sometimes")).isEmpty());
    // Deriving defaults can be expensive (a deployment may parse a schema); gate first.
    verifyNoInteractions(source);

    Assertions.assertEquals(
        "\"US\"", resolverFor(oneDefault()).resolve(tableWithOptIn(" TRUE ")).get(PREFIX + "5"));
  }

  /** A table that opts in, with a source that has nothing to stamp, gets empty config. */
  @Test
  public void testEmptyWhenSourceReturnsNoDefaults() {
    ColumnDefaultsSource emptySource = mock(ColumnDefaultsSource.class);
    Assertions.assertDoesNotThrow(
        () -> when(emptySource.defaults(any())).thenReturn(Collections.emptyMap()));

    Assertions.assertTrue(resolverFor(emptySource).resolve(tableWithOptIn("true")).isEmpty());
    Assertions.assertDoesNotThrow(() -> verify(emptySource).defaults(any()));
  }

  @Test
  public void testStampsColumnDefaultEntry() {
    ColumnDefaultsSource source = tableDto -> Collections.singletonMap(5, TextNode.valueOf("US"));
    Map<String, String> config = resolverFor(source).resolve(tableWithOptIn("true"));
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
    Map<String, String> config = resolverFor(source).resolve(tableWithOptIn("true"));
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
    when(resolver.resolve(eq(tableDto))).thenReturn(resolved);

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
        handlerWith(tableService, tablesMapper, resolverFor(NO_DEFAULTS));

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
