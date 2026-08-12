package com.linkedin.openhouse.tables.readbridge;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class ReadBridgeConfigResolverTest {

  private static final ColumnDefaultsSource NONE = ColumnDefaultsSource.NONE;

  private static final String PREFIX = ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX;

  /** Isolates encoding from ramp. */
  private static final TableFeatureToggle ALL_ON =
      new TableFeatureToggle() {
        @Override
        public boolean isFeatureActivated(String databaseId, String tableId, String featureId) {
          return true;
        }
      };

  private static ReadBridgeConfigResolver resolverFor(ColumnDefaultsSource source) {
    return new ReadBridgeConfigResolver(source, ALL_ON);
  }

  private static ColumnDefaultsSource oneDefault() {
    return tableDto -> Collections.singletonMap(5, TextNode.valueOf("US"));
  }

  /** Table with an explicit {@code .enabled} property. */
  private static TableDto tableWithOverride(String value) {
    return TableDto.builder()
        .databaseId("db")
        .tableId("tbl")
        .tableProperties(
            Collections.singletonMap(
                ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID
                    + TableFeatureToggle.ENABLED_PROPERTY_SUFFIX,
                value))
        .build();
  }

  /** No source → no HouseTables call. */
  @Test
  public void testInertAndSkipsToggleWhenNoSourceSupplied() {
    TableFeatureToggle toggle = mock(TableFeatureToggle.class);
    ReadBridgeConfigResolver resolver =
        new ReadBridgeConfigResolver(ColumnDefaultsSource.NONE, toggle);

    Assertions.assertTrue(resolver.resolve(mock(TableDto.class)).isEmpty());
    verifyNoInteractions(toggle);
  }

  /** HouseTables down → unbridged, not a failed read. */
  @Test
  public void testToggleLookupFailureDegradesInsteadOfFailingTheRead() {
    TableFeatureToggle exploding =
        new TableFeatureToggle() {
          @Override
          public boolean isFeatureActivated(String databaseId, String tableId, String featureId) {
            throw new IllegalStateException("housetables is down");
          }
        };

    Map<String, String> config =
        new ReadBridgeConfigResolver(oneDefault(), exploding)
            .resolve(TableDto.builder().databaseId("db").tableId("tbl").build());

    Assertions.assertTrue(config.isEmpty());
  }

  /** Unramped table is not asked for defaults. */
  @Test
  public void testUnrampedTableIsNotBridgedAndSourceNotConsulted() {
    ColumnDefaultsSource source = mock(ColumnDefaultsSource.class);
    TableFeatureToggle allOff =
        new TableFeatureToggle() {
          @Override
          public boolean isFeatureActivated(String databaseId, String tableId, String featureId) {
            return false;
          }
        };

    Assertions.assertTrue(
        new ReadBridgeConfigResolver(source, allOff)
            .resolve(TableDto.builder().databaseId("db").tableId("tbl").build())
            .isEmpty());
    // Deriving defaults can be expensive; check the ramp first.
    verifyNoInteractions(source);
  }

  /** {@code .enabled=true} wins over a server-side off. */
  @Test
  public void testTablePropertyOptsInOverServerToggle() {
    // Real override method; stub HTS so an accidental call would return false.
    TableFeatureToggle toggle = mock(TableFeatureToggle.class, CALLS_REAL_METHODS);
    when(toggle.isFeatureActivated(anyString(), anyString(), anyString())).thenReturn(false);

    Map<String, String> config =
        new ReadBridgeConfigResolver(oneDefault(), toggle).resolve(tableWithOverride("true"));

    Assertions.assertEquals("\"US\"", config.get(PREFIX + "5"));
    verify(toggle, never()).isFeatureActivated(anyString(), anyString(), anyString());
  }

  /** {@code .enabled=false} wins over a server-side on. */
  @Test
  public void testTablePropertyOptsOutOverServerToggle() {
    Assertions.assertTrue(resolverFor(oneDefault()).resolve(tableWithOverride("false")).isEmpty());
  }

  /** Ramped table whose source has nothing to stamp. */
  @Test
  public void testEmptyWhenSourceReturnsNoDefaults() {
    ColumnDefaultsSource emptySource = mock(ColumnDefaultsSource.class);
    when(emptySource.defaults(any())).thenReturn(Collections.emptyMap());

    Assertions.assertTrue(
        resolverFor(emptySource)
            .resolve(TableDto.builder().databaseId("db").tableId("tbl").build())
            .isEmpty());
    verify(emptySource).defaults(any());
  }

  /** Id, property, and prefix are external contracts; keep them one token. */
  @Test
  public void testFeatureIdPropertyAndKeysAreOneToken() {
    Assertions.assertEquals(
        "read-bridge.column-default", ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID);
    Assertions.assertEquals(
        "read-bridge.column-default.enabled",
        ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID
            + TableFeatureToggle.ENABLED_PROPERTY_SUFFIX);
    Assertions.assertEquals(
        "openhouse.read-bridge.column-default.", ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX);
  }

  /** Ramp is per capability, not a blanket read-bridge id. */
  @Test
  public void testRolloutIdIsScopedToTheCapabilityNotTheMechanism() {
    Assertions.assertNotEquals("read-bridge", ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID);
    Assertions.assertNotEquals(
        "v3-read-bridge", ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID);
    Assertions.assertTrue(
        ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID.startsWith("read-bridge."));
  }

  @Test
  public void testEmptyWhenNoColumnDefaults() {
    Assertions.assertTrue(resolverFor(NONE).resolve(mock(TableDto.class)).isEmpty());
  }

  @Test
  public void testStampsColumnDefaultEntry() {
    ColumnDefaultsSource source = tableDto -> Collections.singletonMap(5, TextNode.valueOf("US"));
    Map<String, String> config = resolverFor(source).resolve(mock(TableDto.class));
    // "US" as Iceberg single-value JSON.
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
    Map<String, String> config = resolverFor(source).resolve(mock(TableDto.class));
    Assertions.assertEquals(2, config.size());
    Assertions.assertEquals("\"US\"", config.get(PREFIX + "5"));
    Assertions.assertEquals("0", config.get(PREFIX + "7"));
  }

  /** getTable puts resolver output on the response. */
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

  /** OSS source → empty config on getTable. */
  @Test
  public void testGetTableLeavesConfigEmptyWithNoColumnDefaults() {
    TablesService tableService = mock(TablesService.class);
    TablesMapper tablesMapper = mock(TablesMapper.class);

    TableDto tableDto = mock(TableDto.class);
    when(tableService.getTable(anyString(), anyString(), anyString())).thenReturn(tableDto);
    when(tablesMapper.toGetTableResponseBody(any()))
        .thenReturn(GetTableResponseBody.builder().tableId("tbl").databaseId("db").build());

    OpenHouseTablesApiHandler handler = handlerWith(tableService, tablesMapper, resolverFor(NONE));

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
