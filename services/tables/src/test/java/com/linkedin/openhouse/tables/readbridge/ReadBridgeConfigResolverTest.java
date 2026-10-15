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

  /** Open-source default source: supplies nothing, so the feature is inert. */
  private static final ColumnDefaultsSource NONE = ColumnDefaultsSource.NONE;

  private static final String PREFIX = ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX;

  private static ReadBridgeConfigResolver resolverFor(ColumnDefaultsSource source) {
    return new ReadBridgeConfigResolver(source);
  }

  private static ColumnDefaultsSource oneDefault() {
    return tableDto -> Collections.singletonMap(5, TextNode.valueOf("US"));
  }

  /** A table carrying an explicit {@code read-bridge.column-default.enabled} value. */
  private static TableDto tableWithOverride(String value) {
    return TableDto.builder()
        .databaseId("db")
        .tableId("tbl")
        .tableProperties(
            Collections.singletonMap(ReadBridgeConfigResolver.COLUMN_DEFAULT_ENABLED_PROP, value))
        .build();
  }

  /** Gate 1: no deployment-supplied source => inert, even for a table that opts in. */
  @Test
  public void testInertWhenNoSourceSupplied() {
    Assertions.assertTrue(resolverFor(NONE).resolve(tableWithOverride("true")).isEmpty());
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
    TableDto table = tableWithOverride("true");

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
    TableDto table = tableWithOverride("true");
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
   * Gate 2: a table is bridged only when it carries {@code true}. No HouseTables rule turns column
   * defaults on, and the source of a table that is not opted in is never asked.
   */
  @Test
  public void testOnlyTheTablePropertyTurnsColumnDefaultsOn() {
    ColumnDefaultsSource source = mock(ColumnDefaultsSource.class);
    ReadBridgeConfigResolver resolver = resolverFor(source);

    Assertions.assertTrue(
        resolver.resolve(TableDto.builder().databaseId("db").tableId("tbl").build()).isEmpty());
    Assertions.assertTrue(resolver.resolve(tableWithOverride("false")).isEmpty());
    Assertions.assertTrue(resolver.resolve(tableWithOverride("sometimes")).isEmpty());
    // Deriving defaults can be expensive (a deployment may parse a schema); gate first.
    verifyNoInteractions(source);

    Assertions.assertEquals(
        "\"US\"", resolverFor(oneDefault()).resolve(tableWithOverride(" TRUE ")).get(PREFIX + "5"));
  }

  /**
   * Source present and table opted in, but the source has nothing to stamp — still empty config.
   * Not the same as {@link ColumnDefaultsSource#NONE}: the source was asked.
   */
  @Test
  public void testEmptyWhenSourceReturnsNoDefaults() {
    ColumnDefaultsSource emptySource = mock(ColumnDefaultsSource.class);
    Assertions.assertDoesNotThrow(
        () -> when(emptySource.defaults(any())).thenReturn(Collections.emptyMap()));

    Assertions.assertTrue(resolverFor(emptySource).resolve(tableWithOverride("true")).isEmpty());
    Assertions.assertDoesNotThrow(() -> verify(emptySource).defaults(any()));
  }

  /**
   * The capability's feature id, its self-service property and its wire keys are one token. Pinned
   * as literals because all three are external contracts: the property is set on customer tables,
   * and the prefix is mirrored by the client decoder. Deriving them from each other keeps them
   * consistent; asserting the literals keeps a refactor from silently renaming all three at once.
   */
  @Test
  public void testFeatureIdPropertyAndKeysAreOneToken() {
    Assertions.assertEquals(
        "read-bridge.column-default", ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID);
    Assertions.assertEquals(
        "read-bridge.column-default.enabled", ReadBridgeConfigResolver.COLUMN_DEFAULT_ENABLED_PROP);
    Assertions.assertEquals(
        "openhouse.read-bridge.column-default.", ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX);
  }

  /**
   * Rollout is per capability, never for read-bridge as a whole: capabilities share only the
   * transport. A table opted out of column defaults must not thereby be opted out of a capability
   * added later, and vice versa. Pinned because the id is baked into a customer-set property, so
   * splitting it after the fact means a migration.
   *
   * <p>The bare "read-bridge" id staying unclaimed is also the room a future superset ramp (e.g.
   * "v3-read-bridge", activating every capability at once) needs in order to exist without
   * colliding with a capability's own id.
   */
  @Test
  public void testRolloutIdIsScopedToTheCapabilityNotTheMechanism() {
    Assertions.assertNotEquals("read-bridge", ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID);
    Assertions.assertNotEquals(
        "v3-read-bridge", ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID);
    Assertions.assertTrue(
        ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID.startsWith("read-bridge."));
  }

  @Test
  public void testStampsColumnDefaultEntry() {
    ColumnDefaultsSource source = tableDto -> Collections.singletonMap(5, TextNode.valueOf("US"));
    Map<String, String> config = resolverFor(source).resolve(tableWithOverride("true"));
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
    Map<String, String> config = resolverFor(source).resolve(tableWithOverride("true"));
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
