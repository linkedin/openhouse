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

  /** Open-source default source: supplies nothing, so the feature is inert. */
  private static final ColumnDefaultsSource NONE = ColumnDefaultsSource.NONE;

  private static final String PREFIX = ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX;

  /** A toggle that ramps everything, so a test isolates the encoder rather than the ramp. */
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

  /** A table carrying an explicit self-service opt-in/opt-out property. */
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

  /** Gate 1: no deployment-supplied source => inert, and crucially no toggle lookup at all. */
  @Test
  public void testInertAndSkipsToggleWhenNoSourceSupplied() {
    TableFeatureToggle toggle = mock(TableFeatureToggle.class);
    ReadBridgeConfigResolver resolver =
        new ReadBridgeConfigResolver(ColumnDefaultsSource.NONE, toggle);

    Assertions.assertTrue(resolver.resolve(mock(TableDto.class)).isEmpty());
    // The toggle is a remote HouseTables call on the table-load path; it must not be made.
    verifyNoInteractions(toggle);
  }

  /**
   * The ramp lookup is a blocking HouseTables call, and this is the table-load path — a path
   * toggles are not otherwise on. A HouseTables outage must degrade bridging, not fail reads. Sound
   * only because not bridging is exactly today's behavior; a capability where ignoring is unsafe
   * (deletion vectors) would have to fail the read instead.
   */
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

  /**
   * A buggy deployment source must not 500 GET. Not bridging is today's NULL, same as a toggle
   * outage.
   */
  @Test
  public void testSourceFailureDegradesInsteadOfFailingTheRead() {
    ColumnDefaultsSource exploding =
        tableDto -> {
          throw new IllegalStateException("encoder exploded");
        };

    Map<String, String> config =
        resolverFor(exploding).resolve(TableDto.builder().databaseId("db").tableId("tbl").build());

    Assertions.assertTrue(config.isEmpty());
  }

  /** Gate 3: a table the ramp has not activated is not bridged, and its source is never asked. */
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
    // Deriving defaults can be expensive (a deployment may parse a schema); gate first.
    verifyNoInteractions(source);
  }

  /** The self-service property opts a table in even when the server-managed ramp says no. */
  @Test
  public void testTablePropertyOptsInOverServerToggle() {
    // CALLS_REAL_METHODS so the override-honoring default reads the table property; stub the
    // server-side form so an accidental HTS call would return false.
    TableFeatureToggle toggle = mock(TableFeatureToggle.class, CALLS_REAL_METHODS);
    when(toggle.isFeatureActivated(anyString(), anyString(), anyString())).thenReturn(false);

    Map<String, String> config =
        new ReadBridgeConfigResolver(oneDefault(), toggle).resolve(tableWithOverride("true"));

    Assertions.assertEquals("\"US\"", config.get(PREFIX + "5"));
    // Explicit opt-in is decided from the table property alone; no HouseTables round-trip.
    verify(toggle, never()).isFeatureActivated(anyString(), anyString(), anyString());
  }

  /** ...and opts it out even when the server-managed ramp says yes. */
  @Test
  public void testTablePropertyOptsOutOverServerToggle() {
    Assertions.assertTrue(resolverFor(oneDefault()).resolve(tableWithOverride("false")).isEmpty());
  }

  /**
   * Source present and table ramped, but the source has nothing to stamp — still empty config. Not
   * the same as {@link ColumnDefaultsSource#NONE}: the toggle ran and the source was asked.
   */
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

  /**
   * The capability's feature id, its self-service property and its wire keys are one token. Pinned
   * as literals because all three are external contracts: the id is stored in HouseTables toggle
   * rules, the property is set on customer tables, and the prefix is mirrored by the client
   * decoder. Deriving them from each other keeps them consistent; asserting the literals keeps a
   * refactor from silently renaming all three at once.
   */
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
  public void testEmptyWhenNoColumnDefaults() {
    Assertions.assertTrue(resolverFor(NONE).resolve(mock(TableDto.class)).isEmpty());
  }

  @Test
  public void testStampsColumnDefaultEntry() {
    ColumnDefaultsSource source = tableDto -> Collections.singletonMap(5, TextNode.valueOf("US"));
    Map<String, String> config = resolverFor(source).resolve(mock(TableDto.class));
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
    Map<String, String> config = resolverFor(source).resolve(mock(TableDto.class));
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
