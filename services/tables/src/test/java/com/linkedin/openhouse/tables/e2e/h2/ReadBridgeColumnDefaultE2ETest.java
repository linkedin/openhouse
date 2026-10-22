package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.model.TableModelConstants.CLUSTER_NAME;
import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildCreateUpdateTableRequestBody;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildGetTableResponseBody;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.jayway.jsonpath.JsonPath;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultsSource;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeConfigResolver;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.SchemaParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

/**
 * HTTP GET stamps {@code config} from a stub {@link ColumnDefaultsSource} for tables that opt in,
 * and fails when the source cannot apply a declared default. A write against such a table fails the
 * same way: the stored table is at fault, not the request. A write whose own table declares such a
 * default is a bad request. Create and update responses omit {@code config}. Resolver unit tests
 * cover the same matrix.
 */
@SpringBootTest
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
@Import(ReadBridgeColumnDefaultE2ETest.StubDefaults.class)
@ContextConfiguration(
    initializers = {
      PropertyOverrideContextInitializer.class,
      AuthorizationPropertiesInitializer.class
    })
public class ReadBridgeColumnDefaultE2ETest {

  private static final String CONFIG_KEY = ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX + "2";
  private static final String ENABLED_PROP =
      ReadBridgeConfigResolver.COLUMN_DEFAULTS_CAPABILITY_PROP;

  private static final String UNUSABLE = "COLUMN_DEFAULT_UNUSABLE: column name has a bad default";

  /** A table carrying this property declares a default the stub source cannot apply. */
  private static final String DECLARES_UNUSABLE = "user.declares-unusable-default";

  /** Makes the stub source reject every table; reset after each test. */
  private static volatile boolean failing;

  @TestConfiguration
  static class StubDefaults {
    @Bean
    ColumnDefaultsSource stubColumnDefaults() {
      return tableDto -> {
        Map<String, String> properties = tableDto.getTableProperties();
        if (failing || (properties != null && properties.containsKey(DECLARES_UNUSABLE))) {
          throw new ColumnDefaultException(UNUSABLE);
        }
        return Collections.singletonMap(2, TextNode.valueOf("US"));
      };
    }
  }

  @Autowired private MockMvc mvc;
  @Autowired private StorageManager storageManager;

  private GetTableResponseBody created;

  @AfterEach
  public void tearDown() throws Exception {
    failing = false;
    if (created != null) {
      RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, created);
      created = null;
    }
  }

  @Test
  public void create_omitsConfigAndGetStampsColumnDefaultWhenEnabled() throws Exception {
    created = create(uniqueTable("prop_on"), Collections.singletonMap(ENABLED_PROP, "true"));

    MvcResult createdResult =
        RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);
    jsonPath("$.config").doesNotExist().match(createdResult);

    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']", is("\"US\"")))
        .andExpect(jsonPath("$.tableProperties['" + ENABLED_PROP + "']", is("true")));
  }

  @Test
  public void get_omitsColumnDefaultConfigWhenFeatureDisabled() throws Exception {
    created = create(uniqueTable("prop_off"), Collections.singletonMap(ENABLED_PROP, "false"));
    RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']").doesNotExist());
  }

  @Test
  public void get_unparseablePropertyFailsClosed() throws Exception {
    created = create(uniqueTable("bad_prop"), Collections.singletonMap(ENABLED_PROP, "sometimes"));
    RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']").doesNotExist());
  }

  /** GET fails instead of returning the table without its declared defaults. */
  @Test
  public void get_failsWhenSourceCannotApplyADefault() throws Exception {
    created = create(uniqueTable("unusable"), Collections.singletonMap(ENABLED_PROP, "true"));
    RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);
    failing = true;

    getTable()
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.message", containsString(UNUSABLE)));
  }

  /** A write is not blamed for a default the stored table already declares but cannot apply. */
  @Test
  public void put_failsAsServerErrorWhenStoredDefaultCannotBeApplied() throws Exception {
    created =
        create(uniqueTable("stored_unusable"), Collections.singletonMap(ENABLED_PROP, "true"));
    RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);
    GetTableResponseBody current =
        buildGetTableResponseBody(getTable().andExpect(status().isOk()).andReturn());
    current.getTableProperties().put("user.new", "value");
    failing = true;

    putTable(current)
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.message", containsString(UNUSABLE)));
  }

  /** A write whose own table declares a default that cannot be applied is a bad request. */
  @Test
  public void put_failsAsBadRequestWhenRequestDeclaresUnusableDefault() throws Exception {
    created =
        create(uniqueTable("declared_unusable"), Collections.singletonMap(ENABLED_PROP, "true"));
    RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);
    GetTableResponseBody current =
        buildGetTableResponseBody(getTable().andExpect(status().isOk()).andReturn());
    current.getTableProperties().put(DECLARES_UNUSABLE, "true");

    putTable(current)
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", containsString(UNUSABLE)));
  }

  /**
   * Iceberg 1.5 {@code sameSchema} includes {@code initial-default}, so this PUT cannot go through
   * {@code updateTableAndValidateResponse}. Create without overlay, PUT a matching handshake, then
   * assert persist dropped it.
   */
  @Test
  public void putWithMatchingOverlay_doesNotPersistInitialDefault() throws Exception {
    created = create(uniqueTable("persist_drop"), Collections.singletonMap(ENABLED_PROP, "true"));
    RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);

    MvcResult get = getTable().andExpect(status().isOk()).andReturn();
    GetTableResponseBody current = buildGetTableResponseBody(get);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(current.getSchema());
    for (JsonNode field : root.get("fields")) {
      if (field.get("id").asInt() == 2) {
        ((ObjectNode) field).put("initial-default", "US");
      }
    }
    GetTableResponseBody overlay =
        current.toBuilder().schema(mapper.writeValueAsString(root)).build();

    putTable(overlay).andExpect(status().isOk());

    MvcResult after =
        getTable()
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']", is("\"US\"")))
            .andReturn();
    String schemaJson = JsonPath.read(after.getResponse().getContentAsString(), "$.schema");
    assertNull(SchemaParser.fromJson(schemaJson).findField(2).initialDefault());
  }

  @Test
  public void policyBypassAllowsDisablingCommittedOptIn() throws Exception {
    created = create(uniqueTable("disable"), Collections.singletonMap(ENABLED_PROP, "true"));
    MvcResult initial =
        RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);
    CreateUpdateTableRequestBody update =
        buildCreateUpdateTableRequestBody(buildGetTableResponseBody(initial));
    Map<String, String> properties = new HashMap<>(update.getTableProperties());
    properties.put(ENABLED_PROP, "false");
    String body = update.toBuilder().tableProperties(properties).build().toJson();
    String path =
        String.format(
            ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/%s",
            created.getDatabaseId(),
            created.getTableId());
    mvc.perform(
            MockMvcRequestBuilders.put(path).contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", containsString("column-default policy override")));
    mvc.perform(
            MockMvcRequestBuilders.put(path)
                .header("X-OpenHouse-Dangerously-Bypass-Column-Default-Policy", "true")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.tableProperties['" + ENABLED_PROP + "']", is("false")));
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.tableProperties['" + ENABLED_PROP + "']", is("false")))
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']").doesNotExist());
  }

  private static GetTableResponseBody create(String tableId, Map<String, String> extraProps) {
    Map<String, String> props = new HashMap<>(GET_TABLE_RESPONSE_BODY.getTableProperties());
    props.putAll(extraProps);
    return GET_TABLE_RESPONSE_BODY
        .toBuilder()
        .tableId(tableId)
        .tableUri(CLUSTER_NAME + ".d1." + tableId)
        .tableProperties(props)
        .build();
  }

  private static String uniqueTable(String suffix) {
    return "rbcd_" + suffix + "_" + UUID.randomUUID().toString().substring(0, 8);
  }

  private ResultActions getTable() throws Exception {
    return mvc.perform(
        MockMvcRequestBuilders.get(
                String.format(
                    ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/%s",
                    created.getDatabaseId(),
                    created.getTableId()))
            .accept(MediaType.APPLICATION_JSON));
  }

  private ResultActions putTable(GetTableResponseBody table) throws Exception {
    return mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/%s",
                    table.getDatabaseId(),
                    table.getTableId()))
            .contentType(MediaType.APPLICATION_JSON)
            .content(buildCreateUpdateTableRequestBody(table).toJson())
            .accept(MediaType.APPLICATION_JSON));
  }
}
