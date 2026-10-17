package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.model.TableModelConstants.CLUSTER_NAME;
import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildCreateUpdateTableRequestBody;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildGetTableResponseBody;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.IcebergSnapshotsModelTestUtilities;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultsSource;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeConfigResolver;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
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
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

/**
 * HTTP GET stamps {@code config} from a stub {@link ColumnDefaultsSource} for tables that opt in,
 * and fails when the source cannot apply a declared default. A write against such a table fails the
 * same way: the stored table is at fault, not the request. A write whose own table declares such a
 * default is a bad request. Create and update responses omit {@code config}. Resolver unit tests
 * cover the same matrix.
 */
@SpringBootTest(properties = "cluster.read-bridge.column-default.minimum-client-version=0.5.100")
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
  private static final String ENABLED_PROP = ReadBridgeConfigResolver.COLUMN_DEFAULT_ENABLED_PROP;

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
  @Autowired private Catalog catalog;

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

    MvcResult createdResult = createTable();
    jsonPath("$.config").doesNotExist().match(createdResult);

    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']", is("\"US\"")))
        .andExpect(jsonPath("$.tableProperties['" + ENABLED_PROP + "']", is("true")));
  }

  @Test
  public void get_omitsColumnDefaultConfigWhenFeatureDisabled() throws Exception {
    created = create(uniqueTable("prop_off"), Collections.singletonMap(ENABLED_PROP, "false"));
    createTable();
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']").doesNotExist());
  }

  @Test
  public void get_unparseablePropertyFailsClosed() throws Exception {
    created = create(uniqueTable("bad_prop"), Collections.singletonMap(ENABLED_PROP, "sometimes"));
    createTable();
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']").doesNotExist());
  }

  /** GET fails instead of returning the table without its declared defaults. */
  @Test
  public void get_failsWhenSourceCannotApplyADefault() throws Exception {
    created = create(uniqueTable("unusable"), Collections.singletonMap(ENABLED_PROP, "true"));
    createTable();
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
    createTable();
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
    createTable();
    GetTableResponseBody current =
        buildGetTableResponseBody(getTable().andExpect(status().isOk()).andReturn());
    current.getTableProperties().put(DECLARES_UNUSABLE, "true");

    putTable(current)
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", containsString(UNUSABLE)));
  }

  /**
   * Iceberg 1.5 {@code sameSchema} includes {@code initial-default}, so this PUT cannot go through
   * {@code updateTableAndValidateResponse}. Create without overlay, PUT the overlay, then assert
   * persist dropped it.
   */
  @Test
  public void putWithMatchingOverlay_doesNotPersistInitialDefault() throws Exception {
    created = create(uniqueTable("persist_drop"), Collections.singletonMap(ENABLED_PROP, "true"));
    createTable();

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
  public void incompatibleClientsCannotLoadOptedInTables() throws Exception {
    created = create(uniqueTable("client_gate"), Collections.singletonMap(ENABLED_PROP, "true"));
    createTable();
    String path = "/v1/databases/" + created.getDatabaseId() + "/tables/" + created.getTableId();
    for (String[] client :
        new String[][] {
          {"trino", "0.5.100"},
          {"spark", "0.5.99"},
          {"spark", "unknown"},
          {"spark", "4.2.100"},
          {"spark", "0.5.100-SNAPSHOT"}
        }) {
      mvc.perform(
              MockMvcRequestBuilders.get(path)
                  .header("X-Client-Name", client[0])
                  .header("User-Agent", "openhouse-java-client/" + client[1]))
          .andExpect(status().isUnprocessableEntity())
          .andExpect(jsonPath("$.tableLocation").doesNotExist());
    }
    mvc.perform(MockMvcRequestBuilders.get(path)).andExpect(status().isUnprocessableEntity());
    mvc.perform(
            MockMvcRequestBuilders.get(path)
                .header("X-OpenHouse-Dangerously-Bypass-Column-Default-Policy", "true"))
        .andExpect(status().isOk());
    mvc.perform(
            MockMvcRequestBuilders.get(path)
                .header("Authorization", "")
                .header("X-OpenHouse-Dangerously-Bypass-Column-Default-Policy", "true"))
        .andExpect(status().isUnauthorized());
    // Search is not gated: many systems search, and every client loads before reading or writing.
    mvc.perform(
            MockMvcRequestBuilders.post("/v2/databases/d1/tables/search")
                .param("fields", "tableLocation"))
        .andExpect(status().isOk());
  }

  @Test
  public void writesCannotBypassAdmissionByRemovingOptIn() throws Exception {
    created = create(uniqueTable("write_gate"), Collections.singletonMap(ENABLED_PROP, "true"));
    String path = "/v1/databases/" + created.getDatabaseId() + "/tables/" + created.getTableId();
    mvc.perform(
            MockMvcRequestBuilders.put(path)
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    buildCreateUpdateTableRequestBody(created)
                        .toBuilder()
                        .baseTableVersion("INITIAL_VERSION")
                        .build()
                        .toJson()))
        .andExpect(status().isUnprocessableEntity());
    MvcResult initial = createTable();
    CreateUpdateTableRequestBody update =
        buildCreateUpdateTableRequestBody(buildGetTableResponseBody(initial));
    Map<String, String> properties = new HashMap<>(update.getTableProperties());
    properties.remove(ENABLED_PROP);
    update = update.toBuilder().tableProperties(properties).build();
    mvc.perform(
            MockMvcRequestBuilders.put(path)
                .contentType(MediaType.APPLICATION_JSON)
                .content(update.toJson()))
        .andExpect(status().isUnprocessableEntity());
    mvc.perform(
            MockMvcRequestBuilders.put(path + "/iceberg/v2/snapshots")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    IcebergSnapshotsRequestBody.builder()
                        .baseTableVersion(update.getBaseTableVersion())
                        .createUpdateTableRequestBody(update)
                        .jsonSnapshots(Collections.emptyList())
                        .build()
                        .toJson()))
        .andExpect(status().isUnprocessableEntity());
    getTable()
        .andExpect(jsonPath("$.tableLocation", is(update.getBaseTableVersion())))
        .andExpect(jsonPath("$.tableProperties['" + ENABLED_PROP + "']", is("true")));
    properties.put(ENABLED_PROP, "true");
    properties.put("user.override", "accepted");
    mvc.perform(
            MockMvcRequestBuilders.put(path)
                .contentType(MediaType.APPLICATION_JSON)
                .content(update.toBuilder().tableProperties(properties).build().toJson())
                .header("X-OpenHouse-Dangerously-Bypass-Column-Default-Policy", "true"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.tableProperties['user.override']", is("accepted")));
  }

  @Test
  public void policyBypassAllowsDisablingCommittedOptIn() throws Exception {
    created = create(uniqueTable("disable"), Collections.singletonMap(ENABLED_PROP, "true"));
    MvcResult initial = createTable();
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
            MockMvcRequestBuilders.put(path)
                .header("X-Client-Name", "spark")
                .header("User-Agent", "openhouse-java-client/0.5.100")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
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

  private MvcResult createTable() throws Exception {
    return mvc.perform(
            MockMvcRequestBuilders.post("/v1/databases/" + created.getDatabaseId() + "/tables")
                .header("X-Client-Name", "spark")
                .header("User-Agent", "openhouse-java-client/0.5.100")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    buildCreateUpdateTableRequestBody(created)
                        .toBuilder()
                        .baseTableVersion("INITIAL_VERSION")
                        .build()
                        .toJson()))
        .andExpect(status().isCreated())
        .andReturn();
  }

  /**
   * On a table with column defaults, every snapshots PUT must come from a default-aware client, on
   * any branch and whatever its snapshots do, unless it carries the column-default policy bypass.
   * The client does not have to send {@code initial-default}.
   * https://github.com/linkedin/openhouse/issues/693
   */
  @Test
  public void snapshotsPut_requiresDefaultAwareClientOnEveryBranch() throws Exception {
    created = create(uniqueTable("branch_rewrite"), Collections.singletonMap(ENABLED_PROP, "true"));
    MvcResult current = createTable();
    TableIdentifier identifier = TableIdentifier.of(created.getDatabaseId(), created.getTableId());

    Table table = catalog.loadTable(identifier);
    Snapshot append = table.newAppend().appendFile(dataFile(table)).apply();
    mvc.perform(snapshotsPut(current, false, branch("main", append), append))
        .andExpect(status().isUnprocessableEntity());
    current =
        mvc.perform(
                snapshotsPut(current, false, branch("main", append), append)
                    .header("X-OpenHouse-Dangerously-Bypass-Column-Default-Policy", "true"))
            .andExpect(status().isOk())
            .andReturn();

    table = catalog.loadTable(identifier);
    Snapshot branchOverwrite =
        table.newOverwrite().addFile(dataFile(table)).toBranch("feature").apply();
    Map<String, String> branchWrite = branch("main", append);
    branchWrite.putAll(branch("feature", branchOverwrite));
    mvc.perform(snapshotsPut(current, false, branchWrite, append, branchOverwrite))
        .andExpect(status().isUnprocessableEntity());
    mvc.perform(snapshotsPut(current, true, branchWrite, append, branchOverwrite))
        .andExpect(status().isOk());
    assertEquals(
        branchOverwrite.snapshotId(),
        catalog.loadTable(identifier).refs().get("feature").snapshotId());
  }

  /**
   * A write built on the version before the table opted in gets the retryable version conflict, not
   * a column-default rejection. Rebuilt on the current version, it commits.
   */
  @Test
  public void snapshotsPut_onVersionBeforeOptIn_conflictsInsteadOfFailing() throws Exception {
    created = create(uniqueTable("stale_opt_in"), Collections.emptyMap());
    MvcResult beforeOptIn =
        RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);
    GetTableResponseBody optIn = buildGetTableResponseBody(beforeOptIn);
    optIn.getTableProperties().put(ENABLED_PROP, "true");
    MvcResult afterOptIn = putTable(optIn).andExpect(status().isOk()).andReturn();

    TableIdentifier identifier = TableIdentifier.of(created.getDatabaseId(), created.getTableId());
    Table table = catalog.loadTable(identifier);
    Snapshot append = table.newAppend().appendFile(dataFile(table)).apply();
    mvc.perform(snapshotsPut(beforeOptIn, true, branch("main", append), append))
        .andExpect(status().isConflict());
    mvc.perform(snapshotsPut(afterOptIn, true, branch("main", append), append))
        .andExpect(status().isOk());
  }

  /**
   * A rename commits to the table, so the rules check it like any other commit: it passes when the
   * stored default applies, and fails as a server error when it cannot unless the request carries
   * the column-default policy bypass.
   */
  @Test
  public void rename_isCheckedLikeEveryCommit() throws Exception {
    created = create(uniqueTable("rename"), Collections.singletonMap(ENABLED_PROP, "true"));
    createTable();

    String renamed = uniqueTable("renamed");
    mvc.perform(rename(renamed)).andExpect(status().isNoContent());
    created = renamed(renamed);
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']", is("\"US\"")));

    failing = true;
    String repaired = uniqueTable("renamed");
    mvc.perform(rename(repaired))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.message", containsString(UNUSABLE)));
    mvc.perform(
            rename(repaired).header("X-OpenHouse-Dangerously-Bypass-Column-Default-Policy", "true"))
        .andExpect(status().isNoContent());
    created = renamed(repaired);
  }

  private MockHttpServletRequestBuilder rename(String toTableId) {
    return MockMvcRequestBuilders.patch(
            String.format(
                ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/%s/rename",
                created.getDatabaseId(),
                created.getTableId()))
        .param("toDatabaseId", created.getDatabaseId())
        .param("toTableId", toTableId)
        .accept(MediaType.APPLICATION_JSON);
  }

  /** {@link #created}, renamed to {@code tableId}. */
  private GetTableResponseBody renamed(String tableId) {
    return created
        .toBuilder()
        .tableId(tableId)
        .tableUri(CLUSTER_NAME + "." + created.getDatabaseId() + "." + tableId)
        .build();
  }

  /** A snapshots PUT, from a default-aware Spark client or from a client that sends no identity. */
  private MockHttpServletRequestBuilder snapshotsPut(
      MvcResult current,
      boolean defaultAwareClient,
      Map<String, String> refs,
      Snapshot... snapshots)
      throws Exception {
    CreateUpdateTableRequestBody envelope = buildCreateUpdateTableRequestBody(current);
    IcebergSnapshotsRequestBody request =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(envelope.getBaseTableVersion())
            .jsonSnapshots(
                Arrays.stream(snapshots).map(SnapshotParser::toJson).collect(Collectors.toList()))
            .snapshotRefs(refs)
            .createUpdateTableRequestBody(envelope)
            .build();
    MockHttpServletRequestBuilder put =
        MockMvcRequestBuilders.put(
                String.format(
                    ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/%s/tables/%s/iceberg/v2/snapshots",
                    created.getDatabaseId(),
                    created.getTableId()))
            .contentType(MediaType.APPLICATION_JSON)
            .content(request.toJson())
            .accept(MediaType.APPLICATION_JSON);
    return defaultAwareClient
        ? put.header("X-Client-Name", "spark").header("User-Agent", "openhouse-java-client/0.5.100")
        : put;
  }

  private DataFile dataFile(Table table) throws IOException {
    return IcebergSnapshotsModelTestUtilities.createDummyDataFile(
        storageManager.getDefaultStorage().getClient().getRootPrefix()
            + "/"
            + UUID.randomUUID()
            + ".orc",
        table.spec());
  }

  private static Map<String, String> branch(String name, Snapshot snapshot) {
    Map<String, String> refs = new HashMap<>();
    refs.put(
        name, SnapshotRefParser.toJson(SnapshotRef.branchBuilder(snapshot.snapshotId()).build()));
    return refs;
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
            .header("X-Client-Name", "spark")
            .header("User-Agent", "openhouse-java-client/0.5.100")
            .accept(MediaType.APPLICATION_JSON));
  }

  private ResultActions putTable(GetTableResponseBody table) throws Exception {
    return mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/%s",
                    table.getDatabaseId(),
                    table.getTableId()))
            .header("X-Client-Name", "spark")
            .header("User-Agent", "openhouse-java-client/0.5.100")
            .contentType(MediaType.APPLICATION_JSON)
            .content(buildCreateUpdateTableRequestBody(table).toJson())
            .accept(MediaType.APPLICATION_JSON));
  }
}
