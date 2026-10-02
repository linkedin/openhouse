package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.model.TableModelConstants.CLUSTER_NAME;
import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildCreateUpdateTableRequestBody;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildGetTableResponseBody;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.jayway.jsonpath.JsonPath;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.IcebergSnapshotsModelTestUtilities;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException.Reason;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultsSource;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeConfigResolver;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.repository.ToggleStatusesRepository;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
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
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

/**
 * HTTP GET stamps {@code config} from a stub {@link ColumnDefaultsSource} according to the
 * OpenHouse ramp, and fails when the source cannot apply a declared default. Create and update
 * responses omit {@code config}. Resolver unit tests cover the same matrix.
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
  private static final String ENABLED_PROP =
      ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID
          + TableFeatureToggle.ENABLED_PROPERTY_SUFFIX;

  /** Tables the stub source rejects; reset after each test. */
  private static volatile Predicate<TableDto> failWhen = table -> false;

  @TestConfiguration
  static class StubDefaults {
    @Bean
    ColumnDefaultsSource stubColumnDefaults() {
      return tableDto -> {
        if (failWhen.test(tableDto)) {
          throw new ColumnDefaultException(
              Reason.INVALID_VALUE, tableDto, 2, "name", "string", "string", null);
        }
        return Collections.singletonMap(2, TextNode.valueOf("US"));
      };
    }
  }

  @Autowired private MockMvc mvc;
  @Autowired private ToggleStatusesRepository toggleStatusesRepository;
  @Autowired private Catalog catalog;
  @Autowired private StorageManager storageManager;

  private GetTableResponseBody created;
  private TableToggleStatus toggleStatus;

  @AfterEach
  public void tearDown() throws Exception {
    failWhen = table -> false;
    if (created != null) {
      RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, created);
      created = null;
    }
    if (toggleStatus != null) {
      toggleStatusesRepository.delete(toggleStatus);
      toggleStatus = null;
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
  public void get_omitsConfigWhenNoPropertyAndNoHtsToggle() throws Exception {
    created = create(uniqueTable("no_ramp"), Collections.emptyMap());
    createTable();
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']").doesNotExist());
  }

  @Test
  public void get_stampsWhenHtsToggleActiveAndNoProperty() throws Exception {
    String tableId = uniqueTable("hts_on");
    created = create(tableId, Collections.emptyMap());
    activateHtsToggle(created);
    createTable();
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']", is("\"US\"")));
  }

  @Test
  public void get_propertyFalseOptsOutEvenWhenHtsToggleActive() throws Exception {
    created =
        create(uniqueTable("hts_on_prop_off"), Collections.singletonMap(ENABLED_PROP, "false"));
    activateHtsToggle(created);
    createTable();
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']").doesNotExist());
  }

  @Test
  public void get_unparseablePropertyFailsClosedEvenIfHtsActive() throws Exception {
    created = create(uniqueTable("bad_prop"), Collections.singletonMap(ENABLED_PROP, "sometimes"));
    activateHtsToggle(created);
    createTable();
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']").doesNotExist());
  }

  /** GET fails loudly instead of returning the table without its declared defaults. */
  @Test
  public void get_failsWithServerErrorWhenDefaultIsUnusable() throws Exception {
    created = create(uniqueTable("unusable"), Collections.singletonMap(ENABLED_PROP, "true"));
    createTable();
    failWhen = table -> table.getTableId().equals(created.getTableId());

    getTable()
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.message", containsString("COLUMN_DEFAULT_UNUSABLE")))
        .andExpect(jsonPath("$.message", containsString("INVALID_VALUE, column name, field ID 2")))
        .andExpect(jsonPath("$.config").doesNotExist());
  }

  /**
   * Nothing consults the source after the commit: create succeeds even though only the stored table
   * is rejected, and the following GET reports the failure.
   */
  @Test
  public void create_commitsWithoutLookupWhenSourceRejectsStoredTable() throws Exception {
    created =
        create(uniqueTable("stored_rejected"), Collections.singletonMap(ENABLED_PROP, "true"));
    failWhen = table -> table.getTableLocation() != null;

    MvcResult createdResult = createTable();
    jsonPath("$.config").doesNotExist().match(createdResult);
    getTable().andExpect(status().isInternalServerError());
  }

  /**
   * Iceberg 1.5 {@code sameSchema} includes {@code initial-default}, so this PUT cannot go through
   * {@code updateTableAndValidateResponse}. Create without overlay, PUT a matching handshake, then
   * assert persist dropped it.
   */
  @Test
  public void putWithMatchingOverlay_doesNotPersistInitialDefault() throws Exception {
    created = create(uniqueTable("persist_drop"), Collections.singletonMap(ENABLED_PROP, "true"));
    createTable();

    MvcResult get = getTable().andExpect(status().isOk()).andReturn();
    GetTableResponseBody current = buildGetTableResponseBody(get);
    GetTableResponseBody overlay =
        current.toBuilder().schema(withInitialDefault(current.getSchema())).build();

    mvc.perform(
            MockMvcRequestBuilders.put(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                            + "/databases/%s/tables/%s",
                        overlay.getDatabaseId(),
                        overlay.getTableId()))
                .header("X-Client-Name", "spark")
                .header("User-Agent", "openhouse-java-client/0.5.100")
                .contentType(MediaType.APPLICATION_JSON)
                .content(buildCreateUpdateTableRequestBody(overlay).toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    MvcResult after =
        getTable()
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']", is("\"US\"")))
            .andReturn();
    String schemaJson = JsonPath.read(after.getResponse().getContentAsString(), "$.schema");
    assertNull(SchemaParser.fromJson(schemaJson).findField(2).initialDefault());
  }

  @Test
  public void incompatibleClientsCannotReadOrExposeMetadataLocations() throws Exception {
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
    mvc.perform(
            MockMvcRequestBuilders.post("/v2/databases/d1/tables/search")
                .param("fields", "tableLocation"))
        .andExpect(status().isUnprocessableEntity());
    mvc.perform(MockMvcRequestBuilders.post("/v2/databases/d1/tables/search"))
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
    String path = "/v1/databases/" + created.getDatabaseId() + "/tables/" + created.getTableId();
    mvc.perform(
            MockMvcRequestBuilders.put(path)
                .header("X-Client-Name", "spark")
                .header("User-Agent", "openhouse-java-client/0.5.100")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath("$.message", containsString("dangerously-bypass-column-default-policy")));
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
        .andExpect(
            jsonPath("$.tableProperties['openhouse.columnDefaultPolicyBypass']").doesNotExist())
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
   * Type 2 gates the rewrite snapshots a snapshots PUT adds on any branch, not snapshots already on
   * the table. https://github.com/linkedin/openhouse/issues/693
   */
  @Test
  public void snapshotsPut_gatesAddedBranchRewritesButNotRefsAtPersistedRewrites()
      throws Exception {
    created = create(uniqueTable("branch_rewrite"), Collections.singletonMap(ENABLED_PROP, "true"));
    MvcResult current = createTable();
    TableIdentifier identifier = TableIdentifier.of(created.getDatabaseId(), created.getTableId());

    Table table = catalog.loadTable(identifier);
    Snapshot append = table.newAppend().appendFile(dataFile(table)).apply();
    current =
        putSnapshots(current, false, branch("main", append), append)
            .andExpect(status().isOk())
            .andReturn();

    table = catalog.loadTable(identifier);
    Snapshot branchOverwrite =
        table.newOverwrite().addFile(dataFile(table)).toBranch("feature").apply();
    Map<String, String> branchWrite = branch("main", append);
    branchWrite.putAll(branch("feature", branchOverwrite));
    putSnapshots(current, false, branchWrite, append, branchOverwrite)
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", containsString("COLUMN_DEFAULT_REWRITE")));

    Snapshot mainOverwrite = table.newOverwrite().addFile(dataFile(table)).apply();
    current =
        putSnapshots(current, true, branch("main", mainOverwrite), append, mainOverwrite)
            .andExpect(status().isOk())
            .andReturn();

    Map<String, String> refOnly = branch("main", mainOverwrite);
    refOnly.putAll(branch("feature", mainOverwrite));
    refOnly.put(
        "release",
        SnapshotRefParser.toJson(SnapshotRef.tagBuilder(mainOverwrite.snapshotId()).build()));
    putSnapshots(current, false, refOnly, append, mainOverwrite).andExpect(status().isOk());
    assertTrue(catalog.loadTable(identifier).refs().get("release").isTag());
  }

  private ResultActions putSnapshots(
      MvcResult current, boolean handshake, Map<String, String> refs, Snapshot... snapshots)
      throws Exception {
    CreateUpdateTableRequestBody envelope = buildCreateUpdateTableRequestBody(current);
    if (handshake) {
      envelope = envelope.toBuilder().schema(withInitialDefault(envelope.getSchema())).build();
    }
    IcebergSnapshotsRequestBody request =
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion(envelope.getBaseTableVersion())
            .jsonSnapshots(
                Arrays.stream(snapshots).map(SnapshotParser::toJson).collect(Collectors.toList()))
            .snapshotRefs(refs)
            .createUpdateTableRequestBody(envelope)
            .build();
    return mvc.perform(
        MockMvcRequestBuilders.put(
                String.format(
                    ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX
                        + "/databases/%s/tables/%s/iceberg/v2/snapshots",
                    created.getDatabaseId(),
                    created.getTableId()))
            .header("X-Client-Name", "spark")
            .header("User-Agent", "openhouse-java-client/0.5.100")
            .contentType(MediaType.APPLICATION_JSON)
            .content(request.toJson())
            .accept(MediaType.APPLICATION_JSON));
  }

  /** The default-aware client handshake: {@code initial-default} equal to the stamp. */
  private static String withInitialDefault(String schemaJson) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(schemaJson);
    for (JsonNode field : root.get("fields")) {
      if (field.get("id").asInt() == 2) {
        ((ObjectNode) field).put("initial-default", "US");
      }
    }
    return mapper.writeValueAsString(root);
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

  private void activateHtsToggle(GetTableResponseBody table) {
    toggleStatus =
        TableToggleStatus.builder()
            .featureId(ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID)
            .databaseId(table.getDatabaseId())
            .tableId(table.getTableId())
            .toggleStatusEnum(ToggleStatus.StatusEnum.ACTIVE)
            .build();
    toggleStatusesRepository.save(toggleStatus);
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
}
