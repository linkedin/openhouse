package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.model.TableModelConstants.CLUSTER_NAME;
import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.node.TextNode;
import com.jayway.jsonpath.JsonPath;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultsSource;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeConfigResolver;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.repository.ToggleStatusesRepository;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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
 * HTTP create/get stamps {@code config} from a stub {@link ColumnDefaultsSource} according to the
 * OpenHouse ramp. Deployment encoders are out of scope; resolver unit tests cover the same matrix.
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

  private static final String CONFIG_KEY = ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX + "5";
  private static final String ENABLED_PROP =
      ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID
          + TableFeatureToggle.ENABLED_PROPERTY_SUFFIX;

  @TestConfiguration
  static class StubDefaults {
    @Bean
    ColumnDefaultsSource stubColumnDefaults() {
      return tableDto -> Collections.singletonMap(5, TextNode.valueOf("US"));
    }
  }

  @Autowired private MockMvc mvc;
  @Autowired private StorageManager storageManager;
  @Autowired private ToggleStatusesRepository toggleStatusesRepository;

  private GetTableResponseBody created;
  private TableToggleStatus toggleStatus;

  @AfterEach
  public void tearDown() throws Exception {
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
  public void createAndGet_stampsColumnDefaultConfigWhenEnabled() throws Exception {
    created = create(uniqueTable("prop_on"), Collections.singletonMap(ENABLED_PROP, "true"));

    MvcResult createdResult =
        RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);
    assertEquals(
        "\"US\"",
        JsonPath.read(
            createdResult.getResponse().getContentAsString(), "$.config['" + CONFIG_KEY + "']"));

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
  public void get_omitsConfigWhenNoPropertyAndNoHtsToggle() throws Exception {
    created = create(uniqueTable("no_ramp"), Collections.emptyMap());
    RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']").doesNotExist());
  }

  @Test
  public void get_stampsWhenHtsToggleActiveAndNoProperty() throws Exception {
    String tableId = uniqueTable("hts_on");
    created = create(tableId, Collections.emptyMap());
    activateHtsToggle(created);
    RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']", is("\"US\"")));
  }

  @Test
  public void get_propertyFalseOptsOutEvenWhenHtsToggleActive() throws Exception {
    created =
        create(uniqueTable("hts_on_prop_off"), Collections.singletonMap(ENABLED_PROP, "false"));
    activateHtsToggle(created);
    RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']").doesNotExist());
  }

  @Test
  public void get_unparseablePropertyFailsClosedEvenIfHtsActive() throws Exception {
    created = create(uniqueTable("bad_prop"), Collections.singletonMap(ENABLED_PROP, "sometimes"));
    activateHtsToggle(created);
    RequestAndValidateHelper.createTableAndValidateResponse(created, mvc, storageManager);
    getTable()
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.config['" + CONFIG_KEY + "']").doesNotExist());
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
            .accept(MediaType.APPLICATION_JSON));
  }
}
