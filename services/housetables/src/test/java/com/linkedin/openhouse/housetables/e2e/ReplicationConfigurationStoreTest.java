package com.linkedin.openhouse.housetables.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.housetables.api.spec.model.ReplicationConfiguration;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationConfigurationSet;
import com.linkedin.openhouse.housetables.repository.ReplicationConfigurationStore;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(classes = SpringH2HtsApplication.class)
@AutoConfigureMockMvc
public class ReplicationConfigurationStoreTest {

  @Autowired private ReplicationConfigurationStore replicationConfigurationStore;

  @Autowired private JdbcTemplate jdbcTemplate;

  @Autowired private MockMvc mockMvc;

  @BeforeEach
  public void cleanTables() {
    jdbcTemplate.update("DELETE FROM replication_configuration");
    jdbcTemplate.update("DELETE FROM replication_configuration_state");
  }

  @Test
  public void testReplaceAndFindMultipleDestinationsCaseInsensitively() {
    replicationConfigurationStore.replace(
        configurationSet(
            true,
            Arrays.asList(
                configuration("clusterB", "dbB", "tableB", "12H"),
                configuration("clusterA", "dbA", "tableA", "1D"))));

    ReplicationConfigurationSet result =
        replicationConfigurationStore.findBySource("source_db", "source_table").get();

    assertThat(result.getConfigured()).isTrue();
    assertThat(result.getSourceDatabaseId()).isEqualTo("SOURCE_DB");
    assertThat(result.getSourceTableId()).isEqualTo("SOURCE_TABLE");
    assertThat(result.getConfigurations())
        .extracting(ReplicationConfiguration::getDestinationClusterId)
        .containsExactly("CLUSTERA", "CLUSTERB");
    assertThat(result.getConfigurations())
        .extracting(ReplicationConfiguration::getReplicationInterval)
        .containsExactly("1D", "12H");
  }

  @Test
  public void testEmptyConfiguredStateAndExplicitClearAreDistinguishedFromMissing() {
    assertThat(replicationConfigurationStore.findBySource("source_db", "source_table")).isEmpty();

    replicationConfigurationStore.replace(configurationSet(true, Collections.emptyList()));
    ReplicationConfigurationSet empty =
        replicationConfigurationStore.findBySource("source_db", "source_table").get();
    assertThat(empty.getConfigured()).isTrue();
    assertThat(empty.getConfigurations()).isEmpty();

    replicationConfigurationStore.replace(configurationSet(false, Collections.emptyList()));
    ReplicationConfigurationSet cleared =
        replicationConfigurationStore.findBySource("source_db", "source_table").get();
    assertThat(cleared.getConfigured()).isFalse();
    assertThat(cleared.getConfigurations()).isEmpty();
  }

  @Test
  public void testReplaceRemovesOldEdgesAndRejectsDuplicateDestinations() {
    replicationConfigurationStore.replace(
        configurationSet(
            true, Collections.singletonList(configuration("clusterA", "db", "tbl", "1D"))));
    replicationConfigurationStore.replace(
        configurationSet(
            true, Collections.singletonList(configuration("clusterB", "db", "tbl", "2D"))));

    assertThat(
            replicationConfigurationStore
                .findBySource("SOURCE_DB", "SOURCE_TABLE")
                .get()
                .getConfigurations())
        .extracting(ReplicationConfiguration::getDestinationClusterId)
        .containsExactly("CLUSTERB");
    assertThatThrownBy(
            () ->
                replicationConfigurationStore.replace(
                    configurationSet(
                        true,
                        Arrays.asList(
                            configuration("clusterB", "db", "tbl", "1D"),
                            configuration("CLUSTERB", "DB", "TBL", "2D")))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate replication destination");
  }

  @Test
  public void testInternalEndpointRoundTripsCatalogConfiguration() throws Exception {
    mockMvc
        .perform(get("/hts/replication-configurations?sourceDatabaseId=db&sourceTableId=tbl"))
        .andExpect(status().isNotFound());

    mockMvc
        .perform(
            put("/hts/replication-configurations")
                .contentType("application/json")
                .content(
                    "{\"sourceDatabaseId\":\"db\",\"sourceTableId\":\"tbl\","
                        + "\"configured\":true,\"configurations\":[{"
                        + "\"destinationClusterId\":\"clusterA\","
                        + "\"destinationDatabaseId\":\"dbA\","
                        + "\"destinationTableId\":\"tblA\","
                        + "\"replicationInterval\":\"12H\"}]}"))
        .andExpect(status().isNoContent());

    mockMvc
        .perform(get("/hts/replication-configurations?sourceDatabaseId=DB&sourceTableId=TBL"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.configured").value(true))
        .andExpect(jsonPath("$.configurations[0].destinationClusterId").value("CLUSTERA"))
        .andExpect(jsonPath("$.configurations[0].replicationInterval").value("12H"))
        .andExpect(content().contentTypeCompatibleWith("application/json"));
  }

  private static ReplicationConfigurationSet configurationSet(
      boolean configured, java.util.List<ReplicationConfiguration> configurations) {
    return ReplicationConfigurationSet.builder()
        .sourceDatabaseId("source_db")
        .sourceTableId("source_table")
        .configured(configured)
        .configurations(configurations)
        .build();
  }

  private static ReplicationConfiguration configuration(
      String clusterId, String databaseId, String tableId, String interval) {
    return ReplicationConfiguration.builder()
        .destinationClusterId(clusterId)
        .destinationDatabaseId(databaseId)
        .destinationTableId(tableId)
        .replicationInterval(interval)
        .build();
  }
}
