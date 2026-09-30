package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;
import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildCreateUpdateTableRequestBody;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildGetTableResponseBody;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.repository.PreservedKeyChecker;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

/**
 * history.expire.max-ref-age-ms where Iceberg table properties are reserved, as with li-openhouse's
 * {@code LiPreservedKeyChecker}: clients cannot write the property, so tables-service alone has to
 * put it on new tables and on tables created before it owned the property. Tables live in this
 * class's own database under random names, so it can run alongside other tests or itself.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration(
    initializers = {
      PropertyOverrideContextInitializer.class,
      AuthorizationPropertiesInitializer.class
    })
@Import(ReservedIcebergPropertiesMaxRefAgeTest.ReservedIcebergProperties.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ReservedIcebergPropertiesMaxRefAgeTest {
  private static final String DATABASE = "d_reserved_iceberg_properties";
  private static final String MAX_REF_AGE = TableProperties.MAX_REF_AGE_MS;
  private static final String SEVEN_DAYS = String.valueOf(TimeUnit.DAYS.toMillis(7));
  private static final String TABLES_URL =
      ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/";
  private static final String TABLE_URL = TABLES_URL + "%s";

  @Autowired MockMvc mvc;

  @Autowired Catalog catalog;

  @Autowired HouseTableRepository houseTables;

  private GetTableResponseBody table;

  @AfterEach
  void dropTable() throws Exception {
    if (table != null) {
      RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, table);
    }
  }

  @Test
  void createIgnoresRequestedValueAndSetsSevenDays() throws Exception {
    Map<String, String> requested = new HashMap<>(GET_TABLE_RESPONSE_BODY.getTableProperties());
    requested.put(MAX_REF_AGE, String.valueOf(TimeUnit.DAYS.toMillis(14)));

    table = create("create", requested);

    Assertions.assertEquals(SEVEN_DAYS, table.getTableProperties().get(MAX_REF_AGE));
  }

  @Test
  void tableWithoutMaxRefAgeGetsItFromItsNextCommit() throws Exception {
    create("backfill", GET_TABLE_RESPONSE_BODY.getTableProperties());
    table = removeMaxRefAgeFromCommittedMetadata(table);
    Assertions.assertNull(table.getTableProperties().get(MAX_REF_AGE));

    // #708's snapshot-expiration backfill: a client adding the reserved property is rejected.
    RequestAndValidateHelper.updateTableWithReservedPropsAndValidateResponse(
        mvc, withProperty(table, MAX_REF_AGE, SEVEN_DAYS), MAX_REF_AGE);

    table = update(withProperty(table, "user.commit", "1"));
    Assertions.assertEquals(SEVEN_DAYS, table.getTableProperties().get(MAX_REF_AGE));

    // Clients send back the value tables-service added, which the reserved-key check accepts.
    table = update(withProperty(table, "user.commit", "2"));
    Assertions.assertEquals(SEVEN_DAYS, table.getTableProperties().get(MAX_REF_AGE));
  }

  /** Creates a table with a random name and records it for {@link #dropTable()}. */
  private GetTableResponseBody create(String name, Map<String, String> properties)
      throws Exception {
    GetTableResponseBody request =
        GET_TABLE_RESPONSE_BODY
            .toBuilder()
            .databaseId(DATABASE)
            .tableId(name + "_" + UUID.randomUUID().toString().replace("-", ""))
            .tableProperties(properties)
            .build();
    MvcResult result =
        mvc.perform(
                MockMvcRequestBuilders.post(String.format(TABLES_URL, request.getDatabaseId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        buildCreateUpdateTableRequestBody(request)
                            .toBuilder()
                            .baseTableVersion(INITIAL_TABLE_VERSION)
                            .build()
                            .toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isCreated())
            .andReturn();
    table = buildGetTableResponseBody(result);
    return table;
  }

  private GetTableResponseBody update(GetTableResponseBody request) throws Exception {
    MvcResult result =
        mvc.perform(
                MockMvcRequestBuilders.put(
                        String.format(TABLE_URL, request.getDatabaseId(), request.getTableId()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(buildCreateUpdateTableRequestBody(request).toJson())
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn();
    return buildGetTableResponseBody(result);
  }

  private static GetTableResponseBody withProperty(
      GetTableResponseBody current, String key, String value) {
    Map<String, String> properties = new HashMap<>(current.getTableProperties());
    properties.put(key, value);
    return current.toBuilder().tableProperties(properties).build();
  }

  /**
   * Rewrites the committed metadata without history.expire.max-ref-age-ms, as for a table created
   * before tables-service owned the property, and returns the table as clients now read it.
   */
  private GetTableResponseBody removeMaxRefAgeFromCommittedMetadata(GetTableResponseBody current)
      throws Exception {
    TableOperations ops =
        ((HasTableOperations)
                catalog.loadTable(
                    TableIdentifier.of(current.getDatabaseId(), current.getTableId())))
            .operations();
    TableMetadata committed = ops.current();
    String location =
        committed.metadataFileLocation().replace(".metadata.json", "-no-max-ref-age.metadata.json");
    Map<String, String> properties = new HashMap<>(committed.properties());
    properties.remove(MAX_REF_AGE);
    properties.put(getCanonicalFieldName("tableLocation"), location);
    TableMetadataParser.write(
        committed.replaceProperties(properties), ops.io().newOutputFile(location));
    HouseTablePrimaryKey key =
        HouseTablePrimaryKey.builder()
            .databaseId(current.getDatabaseId())
            .tableId(current.getTableId())
            .build();
    HouseTable row = houseTables.findById(key).orElseThrow(IllegalStateException::new);
    houseTables.save(row.toBuilder().tableLocation(location).build());

    MvcResult result =
        mvc.perform(
                MockMvcRequestBuilders.get(
                        String.format(TABLE_URL, current.getDatabaseId(), current.getTableId()))
                    .accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn();
    return buildGetTableResponseBody(result);
  }

  @TestConfiguration
  static class ReservedIcebergProperties {
    @Bean
    @Primary
    PreservedKeyChecker icebergPropertiesPreservedKeyChecker() {
      return new IcebergPropertiesPreservedKeyChecker();
    }
  }
}
