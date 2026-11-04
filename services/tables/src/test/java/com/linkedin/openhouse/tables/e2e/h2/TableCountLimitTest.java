package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY;
import static com.linkedin.openhouse.tables.model.TableModelConstants.STAGED_TABLE_DTO;
import static com.linkedin.openhouse.tables.model.TableModelConstants.TABLE_DTO;
import static com.linkedin.openhouse.tables.model.TableModelConstants.buildCreateUpdateTableRequestBody;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.common.exception.UnprocessableEntityException;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.client.model.ToggleStatus;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import com.linkedin.openhouse.tables.toggle.repository.ToggleStatusesRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

/**
 * Behavior of the per-database live table quota. It is enforced by default; only an operator
 * written ACTIVE {@code bypass_table_count_limit} toggle for the new table exempts it. Every test
 * uses its own database ids so nothing leaks between tests or classes.
 */
@SpringBootTest(properties = "cluster.tables.max-tables-per-database=2")
@AutoConfigureMockMvc
@ContextConfiguration(
    initializers = {
      PropertyOverrideContextInitializer.class,
      AuthorizationPropertiesInitializer.class
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class TableCountLimitTest {
  private static final String FEATURE_ID = "bypass_table_count_limit";
  private static final int LIMIT = 2;

  @Autowired private OpenHouseInternalRepository openHouseInternalRepository;
  @Autowired private HouseTableRepository houseTableRepository;
  @Autowired private ToggleStatusesRepository toggleStatusesRepository;
  @Autowired private MockMvc mvc;

  private final List<TableDtoPrimaryKey> tablesToClean = new ArrayList<>();
  private final List<TableToggleStatus> togglesToClean = new ArrayList<>();
  private final List<HouseTablePrimaryKey> viewsToClean = new ArrayList<>();
  private final List<String> databasesToClean = new ArrayList<>();

  @AfterEach
  void cleanUp() {
    for (TableDtoPrimaryKey key : tablesToClean) {
      if (openHouseInternalRepository.existsById(key)) {
        openHouseInternalRepository.deleteById(key);
      }
    }
    for (HouseTablePrimaryKey key : viewsToClean) {
      houseTableRepository.deleteViewById(key);
    }
    for (TableToggleStatus toggle : togglesToClean) {
      toggleStatusesRepository.delete(toggle);
    }
    HouseTablesH2Repository.softDeletedTables
        .keySet()
        .removeIf(key -> databasesToClean.contains(key.getDatabaseId()));
    tablesToClean.clear();
    togglesToClean.clear();
    viewsToClean.clear();
    databasesToClean.clear();
  }

  @Test
  void rejectsCreateAtLimitWithoutPersisting() {
    String db = newDatabase("at_limit");
    createTables(db, LIMIT);

    UnprocessableEntityException e =
        assertThrows(
            UnprocessableEntityException.class,
            () -> openHouseInternalRepository.save(dto(db, "t_over", false)));

    assertTrue(e.getMessage().contains(db), e.getMessage());
    assertFalse(openHouseInternalRepository.existsById(key(db, "t_over")));
    assertEquals(LIMIT, liveTableCount(db));
  }

  @Test
  void restoreIsRefusedAtLimitAndSucceedsAfterDrop() {
    String db = newDatabase("restore");
    createTables(db, LIMIT);
    String restored = "t_seed_0";
    houseTableRepository.deleteById(
        HouseTablePrimaryKey.builder().databaseId(db).tableId(restored).build(), true);
    SoftDeletedTablePrimaryKey softKey =
        HouseTablesH2Repository.softDeletedTables.keySet().stream()
            .filter(k -> k.getDatabaseId().equals(db) && k.getTableId().equals(restored))
            .findFirst()
            .get();
    openHouseInternalRepository.save(dto(db, "t_replacement", false));
    assertEquals(LIMIT, liveTableCount(db));

    assertThrows(
        UnprocessableEntityException.class,
        () -> openHouseInternalRepository.restoreTable(softKey));
    assertFalse(openHouseInternalRepository.existsById(key(db, restored)));
    assertTrue(HouseTablesH2Repository.softDeletedTables.containsKey(softKey));

    openHouseInternalRepository.deleteById(key(db, "t_replacement"));
    openHouseInternalRepository.restoreTable(softKey);
    assertTrue(openHouseInternalRepository.existsById(key(db, restored)));
    assertEquals(LIMIT, liveTableCount(db));
  }

  @Test
  void rejectsStagedCreateAtLimit() {
    String db = newDatabase("staged");
    createTables(db, LIMIT);
    TableDto staged = STAGED_TABLE_DTO.toBuilder().databaseId(db).tableId("t_staged").build();

    assertThrows(
        UnprocessableEntityException.class, () -> openHouseInternalRepository.save(staged));
    assertEquals(LIMIT, liveTableCount(db));
  }

  @Test
  void allowsCreateOverLimitWithActiveBypassToggle() {
    String db = newDatabase("bypass");
    createTables(db, LIMIT);

    assertDoesNotThrow(() -> openHouseInternalRepository.save(dto(db, "t_exempt", true)));
    assertEquals(LIMIT + 1, liveTableCount(db));
  }

  @Test
  void requestPropertyCannotSelfExempt() {
    String db = newDatabase("prop_bypass");
    createTables(db, LIMIT);
    TableDto bypass =
        dto(db, "t_bypass", false)
            .toBuilder()
            .tableProperties(Collections.singletonMap(FEATURE_ID + ".enabled", "true"))
            .build();

    assertThrows(
        UnprocessableEntityException.class, () -> openHouseInternalRepository.save(bypass));
    assertFalse(openHouseInternalRepository.existsById(key(db, "t_bypass")));
  }

  @Test
  void countsAreIsolatedPerDatabase() {
    String full = newDatabase("full");
    String other = newDatabase("other");
    createTables(full, LIMIT);

    assertDoesNotThrow(() -> openHouseInternalRepository.save(dto(other, "t_first", false)));
    assertThrows(
        UnprocessableEntityException.class,
        () -> openHouseInternalRepository.save(dto(full, "t_over", false)));
    assertEquals(1, liveTableCount(other));
  }

  @Test
  void viewsAndSoftDeletedTablesDoNotCount() {
    String db = newDatabase("not_counted");
    createTables(db, 1);
    HouseTablePrimaryKey viewKey =
        HouseTablePrimaryKey.builder().databaseId(db).tableId("a_view").build();
    viewsToClean.add(viewKey);
    houseTableRepository.saveView(
        HouseTable.builder()
            .databaseId(db)
            .tableId("a_view")
            .tableUUID("view-uuid-" + db)
            .tableLocation("file:/warehouse/" + db + "/a_view/metadata.json")
            .storageType("local")
            .entityType("VIEW")
            .build());
    TableDto doomed = dto(db, "t_soft_deleted", false);
    openHouseInternalRepository.save(doomed);
    houseTableRepository.deleteById(
        HouseTablePrimaryKey.builder().databaseId(db).tableId("t_soft_deleted").build(), true);
    assertFalse(openHouseInternalRepository.existsById(key(db, "t_soft_deleted")));

    // One live table, one view and one soft-deleted table: only the live table counts.
    assertDoesNotThrow(() -> openHouseInternalRepository.save(dto(db, "t_second", false)));
    assertThrows(
        UnprocessableEntityException.class,
        () -> openHouseInternalRepository.save(dto(db, "t_third", false)));
    assertEquals(LIMIT, liveTableCount(db));
  }

  @Test
  void dropFreesCapacityAndUpdateStillWorksAtLimit() {
    String db = newDatabase("drop_update");
    List<TableDto> created = createTables(db, LIMIT);
    TableDto first = created.get(0);

    Map<String, String> props = new HashMap<>(first.getTableProperties());
    props.put("user.updated", "yes");
    TableDto updated =
        openHouseInternalRepository.save(
            first
                .toBuilder()
                .tableVersion(first.getTableLocation())
                .tableProperties(props)
                .build());
    assertEquals("yes", updated.getTableProperties().get("user.updated"));
    assertEquals(LIMIT, liveTableCount(db));

    openHouseInternalRepository.deleteById(key(db, first.getTableId()));
    assertDoesNotThrow(() -> openHouseInternalRepository.save(dto(db, "t_after_drop", false)));
    assertThrows(
        UnprocessableEntityException.class,
        () -> openHouseInternalRepository.save(dto(db, "t_over", false)));
    assertEquals(LIMIT, liveTableCount(db));
  }

  @Test
  void apiReturns422WhenLimitReached() throws Exception {
    String db = newDatabase("api");
    createTables(db, LIMIT);
    GetTableResponseBody body =
        GET_TABLE_RESPONSE_BODY.toBuilder().databaseId(db).tableId("t_api").build();

    mvc.perform(
            MockMvcRequestBuilders.post(
                    String.format(
                        ValidationUtilities.CURRENT_MAJOR_VERSION_PREFIX + "/databases/%s/tables/",
                        db))
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    buildCreateUpdateTableRequestBody(body, false)
                        .toBuilder()
                        .baseTableVersion(INITIAL_TABLE_VERSION)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isUnprocessableEntity());

    assertFalse(openHouseInternalRepository.existsById(key(db, "t_api")));
    assertEquals(LIMIT, liveTableCount(db));
  }

  private String newDatabase(String suffix) {
    String db = "tcl_" + suffix;
    databasesToClean.add(db);
    return db;
  }

  private static TableDtoPrimaryKey key(String db, String table) {
    return TableDtoPrimaryKey.builder().databaseId(db).tableId(table).build();
  }

  /** A create-ready DTO; {@code bypass} activates the operator toggle exempting this table. */
  private TableDto dto(String db, String table, boolean bypass) {
    tablesToClean.add(key(db, table));
    if (bypass) {
      activate(db, table);
    }
    return TABLE_DTO
        .toBuilder()
        .databaseId(db)
        .tableId(table)
        .tableVersion(INITIAL_TABLE_VERSION)
        .build();
  }

  private TableToggleStatus activate(String db, String table) {
    TableToggleStatus toggle =
        TableToggleStatus.builder()
            .featureId(FEATURE_ID)
            .databaseId(db)
            .tableId(table)
            .toggleStatusEnum(ToggleStatus.StatusEnum.ACTIVE)
            .build();
    toggleStatusesRepository.save(toggle);
    togglesToClean.add(toggle);
    return toggle;
  }

  /** Creates {@code count} tables below the limit, so setup itself never needs a bypass. */
  private List<TableDto> createTables(String db, int count) {
    List<TableDto> created = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      created.add(openHouseInternalRepository.save(dto(db, "t_seed_" + i, false)));
    }
    assertEquals(count, liveTableCount(db));
    return created;
  }

  private long liveTableCount(String db) {
    return houseTableRepository.findAllByDatabaseId(db, PageRequest.of(0, 100)).getTotalElements();
  }
}
