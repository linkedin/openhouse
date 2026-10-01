package com.linkedin.openhouse.tables.e2e.h2;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.tables.services.DatabasesService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(classes = SpringH2Application.class)
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class ViewsDatabaseEnumerationTest {

  private static final String VIEWS_ONLY_DATABASE = "views_only_database";

  @Autowired private HouseTableRepository houseTableRepository;
  @Autowired private DatabasesService databasesService;

  @Test
  public void neutralDatabaseEnumerationIncludesViewsOnlyDatabase() {
    houseTableRepository.saveView(
        HouseTable.builder()
            .databaseId(VIEWS_ONLY_DATABASE)
            .tableId("only_view")
            .tableUUID("view-uuid")
            .tableLocation("file:/warehouse/views_only_database/only_view/metadata.json")
            .storageType("local")
            .entityType("VIEW")
            .build());

    assertTrue(
        databasesService.getAllDatabases().stream()
            .anyMatch(database -> VIEWS_ONLY_DATABASE.equals(database.getDatabaseId())));
  }
}
