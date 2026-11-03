package com.linkedin.openhouse.tablestest;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

/**
 * Point, list, and typed accessors over the H2-backed House Table stand-in, where a view, a table,
 * and a legacy row share one key space so an untyped accessor would be visible.
 */
public class HouseTablesH2ViewAccessorTest {

  private static final String DB = "viewdb";

  private ConfigurableApplicationContext context;
  private HouseTablesH2Repository repository;

  @BeforeEach
  public void setUp() {
    try {
      org.apache.catalina.webresources.TomcatURLStreamHandlerFactory.register();
    } catch (Error e) {
      org.apache.catalina.webresources.TomcatURLStreamHandlerFactory.disable();
    }
    SpringApplication application = new SpringApplication(SpringH2TestApplication.class);
    application.setDefaultProperties(Collections.singletonMap("server.port", "0"));
    context = application.run();
    repository = context.getBean(HouseTablesH2Repository.class);
    repository.deleteAll();

    repository.save(row("view_a", "VIEW"));
    repository.save(row("view_b", "VIEW"));
    repository.save(row("table_a", "TABLE"));
    repository.save(row("legacy_a", null));
  }

  @AfterEach
  public void tearDown() {
    if (context != null) {
      context.close();
    }
  }

  private static HouseTable row(String id, String entityType) {
    return HouseTable.builder()
        .databaseId(DB)
        .tableId(id)
        .tableLocation("/loc/" + id + "/00001-a.metadata.json")
        .tableVersion("INITIAL_VERSION")
        .storageType("local")
        .entityType(entityType)
        .build();
  }

  private static HouseTablePrimaryKey key(String id) {
    return HouseTablePrimaryKey.builder().databaseId(DB).tableId(id).build();
  }

  @Test
  public void neutralLookupResolvesEitherKindOfEntity() {
    Assertions.assertEquals("VIEW", repository.findEntityById(key("view_a")).get().getEntityType());
    Assertions.assertEquals(
        "TABLE", repository.findEntityById(key("table_a")).get().getEntityType());
    Assertions.assertFalse(repository.findEntityById(key("absent")).isPresent());
  }

  /** A stored null reads back as a table, as the column converter does. */
  @Test
  public void neutralLookupResolvesALegacyRowToTable() {
    Assertions.assertEquals(
        "TABLE",
        repository.findEntityById(key("legacy_a")).get().getEntityType(),
        "a legacy row means a table, so no caller has to interpret a null");
  }

  @Test
  public void typedLookupResolvesViewsOnly() {
    Assertions.assertTrue(repository.findViewById(key("view_a")).isPresent());
    Assertions.assertFalse(repository.findViewById(key("table_a")).isPresent());
    Assertions.assertFalse(repository.findViewById(key("legacy_a")).isPresent());
    Assertions.assertFalse(repository.findViewById(key("absent")).isPresent());
  }

  @Test
  public void tablePointReadAdmitsTablesAndLegacyRowsButNeverAView() {
    Assertions.assertEquals("TABLE", repository.findById(key("table_a")).get().getEntityType());
    Assertions.assertEquals(
        "TABLE",
        repository.findById(key("legacy_a")).get().getEntityType(),
        "a legacy row is a table on the table path too");
    Assertions.assertFalse(
        repository.findById(key("view_a")).isPresent(),
        "a view at a shared key must be absent from every table read");
  }

  @Test
  public void tableListsExcludeViewsAndFilterBeforePaginating() {
    List<HouseTable> unpaged = repository.findAllByDatabaseId(DB);
    Assertions.assertEquals(
        Arrays.asList("legacy_a", "table_a"),
        unpaged.stream().map(HouseTable::getTableId).sorted().collect(Collectors.toList()));
    Assertions.assertTrue(
        unpaged.stream().allMatch(row -> "TABLE".equals(row.getEntityType())),
        "every row the unpaged list returns must report its resolved type, legacy included: "
            + unpaged.stream()
                .map(row -> row.getTableId() + "=" + row.getEntityType())
                .collect(Collectors.toList()));

    Page<HouseTable> firstPage =
        repository.findAllByDatabaseId(DB, PageRequest.of(0, 1, Sort.by("tableId").ascending()));
    Page<HouseTable> secondPage =
        repository.findAllByDatabaseId(DB, PageRequest.of(1, 1, Sort.by("tableId").ascending()));
    Assertions.assertEquals(
        2L,
        firstPage.getTotalElements(),
        "the total counts table rows only, not the whole key space");
    Assertions.assertEquals(2, firstPage.getTotalPages());
    Assertions.assertEquals(
        "legacy_a", firstPage.getContent().get(0).getTableId(), "page one, ascending");
    Assertions.assertEquals(
        "table_a", secondPage.getContent().get(0).getTableId(), "page two, ascending");
    Assertions.assertEquals(
        "TABLE",
        firstPage.getContent().get(0).getEntityType(),
        "the legacy row is hydrated on the paginated overload too");
    Assertions.assertEquals("TABLE", secondPage.getContent().get(0).getEntityType());

    Page<HouseTable> wholePage = repository.findAllByDatabaseId(DB, PageRequest.of(0, 10));
    Assertions.assertEquals(2, wholePage.getContent().size());
    Assertions.assertTrue(
        wholePage.getContent().stream().allMatch(row -> "TABLE".equals(row.getEntityType())),
        "no view may appear on a table page, and every row reports its resolved type");
  }

  /** The sort was applied in SQL; dropping it makes page two an arbitrary set of rows. */
  @Test
  public void tableAndViewListsHonourTheRequestedSortAcrossPages() {
    repository.save(row("table_b", "TABLE"));
    repository.save(row("view_c", "VIEW"));

    Page<HouseTable> descendingFirst =
        repository.findAllByDatabaseId(DB, PageRequest.of(0, 2, Sort.by("tableId").descending()));
    Page<HouseTable> descendingSecond =
        repository.findAllByDatabaseId(DB, PageRequest.of(1, 2, Sort.by("tableId").descending()));
    Assertions.assertEquals(3L, descendingFirst.getTotalElements());
    Assertions.assertEquals(
        Arrays.asList("table_b", "table_a"),
        descendingFirst.getContent().stream()
            .map(HouseTable::getTableId)
            .collect(Collectors.toList()));
    Assertions.assertEquals(
        Collections.singletonList("legacy_a"),
        descendingSecond.getContent().stream()
            .map(HouseTable::getTableId)
            .collect(Collectors.toList()));
    Assertions.assertEquals(
        Sort.by("tableId").descending(),
        descendingFirst.getSort(),
        "the returned page must report the sort it was asked for");

    Page<HouseTable> viewsDescending =
        repository.findAllViewsByDatabaseId(
            DB, PageRequest.of(0, 2, Sort.by("tableId").descending()));
    Assertions.assertEquals(3L, viewsDescending.getTotalElements());
    Assertions.assertEquals(
        Arrays.asList("view_c", "view_b"),
        viewsDescending.getContent().stream()
            .map(HouseTable::getTableId)
            .collect(Collectors.toList()));
  }

  /** Case-insensitive ordering lives in the SQL, so a hand-written comparator would drop it. */
  @Test
  public void tableListsHonourCaseInsensitiveOrdering() {
    repository.deleteAll();
    repository.save(row("B_upper", "TABLE"));
    repository.save(row("a_lower", "TABLE"));

    Page<HouseTable> ignoringCase =
        repository.findAllByDatabaseId(
            DB, PageRequest.of(0, 10, Sort.by(Sort.Order.asc("tableId").ignoreCase())));

    Assertions.assertEquals(
        Arrays.asList("a_lower", "B_upper"),
        ignoringCase.getContent().stream().map(HouseTable::getTableId).collect(Collectors.toList()),
        "an ignore-case ascending sort must order the rows as the database would");
  }

  @Test
  public void typedListReturnsOnlyViewsWithCorrectTotals() {
    Page<HouseTable> page = repository.findAllViewsByDatabaseId(DB, PageRequest.of(0, 10));

    List<String> ids =
        page.getContent().stream()
            .map(HouseTable::getTableId)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(Arrays.asList("view_a", "view_b"), ids);
    Assertions.assertEquals(2L, page.getTotalElements());
  }

  @Test
  public void typedListPaginatesOverViewsOnly() {
    Page<HouseTable> firstPage = repository.findAllViewsByDatabaseId(DB, PageRequest.of(0, 1));
    Page<HouseTable> secondPage = repository.findAllViewsByDatabaseId(DB, PageRequest.of(1, 1));

    Assertions.assertEquals(1, firstPage.getContent().size());
    Assertions.assertEquals(1, secondPage.getContent().size());
    Assertions.assertEquals(2L, firstPage.getTotalElements());
    Assertions.assertEquals(2, firstPage.getTotalPages());
    Assertions.assertNotEquals(
        firstPage.getContent().get(0).getTableId(), secondPage.getContent().get(0).getTableId());
  }

  @Test
  public void savingAViewStampsTheEntityTypeAsTheRouteWould() {
    HouseTable saved =
        repository.saveView(
            HouseTable.builder()
                .databaseId(DB)
                .tableId("view_c")
                .tableLocation("/loc/view_c/00001-a.metadata.json")
                .tableVersion("INITIAL_VERSION")
                .storageType("local")
                .build());

    Assertions.assertEquals("VIEW", saved.getEntityType());
    Assertions.assertEquals("VIEW", repository.findViewById(key("view_c")).get().getEntityType());
    Assertions.assertFalse(
        repository.findById(key("view_c")).isPresent(),
        "a freshly stamped view is still invisible to the table path");
  }

  @Test
  public void typedDeleteRemovesTheViewAndLeavesEveryOtherRowIntact() {
    // Snapshots, so the comparison is against detached values.
    HouseTable tableBefore = repository.findEntityById(key("table_a")).get().toBuilder().build();
    HouseTable legacyBefore = repository.findEntityById(key("legacy_a")).get().toBuilder().build();
    HouseTable otherViewBefore = repository.findEntityById(key("view_b")).get().toBuilder().build();

    Assertions.assertTrue(repository.deleteViewById(key("view_a")));

    Assertions.assertFalse(repository.findEntityById(key("view_a")).isPresent());
    Assertions.assertEquals(
        tableBefore,
        repository.findEntityById(key("table_a")).get(),
        "a view delete must leave a table row byte-for-byte unchanged");
    Assertions.assertEquals(
        legacyBefore,
        repository.findEntityById(key("legacy_a")).get(),
        "a view delete must leave a legacy row byte-for-byte unchanged");
    Assertions.assertEquals(
        otherViewBefore,
        repository.findEntityById(key("view_b")).get(),
        "a view delete must leave sibling views byte-for-byte unchanged");
  }

  @Test
  public void typedDeleteDeclinesTableAndLegacyRows() {
    HouseTable tableBefore = repository.findEntityById(key("table_a")).get().toBuilder().build();
    HouseTable legacyBefore = repository.findEntityById(key("legacy_a")).get().toBuilder().build();

    Assertions.assertFalse(repository.deleteViewById(key("table_a")));
    Assertions.assertFalse(repository.deleteViewById(key("legacy_a")));
    Assertions.assertFalse(repository.deleteViewById(key("absent")));

    Assertions.assertEquals(tableBefore, repository.findEntityById(key("table_a")).get());
    Assertions.assertEquals(legacyBefore, repository.findEntityById(key("legacy_a")).get());
  }
}
