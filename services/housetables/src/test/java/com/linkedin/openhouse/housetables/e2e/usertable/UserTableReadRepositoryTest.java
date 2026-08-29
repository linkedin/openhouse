package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.exception.CorruptEntityTypeConversionException;
import com.linkedin.openhouse.housetables.exception.CorruptUserTableDataException;
import com.linkedin.openhouse.housetables.model.EntityType;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.repository.UserTableReadRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.model.UserViewQuery;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;

/**
 * The adapter against a real database: what it returns for healthy rows, and what it raises for a
 * genuinely corrupt one. The translation itself is unit-tested separately; this pins that the
 * corruption a real converter produces is what arrives at the service boundary, already carrying
 * the column diagnostic.
 */
@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
public class UserTableReadRepositoryTest {

  private static final String ADAPTER_DB = "adapter_read_db";

  @Autowired UserTableReadRepository readRepository;

  @Autowired UserTableHtsJdbcRepository htsRepository;

  @Autowired DataSource dataSource;

  @AfterEach
  public void tearDown() {
    // The JPA cleanup loads every row, so a planted non-canonical spelling must go first.
    new JdbcTemplate(dataSource)
        .update("DELETE FROM user_table_row WHERE entity_type NOT IN ('TABLE', 'VIEW')");
    htsRepository.deleteAll();
  }

  private UserTableRow row(String tableId, EntityType entityType) {
    return UserTableRow.builder()
        .databaseId(ADAPTER_DB)
        .tableId(tableId)
        .version(null)
        .metadataLocation(String.format("/openhouse/%s/%s/v0_metadata.json", ADAPTER_DB, tableId))
        .storageType(TEST_DEFAULT_STORAGE_TYPE)
        .creationTime(TEST_CREATION_TIME)
        .entityType(entityType)
        .build();
  }

  private void seedLegacyRow(String tableId) {
    htsRepository.save(row(tableId, null));
  }

  private void seedTypedRow(String tableId, EntityType entityType) {
    htsRepository.save(row(tableId, entityType));
  }

  private void insertRawEntityType(String tableId, String entityType) {
    new JdbcTemplate(dataSource)
        .update(
            "INSERT INTO user_table_row "
                + "(database_id, table_id, version, metadata_location, storage_type, creation_time, entity_type) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ADAPTER_DB,
            tableId,
            0L,
            String.format("/openhouse/%s/%s/v0_metadata.json", ADAPTER_DB, tableId),
            TEST_DEFAULT_STORAGE_TYPE,
            TEST_CREATION_TIME,
            entityType);
  }

  private Optional<String> readRawEntityType(String tableId) {
    return Optional.ofNullable(
        new JdbcTemplate(dataSource)
            .queryForObject(
                "SELECT entity_type FROM user_table_row WHERE database_id = ? AND table_id = ?",
                String.class,
                ADAPTER_DB,
                tableId));
  }

  /** The occupancy read sees either type, and reports a legacy null as the table that it means. */
  @Test
  public void testNeutralPointReadReportsCanonicalTypeForEitherType() {
    seedTypedRow("neutral_view", EntityType.VIEW);
    seedTypedRow("neutral_table", EntityType.TABLE);
    seedLegacyRow("neutral_legacy");

    assertThat(readRepository.findEntity(ADAPTER_DB, "neutral_view"))
        .hasValueSatisfying(dto -> assertThat(dto.getEntityType()).isEqualTo(EntityType.VIEW));
    assertThat(readRepository.findEntity(ADAPTER_DB, "neutral_table"))
        .hasValueSatisfying(dto -> assertThat(dto.getEntityType()).isEqualTo(EntityType.TABLE));
    assertThat(readRepository.findEntity(ADAPTER_DB, "neutral_legacy"))
        .hasValueSatisfying(dto -> assertThat(dto.getEntityType()).isEqualTo(EntityType.TABLE));

    // Reporting the default does not write it back.
    assertThat(readRawEntityType("neutral_legacy")).isEmpty();
    // Case-insensitive on the key, exactly like every other point read.
    assertThat(readRepository.findEntity(ADAPTER_DB.toUpperCase(), "NEUTRAL_VIEW")).isPresent();
    assertThat(readRepository.findEntity(ADAPTER_DB, "neutral_absent")).isEmpty();
  }

  /** The view point read hides a table and a legacy null rather than failing on them. */
  @Test
  public void testViewPointReadResolvesOnlyViews() {
    seedTypedRow("view_point", EntityType.VIEW);
    seedTypedRow("table_point", EntityType.TABLE);
    seedLegacyRow("legacy_point");

    assertThat(readRepository.findView(ADAPTER_DB, "view_point"))
        .hasValueSatisfying(dto -> assertThat(dto.getTableId()).isEqualTo("view_point"));
    assertThat(readRepository.findView(ADAPTER_DB, "table_point")).isEmpty();
    assertThat(readRepository.findView(ADAPTER_DB, "legacy_point")).isEmpty();
    assertThat(readRepository.findView(ADAPTER_DB, "absent_point")).isEmpty();

    // Hidden, not deleted.
    assertThat(readRawEntityType("table_point")).hasValue("TABLE");
    assertThat(readRawEntityType("legacy_point")).isEmpty();
  }

  /** The unbounded query is every view, and the database-scoped one is every view in it. */
  @Test
  public void testViewQueriesReturnCompleteDtoLists() {
    seedTypedRow("v1", EntityType.VIEW);
    seedTypedRow("v2", EntityType.VIEW);
    seedTypedRow("t1", EntityType.TABLE);
    seedLegacyRow("legacy");

    List<UserTableDto> everyView = readRepository.findViews(UserViewQuery.all());
    assertThat(everyView).extracting(UserTableDto::getTableId).contains("v1", "v2");
    assertThat(everyView).extracting(UserTableDto::getTableId).doesNotContain("t1", "legacy");
    assertThat(everyView)
        .allSatisfy(dto -> assertThat(dto.getEntityType()).isEqualTo(EntityType.VIEW));

    assertThat(readRepository.findViews(UserViewQuery.inDatabase(ADAPTER_DB)))
        .extracting(UserTableDto::getTableId)
        .containsExactlyInAnyOrder("v1", "v2");
  }

  /** Filtering happens before paging, so the totals describe views rather than every row. */
  @Test
  public void testPagedViewQueryFiltersBeforeItPages() {
    seedTypedRow("v1", EntityType.VIEW);
    seedTypedRow("v2", EntityType.VIEW);
    seedTypedRow("v3", EntityType.VIEW);
    seedTypedRow("t1", EntityType.TABLE);
    seedLegacyRow("legacy");

    Page<UserTableDto> page0 =
        readRepository.findViews(
            UserViewQuery.inDatabase(ADAPTER_DB), PageRequest.of(0, 2, Sort.by("tableId")));

    Assertions.assertEquals(3, page0.getTotalElements());
    Assertions.assertEquals(2, page0.getTotalPages());
    assertThat(page0.getContent()).extracting(UserTableDto::getTableId).containsExactly("v1", "v2");
  }

  /**
   * A corrupt discriminator must fail loudly rather than read as absent, because "the key is free"
   * is what lets a writer overwrite an occupant. The failure crosses the boundary as this module's
   * type, not as a raw ORM wrapper, and it still carries the column and the offending value.
   */
  @Test
  public void testCorruptRowOnTheNeutralReadIsTranslatedAndCarriesTheDiagnostic() {
    insertRawEntityType("corrupt_row", "UNKNOWN");

    assertThatThrownBy(() -> readRepository.findEntity(ADAPTER_DB, "corrupt_row"))
        .isInstanceOf(CorruptUserTableDataException.class)
        .hasStackTraceContaining("user_table_row.entity_type")
        .hasStackTraceContaining("UNKNOWN")
        .hasStackTraceContaining(CorruptEntityTypeConversionException.class.getSimpleName());

    // The row is retained for operator repair, not quietly dropped.
    assertThat(readRawEntityType("corrupt_row")).hasValue("UNKNOWN");
  }

  /** The write-preparation read is on the same boundary: it must not report corruption as free. */
  @Test
  public void testCorruptRowOnTheWritePreparationReadIsTranslated() {
    insertRawEntityType("corrupt_for_write", "UNKNOWN");

    assertThatThrownBy(() -> readRepository.findRowForWrite(ADAPTER_DB, "corrupt_for_write"))
        .isInstanceOf(CorruptUserTableDataException.class)
        .hasStackTraceContaining("user_table_row.entity_type");
  }

  /** The write-preparation read hands back a hydrated row for a healthy occupant. */
  @Test
  public void testWritePreparationReadReturnsTheHydratedOccupant() {
    seedTypedRow("occupied", EntityType.VIEW);

    assertThat(readRepository.findRowForWrite(ADAPTER_DB, "occupied"))
        .hasValueSatisfying(
            occupant -> {
              assertThat(occupant.getEntityType()).isEqualTo(EntityType.VIEW);
              assertThat(occupant.getVersion()).isNotNull();
            });
    assertThat(readRepository.findRowForWrite(ADAPTER_DB, "absent")).isEmpty();
  }

  /**
   * A corrupt row is excluded by the view predicate rather than selected by it, so a view query
   * beside one still answers completely. This is the H2 case; the collation-dependent case where
   * SQL matches a spelling Java rejects is covered by the adapter unit tests.
   */
  @Test
  public void testViewQueryBesideACorruptRowStillReturnsEveryView() {
    seedTypedRow("v1", EntityType.VIEW);
    insertRawEntityType("corrupt_row", "UNKNOWN");

    assertThat(readRepository.findViews(UserViewQuery.inDatabase(ADAPTER_DB)))
        .extracting(UserTableDto::getTableId)
        .containsExactly("v1");
    assertThat(readRawEntityType("corrupt_row")).hasValue("UNKNOWN");
  }
}
