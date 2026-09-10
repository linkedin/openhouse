package com.linkedin.openhouse.housetables.model;

import com.linkedin.openhouse.common.exception.CorruptEntityTypeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit-level contract for the converter. {@code HtsRepositoryTest} pins the same asymmetry through
 * a real JDBC read and write; this fixes it without a database in the way.
 */
public class EntityTypeConverterTest {

  private final EntityTypeConverter converter = new EntityTypeConverter();

  /** Read is total: a legacy null column is a table. */
  @Test
  void nullColumnReadsAsTable() {
    Assertions.assertEquals(EntityType.TABLE, converter.convertToEntityAttribute(null));
  }

  /** Read normalizes case, so it agrees with the case-insensitive table predicate in SQL. */
  @ParameterizedTest
  @CsvSource({
    "TABLE, TABLE",
    "table, TABLE",
    "TaBlE, TABLE",
    "VIEW,  VIEW",
    "view,  VIEW",
    "ViEw,  VIEW"
  })
  void everySpellingReadsAsItsConstant(String columnValue, EntityType expected) {
    Assertions.assertEquals(expected, converter.convertToEntityAttribute(columnValue));
  }

  /**
   * Only a null carries the legacy meaning. The escape is deliberately not an {@link
   * IllegalArgumentException}: that lands on the shared advice's 400 branch, reporting server-state
   * corruption as a bad request.
   */
  @ParameterizedTest
  @ValueSource(strings = {"FOO", "", " ", "TABLES", "TABLE ", "TÁBLE"})
  void unrecognizedColumnValueFailsLoudly(String columnValue) {
    CorruptEntityTypeException thrown =
        Assertions.assertThrows(
            CorruptEntityTypeException.class,
            () -> converter.convertToEntityAttribute(columnValue));
    Assertions.assertFalse(
        IllegalArgumentException.class.isAssignableFrom(CorruptEntityTypeException.class),
        "corruption must not be catchable as a client-error IllegalArgumentException");
    Assertions.assertTrue(thrown.getMessage().contains("user_table_row.entity_type"));
    Assertions.assertTrue(thrown.getMessage().contains(columnValue));
  }

  /** Write stores the constant name verbatim, so the enum changes no byte in the column. */
  @ParameterizedTest
  @CsvSource({"TABLE, TABLE", "VIEW, VIEW"})
  void everyConstantWritesAsItsName(EntityType entityType, String expectedColumnValue) {
    Assertions.assertEquals(expectedColumnValue, converter.convertToDatabaseColumn(entityType));
  }

  /**
   * The write is strict where the read defaults: the {@code IS NULL} predicate arm carries rows
   * that predate the discriminator, and nothing may add another. An {@link
   * IllegalArgumentException} because an unstamped write is a caller bug, unlike a corrupt column,
   * which is server state.
   */
  @Test
  void nullWriteIsRejectedRatherThanStoringAnotherLegacyRow() {
    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> converter.convertToDatabaseColumn(null));

    Assertions.assertEquals(
        "Column user_table_row.entity_type cannot be written as null; "
            + "the endpoint serving the request is responsible for stamping the type",
        thrown.getMessage());
    // The two escapes stay disjoint: a caller bug is not stored corruption.
    Assertions.assertFalse(CorruptEntityTypeException.class.isAssignableFrom(thrown.getClass()));
  }
}
