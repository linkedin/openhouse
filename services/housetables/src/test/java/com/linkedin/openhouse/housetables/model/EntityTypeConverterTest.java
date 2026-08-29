package com.linkedin.openhouse.housetables.model;

import com.linkedin.openhouse.housetables.exception.CorruptEntityTypeConversionException;
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
   * Only a null carries the legacy meaning; anything else outside the vocabulary is corrupt.
   *
   * <p>Corrupt storage is a server-state failure whatever wrote it, so the escape is deliberately
   * not an {@link IllegalArgumentException}: that would land it on the shared advice's 400 branch
   * and report bad data as a bad request.
   */
  @ParameterizedTest
  @ValueSource(strings = {"FOO", "", " ", "TABLES", "TABLE ", "TÁBLE"})
  void unrecognizedColumnValueFailsLoudly(String columnValue) {
    CorruptEntityTypeConversionException thrown =
        Assertions.assertThrows(
            CorruptEntityTypeConversionException.class,
            () -> converter.convertToEntityAttribute(columnValue));
    Assertions.assertFalse(
        IllegalArgumentException.class.isAssignableFrom(CorruptEntityTypeConversionException.class),
        "corruption must not be catchable as a client-error IllegalArgumentException");
    Assertions.assertTrue(thrown.getMessage().contains("user_table_row.entity_type"));
    Assertions.assertTrue(thrown.getMessage().contains(columnValue));
  }

  /**
   * Write is faithful, not total. Stamping a type belongs to the endpoint that knows which one it
   * is, so storage passes a null through rather than inventing TABLE.
   */
  @Test
  void writeIsPassThrough() {
    Assertions.assertEquals("TABLE", converter.convertToDatabaseColumn(EntityType.TABLE));
    Assertions.assertEquals("VIEW", converter.convertToDatabaseColumn(EntityType.VIEW));
    Assertions.assertNull(converter.convertToDatabaseColumn(null));
  }
}
