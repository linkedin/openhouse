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
   * Only a null carries the legacy meaning; anything else outside the vocabulary is corrupt. Any
   * padding is significant, whether leading or trailing and whether or not it is a plain space, so
   * a padded spelling matches no typed predicate and stays corrupt. An accented spelling throws
   * too.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "FOO", "", " ", "TABLES", "TÁBLE", "TAB LE", " TABLE", "TABLE ", "\tTABLE", "TABLE\t",
        "TABLE\n", "VIEW\t", " VIEW"
      })
  void unrecognizedColumnValueFailsLoudly(String columnValue) {
    CorruptEntityTypeException thrown =
        Assertions.assertThrows(
            CorruptEntityTypeException.class,
            () -> converter.convertToEntityAttribute(columnValue));
    Assertions.assertTrue(thrown.getMessage().contains("user_table_row.entity_type"));
    Assertions.assertTrue(thrown.getMessage().contains(columnValue));
  }

  /**
   * Padding is significant everywhere. A leading or trailing space, a tab or a newline makes the
   * stored value something SQL compares as distinct, so such a row matches no typed predicate and
   * must stay loudly corrupt rather than hydrating into a row nothing can mutate.
   */
  @Test
  void whitespacePaddingIsCorrupt() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName("TABLE "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName("table   "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName("VIEW "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName(" TABLE"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName(" TABLE "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName(" view"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName("TABLE\t"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName("TABLE\n"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName("VIEW\t"));

    // The read path is what produces CorruptEntityTypeException, so it must agree.
    Assertions.assertThrows(
        CorruptEntityTypeException.class, () -> converter.convertToEntityAttribute("TABLE "));
    Assertions.assertThrows(
        CorruptEntityTypeException.class, () -> converter.convertToEntityAttribute("TaBlE  "));
    Assertions.assertThrows(
        CorruptEntityTypeException.class, () -> converter.convertToEntityAttribute("view "));
    Assertions.assertThrows(
        CorruptEntityTypeException.class, () -> converter.convertToEntityAttribute(" TaBlE "));
    Assertions.assertThrows(
        CorruptEntityTypeException.class, () -> converter.convertToEntityAttribute("VIEW\n"));
  }

  /**
   * An all-space value is outside the vocabulary just like any other padded spelling; it must not
   * become a silent table.
   */
  @Test
  void allSpaceValueIsCorrupt() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName(" "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName("   "));
    Assertions.assertThrows(
        CorruptEntityTypeException.class, () -> converter.convertToEntityAttribute(" "));
  }

  /** Case is the only insignificant difference: an accented value is corruption. */
  @Test
  void accentedSpellingIsStillCorrupt() {
    Assertions.assertThrows(
        CorruptEntityTypeException.class, () -> converter.convertToEntityAttribute("TÁBLE "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName("TÁBLE"));
  }

  /**
   * The dedicated type exists so the advice can bind an explicit server-error branch: however the
   * value got there, stored state the vocabulary does not admit is a server failure, so it answers
   * 500 carrying a diagnostic that names the column and the value rather than a generic body.
   */
  @Test
  void unrecognizedColumnValueThrowsTheDedicatedCorruptionType() {
    CorruptEntityTypeException thrown =
        Assertions.assertThrows(
            CorruptEntityTypeException.class, () -> converter.convertToEntityAttribute("UNKNOWN"));
    Assertions.assertTrue(thrown.getMessage().contains("user_table_row.entity_type"));
    Assertions.assertTrue(thrown.getMessage().contains("UNKNOWN"));
  }

  /**
   * The endpoint stamps at ingress, so a null reaching storage means a missed ingress path. Failing
   * here keeps the legacy-null population closed rather than growing.
   */
  @Test
  void writeConvertsKnownTypesAndRejectsNull() {
    Assertions.assertEquals("TABLE", converter.convertToDatabaseColumn(EntityType.TABLE));
    Assertions.assertEquals("VIEW", converter.convertToDatabaseColumn(EntityType.VIEW));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> converter.convertToDatabaseColumn(null));
  }

  /**
   * Null-to-TABLE must not be folded into {@link EntityType#fromName}: from the column a null means
   * "written before the discriminator existed", from the wire "the caller did not say".
   */
  @Test
  void fromNameRejectsNull() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName(null));
  }
}
