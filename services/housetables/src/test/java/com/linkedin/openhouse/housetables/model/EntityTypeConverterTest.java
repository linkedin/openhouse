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
   * Only a null carries the legacy meaning; anything else outside the vocabulary is corrupt. {@code
   * 'TABLE '} is deliberately absent: MySQL's PAD SPACE collation calls it the same value as {@code
   * 'TABLE'}, so it resolves rather than throwing. A leading space or a non-space whitespace
   * character is significant to SQL and so stays corrupt. An accented spelling still throws.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "FOO", "", " ", "TABLES", "TÁBLE", "TAB LE", " TABLE", "\tTABLE", "TABLE\t", "TABLE\n",
        "VIEW\t", " VIEW"
      })
  void unrecognizedColumnValueFailsLoudly(String columnValue) {
    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> converter.convertToEntityAttribute(columnValue));
    Assertions.assertTrue(thrown.getMessage().contains("user_table_row.entity_type"));
    Assertions.assertTrue(thrown.getMessage().contains(columnValue));
  }

  /**
   * Trailing spaces are not corruption. MySQL's PAD SPACE collations already treat {@code 'TABLE '}
   * as equal to {@code 'TABLE'}, so the SQL predicates match such a row; Java refusing it would
   * disagree with storage about a value storage calls identical, and turn a match into a 500.
   */
  @Test
  void trailingSpacesResolveToTheirConstant() {
    Assertions.assertEquals(EntityType.TABLE, EntityType.fromName("TABLE "));
    Assertions.assertEquals(EntityType.TABLE, EntityType.fromName("table   "));
    Assertions.assertEquals(EntityType.VIEW, EntityType.fromName("VIEW "));
    Assertions.assertEquals(EntityType.VIEW, EntityType.fromName("view   "));

    // The read path is what produces CorruptEntityTypeException, so it must agree.
    Assertions.assertEquals(EntityType.TABLE, converter.convertToEntityAttribute("TABLE "));
    Assertions.assertEquals(EntityType.TABLE, converter.convertToEntityAttribute("TaBlE  "));
    Assertions.assertEquals(EntityType.VIEW, converter.convertToEntityAttribute("view "));
  }

  /**
   * Padding is ignored only where the collation ignores it. A leading space, a tab or a newline is
   * significant to SQL, so such a row matches no typed predicate and must stay loudly corrupt
   * rather than hydrating into a row nothing can mutate.
   */
  @Test
  void leadingOrNonSpaceWhitespaceIsCorrupt() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName(" TABLE"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName(" TABLE "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName(" view"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName("TABLE\t"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName("TABLE\n"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName("VIEW\t"));

    Assertions.assertThrows(
        CorruptEntityTypeException.class, () -> converter.convertToEntityAttribute(" TaBlE "));
    Assertions.assertThrows(
        CorruptEntityTypeException.class, () -> converter.convertToEntityAttribute("VIEW\n"));
  }

  /**
   * An all-space value strips to the empty string, which is outside the vocabulary; it must not
   * become a silent table.
   */
  @Test
  void allSpaceValueIsCorrupt() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName(" "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName("   "));
    Assertions.assertThrows(
        CorruptEntityTypeException.class, () -> converter.convertToEntityAttribute(" "));
  }

  /** Ignoring trailing spaces does not soften the vocabulary: an accented value is corruption. */
  @Test
  void accentedSpellingIsStillCorrupt() {
    Assertions.assertThrows(
        CorruptEntityTypeException.class, () -> converter.convertToEntityAttribute("TÁBLE "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> EntityType.fromName("TÁBLE"));
  }

  /**
   * The dedicated type exists so the advice can bind an explicit server-error branch: the inherited
   * {@code IllegalArgumentException} advice answers 400, wrong for a row the server itself wrote.
   * It stays an {@link IllegalArgumentException} so existing callers keep catching it.
   */
  @Test
  void unrecognizedColumnValueThrowsTheDedicatedCorruptionType() {
    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> converter.convertToEntityAttribute("UNKNOWN"));
    Assertions.assertTrue(thrown instanceof CorruptEntityTypeException);
    Assertions.assertTrue(thrown.getMessage().contains("user_table_row.entity_type"));
    Assertions.assertTrue(thrown.getMessage().contains("UNKNOWN"));
  }

  /**
   * The endpoint stamps at ingress, so a null reaching storage means a missed ingress path. Failing
   * here keeps the legacy-null population closed rather than growing.
   */
  @Test
  void writeIsPassThrough() {
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
