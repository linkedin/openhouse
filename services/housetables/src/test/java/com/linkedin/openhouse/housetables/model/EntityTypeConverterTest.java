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
   * 'TABLE'}, so it resolves rather than throwing. An accented spelling still throws.
   */
  @ParameterizedTest
  @ValueSource(strings = {"FOO", "", " ", "TABLES", "TÁBLE", "TAB LE"})
  void unrecognizedColumnValueFailsLoudly(String columnValue) {
    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> converter.convertToEntityAttribute(columnValue));
    Assertions.assertTrue(thrown.getMessage().contains("user_table_row.entity_type"));
    Assertions.assertTrue(thrown.getMessage().contains(columnValue));
  }

  /**
   * Surrounding whitespace is not corruption. MySQL's PAD SPACE collations already treat {@code
   * 'TABLE '} as equal to {@code 'TABLE'}, so the SQL predicates match such a row; Java refusing it
   * would disagree with storage about a value storage calls identical, and turn a match into a 500.
   */
  @Test
  void surroundingWhitespaceResolvesToItsConstant() {
    Assertions.assertEquals(EntityType.TABLE, EntityType.fromName(" TABLE "));
    Assertions.assertEquals(EntityType.TABLE, EntityType.fromName("TABLE "));
    Assertions.assertEquals(EntityType.TABLE, EntityType.fromName(" table"));
    Assertions.assertEquals(EntityType.VIEW, EntityType.fromName(" VIEW "));
    Assertions.assertEquals(EntityType.VIEW, EntityType.fromName("VIEW "));
    Assertions.assertEquals(EntityType.VIEW, EntityType.fromName(" view"));

    // The read path is what produces CorruptEntityTypeException, so it must agree.
    Assertions.assertEquals(EntityType.TABLE, converter.convertToEntityAttribute("TABLE "));
    Assertions.assertEquals(EntityType.TABLE, converter.convertToEntityAttribute(" TaBlE "));
    Assertions.assertEquals(EntityType.VIEW, converter.convertToEntityAttribute(" view"));
  }

  /** Trimming does not soften the vocabulary: an accented value is still corruption. */
  @Test
  void accentedSpellingIsStillCorrupt() {
    Assertions.assertThrows(
        CorruptEntityTypeException.class, () -> converter.convertToEntityAttribute(" TÁBLE "));
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
