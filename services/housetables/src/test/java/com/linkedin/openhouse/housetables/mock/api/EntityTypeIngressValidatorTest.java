package com.linkedin.openhouse.housetables.mock.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.validator.EntityTypeIngressResult;
import com.linkedin.openhouse.housetables.api.validator.EntityTypeIngressValidator;
import com.linkedin.openhouse.housetables.model.EntityType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Ingress normalization states what is wrong and returns it; it never throws. That is what keeps
 * the choice of HTTP status with the controller, and what makes the rule testable without a
 * request.
 */
public class EntityTypeIngressValidatorTest {

  private final EntityTypeIngressValidator validator = new EntityTypeIngressValidator();

  private static UserTable entity(String entityType) {
    return UserTable.builder()
        .databaseId("db1")
        .tableId("tb1")
        .tableVersion("INITIAL_VERSION")
        .metadataLocation("/openhouse/db1/tb1/v0_metadata.json")
        .entityType(entityType)
        .build();
  }

  /**
   * The wire field stays nullable for rolling deploys: an un-upgraded client sends no
   * discriminator, and the route resolves it rather than rejecting the request.
   */
  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"TABLE", "table", "TaBlE"})
  public void testTableRouteStampsCanonicalTableForAgreeingOrSilentPayloads(String declared) {
    EntityTypeIngressResult result = validator.normalize(entity(declared), EntityType.TABLE);

    Assertions.assertTrue(result.isSuccess());
    assertThat(result.getFailureMessage()).isEmpty();
    assertThat(result.getNormalizedEntity())
        .hasValueSatisfying(
            normalized -> Assertions.assertEquals("TABLE", normalized.getEntityType()));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"VIEW", "view", "ViEw"})
  public void testViewRouteStampsCanonicalViewForAgreeingOrSilentPayloads(String declared) {
    EntityTypeIngressResult result = validator.normalize(entity(declared), EntityType.VIEW);

    Assertions.assertTrue(result.isSuccess());
    assertThat(result.getNormalizedEntity())
        .hasValueSatisfying(
            normalized -> Assertions.assertEquals("VIEW", normalized.getEntityType()));
  }

  /** Every other field is carried through untouched; only the discriminator is decided here. */
  @Test
  public void testNormalizationChangesNothingButTheDiscriminator() {
    UserTable submitted = entity(null);

    UserTable normalized =
        validator
            .normalize(submitted, EntityType.VIEW)
            .getNormalizedEntity()
            .orElseThrow(() -> new AssertionError("a silent payload must normalize"));

    Assertions.assertEquals(submitted.getDatabaseId(), normalized.getDatabaseId());
    Assertions.assertEquals(submitted.getTableId(), normalized.getTableId());
    Assertions.assertEquals(submitted.getTableVersion(), normalized.getTableVersion());
    Assertions.assertEquals(submitted.getMetadataLocation(), normalized.getMetadataLocation());
    Assertions.assertEquals(submitted.getStorageType(), normalized.getStorageType());
    Assertions.assertEquals(submitted.getCreationTime(), normalized.getCreationTime());
  }

  /**
   * The endpoint declares the type; a payload may agree with it or stay silent, never override it.
   * The message names both sides so the caller can see which route it actually reached.
   */
  @ParameterizedTest
  @CsvSource({"TABLE, VIEW", "TABLE, view", "VIEW, TABLE", "VIEW, TaBlE"})
  public void testContradictoryDeclaredTypeIsAFailureNamingBothSides(
      EntityType routeEntityType, String declared) {
    EntityTypeIngressResult result = validator.normalize(entity(declared), routeEntityType);

    Assertions.assertFalse(result.isSuccess());
    assertThat(result.getNormalizedEntity()).isEmpty();
    assertThat(result.getFailureMessage())
        .hasValue(
            String.format(
                EntityTypeIngressValidator.TYPE_MISMATCH_MESSAGE_FORMAT,
                declared,
                routeEntityType.name()));
  }

  /**
   * An unrecognized spelling is neither agreement nor silence, so it is rejected here too rather
   * than being carried to a boundary that would answer with a server error.
   */
  @ParameterizedTest
  @ValueSource(strings = {"UNKNOWN", "", " ", "TABLES", "TABLE "})
  public void testUnrecognizedDeclaredTypeIsAFailure(String declared) {
    EntityTypeIngressResult result = validator.normalize(entity(declared), EntityType.TABLE);

    Assertions.assertFalse(result.isSuccess());
    assertThat(result.getNormalizedEntity()).isEmpty();
    assertThat(result.getFailureMessage()).isPresent();
  }

  /**
   * Ingress runs ahead of the validator, so it is the first code to see an absent entity, and
   * reporting it rather than dereferencing it is its job.
   */
  @ParameterizedTest
  @org.junit.jupiter.params.provider.EnumSource(EntityType.class)
  public void testAbsentEntityIsAFailureOnEveryRoute(EntityType routeEntityType) {
    EntityTypeIngressResult result = validator.normalize(null, routeEntityType);

    Assertions.assertFalse(result.isSuccess());
    assertThat(result.getNormalizedEntity()).isEmpty();
    assertThat(result.getFailureMessage())
        .hasValue(EntityTypeIngressValidator.EMPTY_ENTITY_MESSAGE);
  }

  /** The result type itself cannot represent a half state. */
  @Test
  public void testResultCannotBeBothOutcomesAtOnce() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> EntityTypeIngressResult.success(null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> EntityTypeIngressResult.failure(null));
  }
}
