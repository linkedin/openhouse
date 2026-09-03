package com.linkedin.openhouse.housetables.mock.api;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.validator.EntityTypeIngressValidator;
import com.linkedin.openhouse.housetables.model.EntityType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

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

  /** The wire field stays nullable for rolling deploys: the route resolves it, never rejects it. */
  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"TABLE", "table", "TaBlE"})
  public void testTableRouteStampsCanonicalTableForAgreeingOrSilentPayloads(String declared) {
    Assertions.assertEquals(
        "TABLE", validator.normalize(entity(declared), EntityType.TABLE).getEntityType());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"VIEW", "view", "ViEw"})
  public void testViewRouteStampsCanonicalViewForAgreeingOrSilentPayloads(String declared) {
    Assertions.assertEquals(
        "VIEW", validator.normalize(entity(declared), EntityType.VIEW).getEntityType());
  }

  /** Every other field is carried through untouched; only the discriminator is decided here. */
  @Test
  public void testNormalizationChangesNothingButTheDiscriminator() {
    UserTable submitted = entity(null);

    UserTable normalized = validator.normalize(submitted, EntityType.VIEW);

    Assertions.assertEquals(submitted.getDatabaseId(), normalized.getDatabaseId());
    Assertions.assertEquals(submitted.getTableId(), normalized.getTableId());
    Assertions.assertEquals(submitted.getTableVersion(), normalized.getTableVersion());
    Assertions.assertEquals(submitted.getMetadataLocation(), normalized.getMetadataLocation());
    Assertions.assertEquals(submitted.getStorageType(), normalized.getStorageType());
    Assertions.assertEquals(submitted.getCreationTime(), normalized.getCreationTime());
  }

  /** The message names both sides, so the caller can see which route it actually reached. */
  @ParameterizedTest
  @CsvSource({"TABLE, VIEW", "TABLE, view", "VIEW, TABLE", "VIEW, TaBlE"})
  public void testContradictoryDeclaredTypeIsRejectedNamingBothSides(
      EntityType routeEntityType, String declared) {
    RequestValidationFailureException thrown =
        Assertions.assertThrows(
            RequestValidationFailureException.class,
            () -> validator.normalize(entity(declared), routeEntityType));

    Assertions.assertEquals(
        String.format(
            EntityTypeIngressValidator.TYPE_MISMATCH_MESSAGE_FORMAT,
            declared,
            routeEntityType.name()),
        thrown.getMessage());
  }

  /**
   * Neither agreement nor silence, so it is rejected here rather than at a 500-answering boundary.
   */
  @ParameterizedTest
  @ValueSource(strings = {"UNKNOWN", "", " ", "TABLES", "TABLE "})
  public void testUnrecognizedDeclaredTypeIsRejected(String declared) {
    Assertions.assertThrows(
        RequestValidationFailureException.class,
        () -> validator.normalize(entity(declared), EntityType.TABLE));
  }

  /** Ingress runs ahead of the validator, so it is the first code to see an absent entity. */
  @ParameterizedTest
  @EnumSource(EntityType.class)
  public void testAbsentEntityIsRejectedOnEveryRoute(EntityType routeEntityType) {
    RequestValidationFailureException thrown =
        Assertions.assertThrows(
            RequestValidationFailureException.class,
            () -> validator.normalize(null, routeEntityType));

    Assertions.assertEquals(EntityTypeIngressValidator.EMPTY_ENTITY_MESSAGE, thrown.getMessage());
  }
}
