package com.linkedin.openhouse.tables.api.validator;

import com.linkedin.openhouse.common.schema.IcebergSchemaHelper;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Pins the exception taxonomy {@code OpenHouseViewsApiValidator#validateSchema} depends on.
 *
 * <p>That method catches {@link IllegalArgumentException} and {@link UncheckedIOException} rather
 * than bare {@code Exception}, so a genuine parser defect propagates as a server fault instead of
 * being reported to the caller as a bad request. The narrowing is only correct while those two
 * types remain exactly what Iceberg raises for caller-supplied text, and that is a property of a
 * third-party library which an upgrade can change silently.
 *
 * <p>Iceberg splits the failure in two, and both halves are reachable from a request body:
 *
 * <ul>
 *   <li>Text that Jackson cannot parse as JSON at all fails inside {@code JsonUtil.parse}, which
 *       wraps the {@code IOException} in an {@link UncheckedIOException}.
 *   <li>Structurally valid JSON that is not an Iceberg schema fails Iceberg's own semantic checks
 *       and surfaces as an {@link IllegalArgumentException}.
 * </ul>
 *
 * <p>Catching only the second would let a merely malformed document escape and be reported as a
 * 500. This test fails the moment that split moves.
 *
 * <p>The complementary assertion — that all of these inputs are reported to the caller as a 400
 * carrying the same fixed, redacted schema message — lives in {@code
 * ViewsValidatorTest#validateRejectsEverySchemaIcebergCannotParse}, which drives the same three
 * fixtures through the validator itself.
 */
public class ViewSchemaParseBoundaryTest {

  /**
   * The types {@code validateSchema} catches. Any parse failure not assignable to one of these
   * escapes the validator and becomes a 500.
   */
  private static final List<Class<? extends RuntimeException>> CAUGHT_BY_VALIDATOR =
      Arrays.asList(IllegalArgumentException.class, UncheckedIOException.class);

  private static Stream<Arguments> unparseableSchemas() {
    return Stream.of(
        Arguments.of(
            "syntactically malformed JSON",
            ViewModelConstants.MALFORMED_SCHEMA_LITERAL,
            UncheckedIOException.class),
        Arguments.of("text that is not JSON at all", "not json at all", UncheckedIOException.class),
        Arguments.of(
            "Spark StructType JSON",
            ViewModelConstants.SPARK_STRUCT_TYPE_SCHEMA_LITERAL,
            IllegalArgumentException.class),
        Arguments.of(
            "duplicate field ids",
            ViewModelConstants.DUPLICATE_FIELD_ID_SCHEMA_LITERAL,
            IllegalArgumentException.class));
  }

  @ParameterizedTest(name = "{0} is rejected as {2}")
  @MethodSource("unparseableSchemas")
  public void icebergRejectsEachUnparseableSchemaWithATypeTheValidatorCatches(
      String description, String schema, Class<? extends RuntimeException> expectedType) {
    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> IcebergSchemaHelper.getSchemaFromSchemaJson(schema),
            description + " must not parse as an Iceberg schema.");

    Assertions.assertEquals(
        expectedType,
        thrown.getClass(),
        description
            + " must keep failing as "
            + expectedType.getSimpleName()
            + ". If Iceberg changed this, the catch clause in OpenHouseViewsApiValidator"
            + ".validateSchema has to change with it.");

    Assertions.assertTrue(
        CAUGHT_BY_VALIDATOR.stream().anyMatch(caught -> caught.isInstance(thrown)),
        description
            + " throws "
            + thrown.getClass().getName()
            + ", which OpenHouseViewsApiValidator.validateSchema does not catch, so a client"
            + " sending it would receive a 500 instead of a 400.");
  }
}
