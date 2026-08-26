package com.linkedin.openhouse.tables.mock.api;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation;
import com.linkedin.openhouse.tables.api.validator.ViewsApiValidator;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.exception.ViewRequestValidationFailureException;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * The half of the dialect rule that a single-dialect deployment cannot demonstrate: what a
 * deployment that configures a second dialect actually accepts.
 *
 * <p>{@link ViewsValidatorTest} covers the default configuration, where {@code spark} is the only
 * supported dialect. That default makes "supported" and "the only one" indistinguishable, so it
 * cannot show that the rule is a membership test against configuration rather than a count of the
 * list or a literal comparison against Spark. This class configures {@code spark,trino} and asserts
 * the difference that makes: a two-representation request is accepted, and every rule that is not
 * about the supported set still rejects what it did before.
 */
@SpringBootTest(properties = "cluster.tables.views.supported-dialects=spark,trino")
public class ViewsValidatorMultiDialectTest {

  private static final ViewRepresentation TRINO_REPRESENTATION =
      ViewModelConstants.SPARK_REPRESENTATION.toBuilder().dialect("trino").build();

  @Autowired private ViewsApiValidator viewsApiValidator;

  @Autowired private ClusterProperties clusterProperties;

  @Test
  public void validateAcceptsOneRepresentationPerConfiguredDialect() {
    assertDoesNotThrow(
        createOf(
            requestWith(
                Arrays.asList(ViewModelConstants.SPARK_REPRESENTATION, TRINO_REPRESENTATION))),
        "Both dialects are configured and neither is duplicated, so the request is unambiguous and"
            + " must be accepted; the old rule rejected it purely for its length.");
  }

  /** The source may name either configured dialect, not just the one that happens to be first. */
  @Test
  public void validateAcceptsAConfiguredSourceDialectThatIsNotSpark() {
    assertDoesNotThrow(
        createOf(
            requestWith(
                    Arrays.asList(ViewModelConstants.SPARK_REPRESENTATION, TRINO_REPRESENTATION))
                .toBuilder()
                .sourceDialect("trino")
                .build()),
        "trino is configured and is supplied as a representation, so it is a legal source dialect.");

    assertDoesNotThrow(
        createOf(
            requestWith(Arrays.asList(TRINO_REPRESENTATION))
                .toBuilder()
                .sourceDialect("trino")
                .build()),
        "A view defined only in trino is legal once trino is configured.");
  }

  /** Widening the set widens it by exactly what was configured, and by nothing else. */
  @Test
  public void validateStillRejectsADialectOutsideTheConfiguredSet() {
    assertRejected(
        createOf(
            requestWith(
                Arrays.asList(
                    ViewModelConstants.SPARK_REPRESENTATION,
                    ViewModelConstants.SPARK_REPRESENTATION
                        .toBuilder()
                        .dialect("presto")
                        .build()))),
        ViewErrorCode.UNSUPPORTED_VIEW_DIALECT,
        "representations[1].dialect : must be one of the supported dialects: spark, trino");

    assertRejected(
        createOf(
            requestWith(Arrays.asList(ViewModelConstants.SPARK_REPRESENTATION))
                .toBuilder()
                .sourceDialect("presto")
                .build()),
        ViewErrorCode.UNSUPPORTED_VIEW_DIALECT,
        "sourceDialect : must be one of the supported dialects: spark, trino");
  }

  /**
   * Uniqueness is what makes dropping the count rule safe, so it has to keep holding for a dialect
   * that only a widened configuration can reach.
   */
  @Test
  public void validateStillRejectsDuplicateDialectsCaseInsensitively() {
    assertRejected(
        createOf(
            requestWith(
                Arrays.asList(
                    TRINO_REPRESENTATION,
                    TRINO_REPRESENTATION.toBuilder().dialect("TRINO").build()))),
        ViewErrorCode.UNSUPPORTED_VIEW_DIALECT,
        "representations : dialects must be unique, duplicated: trino");
  }

  /** Configuring a dialect widens the supported set; it does not relax the exact-lowercase rule. */
  @Test
  public void validateStillRejectsAConfiguredDialectSuppliedInTheWrongCase() {
    assertRejected(
        createOf(
            requestWith(
                Arrays.asList(
                    ViewModelConstants.SPARK_REPRESENTATION,
                    TRINO_REPRESENTATION.toBuilder().dialect("Trino").build()))),
        ViewErrorCode.UNSUPPORTED_VIEW_DIALECT,
        "representations[1].dialect : must be one of the supported dialects: spark, trino");
  }

  private CreateUpdateViewRequestBody requestWith(List<ViewRepresentation> representations) {
    return ViewModelConstants.createRequestWithoutBaseVersion()
        .toBuilder()
        .clusterId(clusterProperties.getClusterName())
        .representations(representations)
        .build();
  }

  private Executable createOf(CreateUpdateViewRequestBody requestBody) {
    return () ->
        viewsApiValidator.validateCreateView(
            clusterProperties.getClusterName(), ViewModelConstants.DATABASE_ID, requestBody);
  }

  private void assertRejected(
      Executable executable, ViewErrorCode expectedCode, String expectedMessage) {
    ViewRequestValidationFailureException exception =
        Assertions.assertThrows(ViewRequestValidationFailureException.class, executable);
    Assertions.assertTrue(
        exception.getMessage().contains(expectedMessage),
        String.format(
            "Expected the failure to report \"%s\" but it reported \"%s\"",
            expectedMessage, exception.getMessage()));
    Assertions.assertEquals(expectedCode, exception.getErrorCode());
  }
}
