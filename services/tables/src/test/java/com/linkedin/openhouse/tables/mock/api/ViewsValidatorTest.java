package com.linkedin.openhouse.tables.mock.api;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.MAX_VIEW_IDENTIFIER_LENGTH;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.MAX_VIEW_SCHEMA_BYTES;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.MAX_VIEW_SQL_BYTES;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation;
import com.linkedin.openhouse.tables.api.validator.ViewsApiValidator;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.exception.ViewRequestValidationFailureException;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Coverage of {@link ViewsApiValidator}: the requests it must accept, and every structural rule it
 * must reject.
 *
 * <p>Rejections are asserted on <b>both</b> the exact message and the internal {@link
 * ViewErrorCode}. The code never reaches the wire, so this test is the only place its selection is
 * observable, and all three 400-mapped codes are otherwise indistinguishable from a client's point
 * of view.
 *
 * <p>Nothing here parses or interprets SQL: SQL is opaque to the server and the only rule applied
 * to it is a size ceiling.
 */
@SpringBootTest
public class ViewsValidatorTest {

  @Autowired private ViewsApiValidator viewsApiValidator;

  @Autowired private ClusterProperties clusterProperties;

  /**
   * {@link ViewModelConstants} fixes a literal cluster id so the contract test stays byte-stable,
   * but the validator compares against the cluster this server is actually serving. Rebind it here.
   */
  private CreateUpdateViewRequestBody servingCluster(CreateUpdateViewRequestBody requestBody) {
    return requestBody.toBuilder().clusterId(clusterProperties.getClusterName()).build();
  }

  @Test
  public void validateCreateViewAcceptsBothLegalPostTokenForms() {
    assertDoesNotThrow(
        () ->
            viewsApiValidator.validateCreateView(
                clusterProperties.getClusterName(),
                ViewModelConstants.DATABASE_ID,
                servingCluster(ViewModelConstants.createRequestWithoutBaseVersion())),
        "A POST that omits baseViewVersion entirely is the plain create shape and must be accepted.");

    assertDoesNotThrow(
        () ->
            viewsApiValidator.validateCreateView(
                clusterProperties.getClusterName(),
                ViewModelConstants.DATABASE_ID,
                servingCluster(ViewModelConstants.createRequestWithInitialBaseVersion())),
        "The Iceberg client sends "
            + INITIAL_TABLE_VERSION
            + " on create, so that form must be accepted too.");
  }

  @Test
  public void validateUpdateViewAcceptsAnOpaqueBaseVersionToken() {
    CreateUpdateViewRequestBody request =
        servingCluster(ViewModelConstants.fullyPopulatedRequest())
            .toBuilder()
            // Deliberately not a metadata path: PUT treats the token as fully opaque.
            .baseViewVersion("an-entirely-opaque-token")
            .build();

    assertDoesNotThrow(
        () ->
            viewsApiValidator.validateUpdateView(
                clusterProperties.getClusterName(),
                ViewModelConstants.DATABASE_ID,
                ViewModelConstants.VIEW_ID,
                request));
  }

  @Test
  public void validateIdentifierAndPagingRoutesAcceptValidInput() {
    assertDoesNotThrow(
        () ->
            viewsApiValidator.validateGetView(
                ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID));

    assertDoesNotThrow(
        () ->
            viewsApiValidator.validateDeleteView(
                ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID));

    assertDoesNotThrow(
        () -> viewsApiValidator.validateGetAllViews(ViewModelConstants.DATABASE_ID, 0, 50, null),
        "The controller's default paging values must pass unchanged.");

    assertDoesNotThrow(
        () ->
            viewsApiValidator.validateGetAllViews(
                ViewModelConstants.DATABASE_ID, 3, 10000, "viewId"),
        "A single sort field and a large page size are both legal: view paging deliberately has no"
            + " upper size cap, matching the shared table paging rules.");
  }

  @Test
  public void validateGetViewAcceptsMaximumLengthIdentifiers() {
    String maxLengthId = String.join("", Collections.nCopies(MAX_VIEW_IDENTIFIER_LENGTH, "a"));
    Assertions.assertEquals(MAX_VIEW_IDENTIFIER_LENGTH, maxLengthId.length());

    assertDoesNotThrow(
        () -> viewsApiValidator.validateGetView(maxLengthId, maxLengthId),
        "The identifier length limit is inclusive, so an identifier of exactly"
            + " MAX_VIEW_IDENTIFIER_LENGTH characters must still be accepted.");
  }

  // ---------------------------------------------------------------------------------------------
  // Negative paths
  // ---------------------------------------------------------------------------------------------

  private static final String SCHEMA_PARSE_MESSAGE =
      "schema : must be valid Iceberg schema JSON; Spark StructType JSON is not supported";

  /**
   * Asserts the request is rejected, that the reported reason is exactly {@code expectedMessage},
   * and that the internal code is the expected one. The code is asserted everywhere because it is
   * never serialized: all three 400-mapped codes look identical to a client, so this test class is
   * the only place their selection is observable.
   */
  private ViewRequestValidationFailureException assertRejected(
      Executable executable, ViewErrorCode expectedCode, String expectedMessage) {
    ViewRequestValidationFailureException exception =
        Assertions.assertThrows(ViewRequestValidationFailureException.class, executable);
    Assertions.assertTrue(
        exception.getMessage().contains(expectedMessage),
        String.format(
            "Expected the failure to report \"%s\" but it reported \"%s\"",
            expectedMessage, exception.getMessage()));
    Assertions.assertEquals(
        expectedCode,
        exception.getErrorCode(),
        "The internal code must be the most specific one the accumulated failures warrant.");
    return exception;
  }

  /** A request that a POST would accept unchanged: serving cluster, no base version. */
  private CreateUpdateViewRequestBody validCreateRequest() {
    return servingCluster(ViewModelConstants.createRequestWithoutBaseVersion());
  }

  /** A request that a PUT would accept unchanged: serving cluster, replace token present. */
  private CreateUpdateViewRequestBody validUpdateRequest() {
    return servingCluster(ViewModelConstants.fullyPopulatedRequest());
  }

  private CreateUpdateViewRequestBody createRequestWith(List<ViewRepresentation> representations) {
    return validCreateRequest().toBuilder().representations(representations).build();
  }

  private CreateUpdateViewRequestBody createRequestWith(ViewRepresentation representation) {
    return createRequestWith(Collections.singletonList(representation));
  }

  private static ViewRepresentation sparkRepresentationWithSql(String sql) {
    return ViewModelConstants.SPARK_REPRESENTATION.toBuilder().sql(sql).build();
  }

  private Executable createOf(CreateUpdateViewRequestBody requestBody) {
    return () ->
        viewsApiValidator.validateCreateView(
            clusterProperties.getClusterName(), ViewModelConstants.DATABASE_ID, requestBody);
  }

  private Executable updateOf(CreateUpdateViewRequestBody requestBody) {
    return () ->
        viewsApiValidator.validateUpdateView(
            clusterProperties.getClusterName(),
            ViewModelConstants.DATABASE_ID,
            ViewModelConstants.VIEW_ID,
            requestBody);
  }

  @Test
  public void validateRejectsIdentifierMismatchesAgainstPathAndCluster() {
    assertRejected(
        createOf(validCreateRequest().toBuilder().databaseId("another_database").build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        String.format(
            "databaseId : provided %s, doesn't match with the RequestBody another_database",
            ViewModelConstants.DATABASE_ID));

    assertRejected(
        updateOf(validUpdateRequest().toBuilder().viewId("another_view").build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        String.format(
            "viewId : provided %s, doesn't match with the RequestBody another_view",
            ViewModelConstants.VIEW_ID));

    assertRejected(
        createOf(validCreateRequest().toBuilder().clusterId("not-the-serving-cluster").build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        String.format(
            "clusterId : provided not-the-serving-cluster, doesn't match with the server cluster %s",
            clusterProperties.getClusterName()));
  }

  @Test
  public void validateRejectsMissingRequiredBodyFields() {
    assertRejected(
        createOf(validCreateRequest().toBuilder().schema(null).build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "CreateUpdateViewRequestBody.schema : schema cannot be empty");

    assertRejected(
        createOf(validCreateRequest().toBuilder().sourceDialect(null).build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "CreateUpdateViewRequestBody.sourceDialect : sourceDialect cannot be empty");

    assertRejected(
        createOf(createRequestWith(Collections.<ViewRepresentation>emptyList())),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "CreateUpdateViewRequestBody.representations : representations cannot be empty");
  }

  @Test
  public void validateGetViewRejectsEmptyAndMalformedIdentifiers() {
    assertRejected(
        () -> viewsApiValidator.validateGetView("", ViewModelConstants.VIEW_ID),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "databaseId : Cannot be empty");

    assertRejected(
        () -> viewsApiValidator.validateGetView(ViewModelConstants.DATABASE_ID, "bad view!"),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "viewId : provided bad view!, Only alphanumerics and underscore supported");

    assertRejected(
        () -> viewsApiValidator.validateDeleteView(ViewModelConstants.DATABASE_ID, ""),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "viewId : Cannot be empty");
  }

  /**
   * <b>Known deviation from the table convention.</b> {@code OpenHouseTablesApiValidator} checks
   * only emptiness and the character regex on path identifiers, so this length rule is new
   * behaviour rather than a copy of the table rule. It is here because an over-long identifier
   * would otherwise pass validation and come back as a misleading 404 rather than a 400 naming the
   * actual problem. If the table convention is preferred, removing this rule and this test is the
   * whole change.
   */
  @Test
  public void validateGetViewRejectsOverLongIdentifiersUnlikeTheTableValidator() {
    String tooLong = String.join("", Collections.nCopies(MAX_VIEW_IDENTIFIER_LENGTH + 1, "a"));

    ViewRequestValidationFailureException exception =
        assertRejected(
            () -> viewsApiValidator.validateGetView(ViewModelConstants.DATABASE_ID, tooLong),
            ViewErrorCode.INVALID_VIEW_DEFINITION,
            String.format(
                "viewId : exceeds the maximum length of %d characters",
                MAX_VIEW_IDENTIFIER_LENGTH));

    Assertions.assertFalse(
        exception.getMessage().contains(tooLong),
        "An over-long identifier is by definition large and the message is copied into the error"
            + " body and into audit events, so it must not be echoed back.");
  }

  /**
   * Malformed JSON, the Spark {@code StructType} shape an engine might send by mistake, and a
   * schema with duplicate field ids all fail inside Iceberg's parser and must collapse to the same
   * fixed message. The duplicate-id case is why the validator deliberately has no duplicate-id
   * check of its own.
   */
  @ParameterizedTest
  @ValueSource(strings = {"malformed", "sparkStructType", "duplicateFieldIds"})
  public void validateRejectsEverySchemaIcebergCannotParse(String variant) {
    String schema;
    if ("malformed".equals(variant)) {
      schema = ViewModelConstants.MALFORMED_SCHEMA_LITERAL;
    } else if ("sparkStructType".equals(variant)) {
      schema = ViewModelConstants.SPARK_STRUCT_TYPE_SCHEMA_LITERAL;
    } else {
      schema = ViewModelConstants.DUPLICATE_FIELD_ID_SCHEMA_LITERAL;
    }

    ViewRequestValidationFailureException exception =
        assertRejected(
            createOf(validCreateRequest().toBuilder().schema(schema).build()),
            ViewErrorCode.UNSUPPORTED_VIEW_SCHEMA,
            SCHEMA_PARSE_MESSAGE);

    Assertions.assertFalse(
        exception.getMessage().contains("fields"),
        "Iceberg's own parse message can echo the submitted schema, so the validator must replace"
            + " it with a fixed one rather than wrap it.");
  }

  @Test
  public void validateEnforcesTheSchemaUtf8ByteBoundary() {
    assertDoesNotThrow(
        createOf(
            validCreateRequest()
                .toBuilder()
                .schema(ViewModelConstants.schemaAtMaxUtf8Size())
                .build()),
        "The limit is inclusive: a schema of exactly the maximum size must be accepted.");

    assertRejected(
        createOf(
            validCreateRequest()
                .toBuilder()
                .schema(ViewModelConstants.schemaOneByteOverMaxUtf8Size())
                .build()),
        ViewErrorCode.UNSUPPORTED_VIEW_SCHEMA,
        String.format("schema : exceeds maximum UTF-8 size of %d bytes", MAX_VIEW_SCHEMA_BYTES));
  }

  @Test
  public void validateEnforcesTheSqlUtf8ByteBoundary() {
    assertDoesNotThrow(
        createOf(
            createRequestWith(sparkRepresentationWithSql(ViewModelConstants.sqlAtMaxUtf8Size()))),
        "The limit is inclusive: SQL of exactly the maximum size must be accepted.");

    assertRejected(
        createOf(
            createRequestWith(
                sparkRepresentationWithSql(ViewModelConstants.sqlOneByteOverMaxUtf8Size()))),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        String.format(
            "representations[0].sql : exceeds maximum UTF-8 size of %d bytes", MAX_VIEW_SQL_BYTES));
  }

  /**
   * The reason the size rules are hand-written instead of declared with {@code @Size}: a bean
   * constraint counts UTF-16 characters, so this payload — under the limit in characters, and at
   * roughly twice the limit in bytes — would have been accepted.
   */
  @Test
  public void validateCountsSqlInUtf8BytesNotCharacters() {
    String multiByteSql = ViewModelConstants.multiByteSql(MAX_VIEW_SQL_BYTES - 1);

    Assertions.assertTrue(
        multiByteSql.length() <= MAX_VIEW_SQL_BYTES,
        "Precondition: the fixture must be within the limit when counted as characters.");

    assertRejected(
        createOf(createRequestWith(sparkRepresentationWithSql(multiByteSql))),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        String.format(
            "representations[0].sql : exceeds maximum UTF-8 size of %d bytes", MAX_VIEW_SQL_BYTES));
  }

  /**
   * The redaction invariant. Exception messages are copied verbatim into the error response body
   * and into service audit events, so a rejection must never carry back the payload that caused it.
   */
  @Test
  public void rejectionMessagesNeverEchoSqlSchemaOrVersionToken() {
    String secretSql =
        "SELECT secret_column FROM secret_database.secret_table "
            + ViewModelConstants.multiByteSql(MAX_VIEW_SQL_BYTES);
    String secretToken = "file:/secret/metadata/00000-secret.metadata.json";

    CreateUpdateViewRequestBody requestBody =
        createRequestWith(sparkRepresentationWithSql(secretSql))
            .toBuilder()
            .schema(ViewModelConstants.SPARK_STRUCT_TYPE_SCHEMA_LITERAL)
            .baseViewVersion(secretToken)
            .build();

    ViewRequestValidationFailureException exception =
        Assertions.assertThrows(ViewRequestValidationFailureException.class, updateOf(requestBody));

    Assertions.assertTrue(
        exception.getMessage().contains("exceeds maximum UTF-8 size"),
        "Precondition: this request must actually be rejected for its oversized SQL.");
    Assertions.assertFalse(
        exception.getMessage().contains("secret_column"), "SQL text must not reach the message.");
    Assertions.assertFalse(
        exception.getMessage().contains("nullable"), "Schema text must not reach the message.");
    Assertions.assertFalse(
        exception.getMessage().contains(secretToken),
        "The base version token must not reach the message.");
  }

  @Test
  public void validateRejectsRepresentationCountNullElementAndType() {
    // A second representation necessarily also breaks a dialect rule, because spark is the only
    // supported dialect and duplicates of it are rejected. The count message is what is asserted;
    // the dialect code simply reflects that more specific coexisting failure.
    assertRejected(
        createOf(
            createRequestWith(
                Arrays.asList(
                    ViewModelConstants.SPARK_REPRESENTATION,
                    ViewModelConstants.SPARK_REPRESENTATION.toBuilder().dialect("trino").build()))),
        ViewErrorCode.UNSUPPORTED_VIEW_DIALECT,
        "representations : must contain exactly one representation");

    assertRejected(
        createOf(createRequestWith(Collections.singletonList((ViewRepresentation) null))),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "representations[0] : cannot be null");

    assertRejected(
        createOf(
            createRequestWith(
                ViewModelConstants.SPARK_REPRESENTATION.toBuilder().type("SQL").build())),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "representations[0].type : must be 'sql'");
  }

  @Test
  public void validateRejectsEveryDialectOtherThanExactlySpark() {
    assertRejected(
        createOf(
            createRequestWith(
                ViewModelConstants.SPARK_REPRESENTATION.toBuilder().dialect("trino").build())),
        ViewErrorCode.UNSUPPORTED_VIEW_DIALECT,
        "representations[0].dialect : only 'spark' is supported");

    assertRejected(
        createOf(
            createRequestWith(
                ViewModelConstants.SPARK_REPRESENTATION.toBuilder().dialect("SPARK").build())),
        ViewErrorCode.UNSUPPORTED_VIEW_DIALECT,
        "representations[0].dialect : only 'spark' is supported");

    assertRejected(
        createOf(validCreateRequest().toBuilder().sourceDialect("trino").build()),
        ViewErrorCode.UNSUPPORTED_VIEW_DIALECT,
        "sourceDialect : only 'spark' is supported");
  }

  /**
   * A source dialect naming no supplied representation leaves the view with no definition to read,
   * which is a different failure from an unsupported dialect and gets its own message.
   */
  @Test
  public void validateRejectsASourceDialectThatNamesNoSuppliedRepresentation() {
    assertRejected(
        createOf(
            createRequestWith(
                ViewModelConstants.SPARK_REPRESENTATION.toBuilder().dialect(null).build())),
        ViewErrorCode.UNSUPPORTED_VIEW_DIALECT,
        "sourceDialect : does not name a supplied representation");
  }

  /** Two representations claiming the same engine are ambiguous, whatever their casing. */
  @Test
  public void validateRejectsDuplicateDialectsCaseInsensitively() {
    assertRejected(
        createOf(
            createRequestWith(
                Arrays.asList(
                    ViewModelConstants.SPARK_REPRESENTATION,
                    ViewModelConstants.SPARK_REPRESENTATION.toBuilder().dialect("SPARK").build()))),
        ViewErrorCode.UNSUPPORTED_VIEW_DIALECT,
        "representations : dialects must be unique, duplicated: spark");
  }

  @Test
  public void validateRejectsBlankAndOverLongDefaultCatalog() {
    assertRejected(
        createOf(validCreateRequest().toBuilder().defaultCatalog("   ").build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "defaultCatalog : cannot be blank when provided");

    assertRejected(
        createOf(
            validCreateRequest()
                .toBuilder()
                .defaultCatalog(
                    String.join("", Collections.nCopies(MAX_VIEW_IDENTIFIER_LENGTH + 1, "c")))
                .build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        String.format(
            "defaultCatalog : exceeds the maximum length of %d characters",
            MAX_VIEW_IDENTIFIER_LENGTH));

    assertDoesNotThrow(
        createOf(validCreateRequest().toBuilder().defaultCatalog(null).build()),
        "The catalog is optional: omitting it entirely stays legal.");
  }

  @Test
  public void validateRejectsEmptyAndMalformedDefaultNamespaceSegments() {
    assertRejected(
        createOf(
            validCreateRequest()
                .toBuilder()
                .defaultNamespace(Collections.<String>emptyList())
                .build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "defaultNamespace : cannot be empty when provided");

    assertRejected(
        createOf(
            validCreateRequest()
                .toBuilder()
                .defaultNamespace(Collections.singletonList((String) null))
                .build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "defaultNamespace[0] : cannot be blank");

    assertRejected(
        createOf(
            validCreateRequest()
                .toBuilder()
                .defaultNamespace(Arrays.asList(ViewModelConstants.DATABASE_ID, "  "))
                .build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "defaultNamespace[1] : cannot be blank");

    assertRejected(
        createOf(
            validCreateRequest()
                .toBuilder()
                .defaultNamespace(Collections.singletonList("not a namespace!"))
                .build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "defaultNamespace[0] : Only alphanumerics and underscore supported");

    assertRejected(
        createOf(
            validCreateRequest()
                .toBuilder()
                .defaultNamespace(
                    Collections.singletonList(
                        String.join("", Collections.nCopies(MAX_VIEW_IDENTIFIER_LENGTH + 1, "n"))))
                .build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        String.format(
            "defaultNamespace[0] : exceeds the maximum length of %d characters",
            MAX_VIEW_IDENTIFIER_LENGTH));
  }

  @Test
  public void validateRejectsBlankPropertyKeysAndNullPropertyValues() {
    Map<String, String> blankKey = new LinkedHashMap<>();
    blankKey.put("  ", "value");
    assertRejected(
        createOf(validCreateRequest().toBuilder().viewProperties(blankKey).build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "viewProperties : property keys cannot be blank");

    Map<String, String> nullValues = new LinkedHashMap<>();
    nullValues.put("owner", null);
    nullValues.put("team", null);
    assertRejected(
        createOf(validCreateRequest().toBuilder().viewProperties(nullValues).build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "viewProperties : property values cannot be null, keys: owner, team");
  }

  /**
   * Reserved-key detection reuses the internal catalog's case-sensitive {@code openhouse.}
   * predicate verbatim. The final assertion is the point of the test: because that predicate is
   * case-sensitive and {@code policies} is matched exactly, a user property such as {@code
   * OpenHouse.myTeam} is <b>not</b> reserved and must keep working.
   */
  @Test
  public void validateRejectsReservedPropertyKeysCaseSensitively() {
    Map<String, String> openhousePrefixed = new LinkedHashMap<>();
    openhousePrefixed.put("openhouse.tableId", "hijacked");
    assertRejected(
        createOf(validCreateRequest().toBuilder().viewProperties(openhousePrefixed).build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "viewProperties : reserved keys are not allowed: openhouse.tableId");

    Map<String, String> policies = new LinkedHashMap<>();
    policies.put("policies", "hijacked");
    assertRejected(
        createOf(validCreateRequest().toBuilder().viewProperties(policies).build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "viewProperties : reserved keys are not allowed: policies");

    Map<String, String> userOwned = new LinkedHashMap<>();
    userOwned.put("OpenHouse.myTeam", "grid");
    userOwned.put("policies_owner", "grid");
    assertDoesNotThrow(
        createOf(validCreateRequest().toBuilder().viewProperties(userOwned).build()),
        "Neither of these user-owned keys is reserved: the openhouse. predicate is case-sensitive"
            + " and policies is matched exactly.");
  }

  @Test
  public void validateRejectsIllegalBaseVersionTokensPerVerb() {
    assertRejected(
        createOf(validCreateRequest().toBuilder().baseViewVersion("some-other-token").build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "baseViewVersion : must be omitted or " + INITIAL_TABLE_VERSION + " on POST create");

    assertRejected(
        updateOf(validUpdateRequest().toBuilder().baseViewVersion(null).build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "baseViewVersion : is required and cannot be blank on PUT");

    assertRejected(
        updateOf(validUpdateRequest().toBuilder().baseViewVersion("   ").build()),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "baseViewVersion : is required and cannot be blank on PUT");
  }

  @Test
  public void validateGetAllViewsRejectsInvalidPagingAndCompositeSort() {
    assertRejected(
        () -> viewsApiValidator.validateGetAllViews(ViewModelConstants.DATABASE_ID, -1, 50, null),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "page : provided -1, cannot be negative");

    assertRejected(
        () -> viewsApiValidator.validateGetAllViews(ViewModelConstants.DATABASE_ID, 0, 0, null),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "size : provided 0, must be greater than 0");

    assertRejected(
        () ->
            viewsApiValidator.validateGetAllViews(
                ViewModelConstants.DATABASE_ID, 0, 50, "viewId,databaseId"),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "sortBy : provided viewId,databaseId, does not support multiple sort fields or directions");
  }

  /** Failures accumulate, so a client sees every structural problem in one response. */
  @Test
  public void validateReportsEveryFailureRatherThanStoppingAtTheFirst() {
    CreateUpdateViewRequestBody requestBody =
        validCreateRequest()
            .toBuilder()
            .databaseId("another_database")
            .defaultCatalog("   ")
            .baseViewVersion("not-the-initial-token")
            .build();

    ViewRequestValidationFailureException exception =
        Assertions.assertThrows(ViewRequestValidationFailureException.class, createOf(requestBody));

    Assertions.assertTrue(exception.getMessage().contains("databaseId : provided"));
    Assertions.assertTrue(
        exception.getMessage().contains("defaultCatalog : cannot be blank when provided"));
    Assertions.assertTrue(exception.getMessage().contains("baseViewVersion : must be omitted"));
    Assertions.assertTrue(
        exception.getMessage().contains("; "),
        "Reasons are joined with \"; \", matching how the table API reports multiple failures.");
  }

  /**
   * When a request breaks several rules at once the thrown code is the most specific one: schema
   * beats dialect, and dialect beats the generic definition code. All three map to 400, so this
   * ordering is invisible on the wire and only assertable here.
   */
  @Test
  public void errorCodePrecedenceIsSchemaThenDialectThenGeneric() {
    CreateUpdateViewRequestBody allThree =
        createRequestWith(
                ViewModelConstants.SPARK_REPRESENTATION.toBuilder().dialect("trino").build())
            .toBuilder()
            .schema(ViewModelConstants.MALFORMED_SCHEMA_LITERAL)
            .defaultCatalog("   ")
            .build();
    assertRejected(createOf(allThree), ViewErrorCode.UNSUPPORTED_VIEW_SCHEMA, SCHEMA_PARSE_MESSAGE);

    CreateUpdateViewRequestBody dialectAndGeneric =
        allThree.toBuilder().schema(ViewModelConstants.VIEW_SCHEMA_LITERAL).build();
    assertRejected(
        createOf(dialectAndGeneric),
        ViewErrorCode.UNSUPPORTED_VIEW_DIALECT,
        "defaultCatalog : cannot be blank when provided");

    CreateUpdateViewRequestBody genericOnly =
        validCreateRequest().toBuilder().defaultCatalog("   ").build();
    assertRejected(
        createOf(genericOnly),
        ViewErrorCode.INVALID_VIEW_DEFINITION,
        "defaultCatalog : cannot be blank when provided");
  }
}
