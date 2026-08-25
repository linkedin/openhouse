package com.linkedin.openhouse.tables.api.validator.impl;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.ALPHA_NUM_UNDERSCORE_ERROR_MSG;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.ALPHA_NUM_UNDERSCORE_REGEX;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.MAX_VIEW_IDENTIFIER_LENGTH;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.MAX_VIEW_SCHEMA_BYTES;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.MAX_VIEW_SQL_BYTES;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.SPARK_VIEW_DIALECT;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.SQL_VIEW_REPRESENTATION_TYPE;

import com.linkedin.openhouse.common.api.validator.ApiValidatorUtil;
import com.linkedin.openhouse.common.schema.IcebergSchemaHelper;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation;
import com.linkedin.openhouse.tables.api.validator.ViewsApiValidator;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.exception.ViewRequestValidationFailureException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Structural validation of /v2 views requests.
 *
 * <p><b>Security invariant:</b> no message built here interpolates SQL text, schema text or a
 * {@code baseViewVersion} token. Messages are copied verbatim into the error response body and into
 * service audit events, so every payload-derived failure uses a fixed redacted message.
 *
 * <p>SQL is opaque: nothing here parses, translates or engine-validates a view definition.
 */
@Slf4j
@Component
public class OpenHouseViewsApiValidator implements ViewsApiValidator {

  /**
   * Exact server-owned property key. {@code InternalRepositoryUtils.POLICIES_KEY} carries the same
   * literal but is {@code protected} in another package, so it cannot be referenced from here; the
   * {@code openhouse.} prefix check does reuse its canonical predicate.
   */
  private static final String POLICIES_PROPERTY_KEY = "policies";

  @Autowired private Validator validator;

  @Override
  public void validateGetView(String databaseId, String viewId) {
    ViewValidationFailures failures = new ViewValidationFailures();
    validateDatabaseId(databaseId, failures);
    validateViewId(viewId, failures);
    failures.throwIfPresent();
  }

  @Override
  public void validateGetAllViews(String databaseId, int page, int size, String sortBy) {
    ViewValidationFailures failures = new ViewValidationFailures();
    validateDatabaseId(databaseId, failures);
    ApiValidatorUtil.validatePageable(page, size, sortBy, failures.getMessages());
    failures.throwIfPresent();
  }

  @Override
  public void validateCreateView(
      String clusterId, String databaseId, CreateUpdateViewRequestBody requestBody) {
    ViewValidationFailures failures = new ViewValidationFailures();
    validateBody(clusterId, databaseId, requestBody, failures);
    validateCreateBaseViewVersion(requestBody.getBaseViewVersion(), failures);
    failures.throwIfPresent();
  }

  @Override
  public void validateUpdateView(
      String clusterId, String databaseId, String viewId, CreateUpdateViewRequestBody requestBody) {
    ViewValidationFailures failures = new ViewValidationFailures();
    validateBody(clusterId, databaseId, requestBody, failures);
    if (requestBody.getViewId() != null && !requestBody.getViewId().equals(viewId)) {
      failures.addGeneric(
          String.format(
              "viewId : provided %s, doesn't match with the RequestBody %s",
              viewId, requestBody.getViewId()));
    }
    validateUpdateBaseViewVersion(requestBody.getBaseViewVersion(), failures);
    failures.throwIfPresent();
  }

  @Override
  public void validateDeleteView(String databaseId, String viewId) {
    // Identifier rules are identical to a read, so reuse them.
    validateGetView(databaseId, viewId);
  }

  /** Rules shared by POST and PUT. Verb-specific base-version rules are applied by the caller. */
  @SuppressWarnings("checkstyle:OperatorWrap")
  private void validateBody(
      String clusterId,
      String databaseId,
      CreateUpdateViewRequestBody requestBody,
      ViewValidationFailures failures) {
    for (ConstraintViolation<CreateUpdateViewRequestBody> violation :
        validator.validate(requestBody)) {
      failures.addGeneric(
          String.format("%s : %s", ApiValidatorUtil.getField(violation), violation.getMessage()));
    }
    if (requestBody.getClusterId() != null && !requestBody.getClusterId().equals(clusterId)) {
      failures.addGeneric(
          String.format(
              "clusterId : provided %s, doesn't match with the server cluster %s",
              requestBody.getClusterId(), clusterId));
    }
    if (requestBody.getDatabaseId() != null && !requestBody.getDatabaseId().equals(databaseId)) {
      failures.addGeneric(
          String.format(
              "databaseId : provided %s, doesn't match with the RequestBody %s",
              databaseId, requestBody.getDatabaseId()));
    }
    validateSchema(requestBody.getSchema(), failures);
    validateRepresentations(requestBody.getRepresentations(), failures);
    validateUniqueDialects(requestBody.getRepresentations(), failures);
    validateSourceDialect(
        requestBody.getSourceDialect(), requestBody.getRepresentations(), failures);
    validateDefaultCatalog(requestBody.getDefaultCatalog(), failures);
    validateDefaultNamespace(requestBody.getDefaultNamespace(), failures);
    validateViewProperties(requestBody.getViewProperties(), failures);
  }

  /**
   * Parse the schema with Iceberg. Iceberg already rejects duplicate field ids and malformed JSON,
   * so this only has to wrap the failure; there is deliberately no separate duplicate-id check.
   *
   * <p>The size rule runs first and short-circuits parsing, so an oversized document is never fed
   * to the parser.
   */
  private void validateSchema(String schema, ViewValidationFailures failures) {
    if (StringUtils.isEmpty(schema)) {
      // An absent schema is already reported by bean validation.
      return;
    }
    if (utf8Size(schema) > MAX_VIEW_SCHEMA_BYTES) {
      failures.addSchema(
          String.format("schema : exceeds maximum UTF-8 size of %d bytes", MAX_VIEW_SCHEMA_BYTES));
      return;
    }
    try {
      IcebergSchemaHelper.getSchemaFromSchemaJson(schema);
    } catch (Exception e) {
      // Only the exception type is logged: the parser message can echo the caller's schema text.
      log.warn("Rejected a view request with an unparseable Iceberg schema: {}", e.getClass());
      failures.addSchema(
          "schema : must be valid Iceberg schema JSON; Spark StructType JSON is not supported");
    }
  }

  private void validateRepresentations(
      List<ViewRepresentation> representations, ViewValidationFailures failures) {
    if (representations == null || representations.isEmpty()) {
      // An absent or empty list is already reported by bean validation.
      return;
    }
    if (representations.size() != 1) {
      failures.addGeneric("representations : must contain exactly one representation");
    }
    for (int index = 0; index < representations.size(); index++) {
      ViewRepresentation representation = representations.get(index);
      if (representation == null) {
        failures.addGeneric(String.format("representations[%d] : cannot be null", index));
        continue;
      }
      if (!SQL_VIEW_REPRESENTATION_TYPE.equals(representation.getType())) {
        failures.addGeneric(
            String.format(
                "representations[%d].type : must be '%s'", index, SQL_VIEW_REPRESENTATION_TYPE));
      }
      if (!SPARK_VIEW_DIALECT.equals(representation.getDialect())) {
        failures.addDialect(
            String.format(
                "representations[%d].dialect : only '%s' is supported", index, SPARK_VIEW_DIALECT));
      }
      validateRepresentationSql(index, representation.getSql(), failures);
    }
  }

  /**
   * SQL is opaque, so the only rule is a size ceiling. It is counted in UTF-8 bytes rather than
   * characters, which is what a {@code @Size} bean constraint would have counted.
   */
  private void validateRepresentationSql(int index, String sql, ViewValidationFailures failures) {
    if (StringUtils.isEmpty(sql)) {
      // An absent SQL text is already reported by bean validation.
      return;
    }
    if (utf8Size(sql) > MAX_VIEW_SQL_BYTES) {
      failures.addGeneric(
          String.format(
              "representations[%d].sql : exceeds maximum UTF-8 size of %d bytes",
              index, MAX_VIEW_SQL_BYTES));
    }
  }

  /**
   * Dialects identify a representation, so two representations claiming the same dialect are
   * ambiguous. Compared case-insensitively: {@code SPARK} and {@code spark} name the same engine,
   * and rejecting the pair as duplicates is more useful than reporting only the casing failure.
   */
  private void validateUniqueDialects(
      List<ViewRepresentation> representations, ViewValidationFailures failures) {
    if (representations == null) {
      return;
    }
    Set<String> seen = new HashSet<>();
    Set<String> duplicates = new TreeSet<>();
    for (ViewRepresentation representation : representations) {
      if (representation == null || StringUtils.isEmpty(representation.getDialect())) {
        continue;
      }
      String normalized = representation.getDialect().toLowerCase(Locale.ROOT);
      if (!seen.add(normalized)) {
        duplicates.add(normalized);
      }
    }
    if (!duplicates.isEmpty()) {
      failures.addDialect(
          String.format(
              "representations : dialects must be unique, duplicated: %s",
              String.join(", ", duplicates)));
    }
  }

  private void validateSourceDialect(
      String sourceDialect,
      List<ViewRepresentation> representations,
      ViewValidationFailures failures) {
    if (StringUtils.isEmpty(sourceDialect)) {
      // An absent source dialect is already reported by bean validation.
      return;
    }
    if (!SPARK_VIEW_DIALECT.equals(sourceDialect)) {
      failures.addDialect(
          String.format("sourceDialect : only '%s' is supported", SPARK_VIEW_DIALECT));
      return;
    }
    // Only meaningful when at least one usable representation was supplied. With none, the missing
    // or null representation is already reported, and adding a second message here would both
    // duplicate that diagnosis and promote a malformed body to the more specific dialect code.
    List<ViewRepresentation> suppliedRepresentations =
        representations == null
            ? Collections.emptyList()
            : representations.stream().filter(Objects::nonNull).collect(Collectors.toList());
    if (suppliedRepresentations.isEmpty()) {
      return;
    }
    if (suppliedRepresentations.stream()
        .noneMatch(representation -> sourceDialect.equals(representation.getDialect()))) {
      failures.addDialect("sourceDialect : does not name a supplied representation");
    }
  }

  /**
   * The resolution catalog is optional, but supplying a blank or unbounded one is a client bug
   * rather than an omission, so it is rejected instead of silently ignored.
   */
  private void validateDefaultCatalog(String defaultCatalog, ViewValidationFailures failures) {
    if (defaultCatalog == null) {
      return;
    }
    if (StringUtils.isBlank(defaultCatalog)) {
      failures.addGeneric("defaultCatalog : cannot be blank when provided");
    } else if (defaultCatalog.length() > MAX_VIEW_IDENTIFIER_LENGTH) {
      failures.addGeneric(
          String.format(
              "defaultCatalog : exceeds the maximum length of %d characters",
              MAX_VIEW_IDENTIFIER_LENGTH));
    }
  }

  /**
   * Namespace segments follow the same identifier rules as a database id. Messages are indexed but
   * fixed: the offending segment is never echoed, keeping every payload-derived message redacted.
   */
  private void validateDefaultNamespace(
      List<String> defaultNamespace, ViewValidationFailures failures) {
    if (defaultNamespace == null) {
      return;
    }
    if (defaultNamespace.isEmpty()) {
      failures.addGeneric("defaultNamespace : cannot be empty when provided");
      return;
    }
    for (int index = 0; index < defaultNamespace.size(); index++) {
      String segment = defaultNamespace.get(index);
      if (StringUtils.isBlank(segment)) {
        failures.addGeneric(String.format("defaultNamespace[%d] : cannot be blank", index));
      } else if (!segment.matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
        failures.addGeneric(
            String.format("defaultNamespace[%d] : %s", index, ALPHA_NUM_UNDERSCORE_ERROR_MSG));
      } else if (segment.length() > MAX_VIEW_IDENTIFIER_LENGTH) {
        failures.addGeneric(
            String.format(
                "defaultNamespace[%d] : exceeds the maximum length of %d characters",
                index, MAX_VIEW_IDENTIFIER_LENGTH));
      }
    }
  }

  /**
   * View properties are user-owned, with two exceptions carved out for the server: the {@code
   * openhouse.} namespace, whose canonical case-sensitive predicate is reused from the internal
   * catalog, and the exact key {@code policies}. Case sensitivity is deliberate and inherited: a
   * user property such as {@code OpenHouse.myTeam} stays legal.
   *
   * <p>Property keys are user-authored identifiers rather than payload text, so listing the
   * offending keys is intentional and does not breach the SQL/schema/token redaction invariant.
   */
  private void validateViewProperties(
      Map<String, String> viewProperties, ViewValidationFailures failures) {
    if (viewProperties == null || viewProperties.isEmpty()) {
      return;
    }
    boolean blankKey = false;
    Set<String> nullValueKeys = new TreeSet<>();
    Set<String> reservedKeys = new TreeSet<>();
    for (Map.Entry<String, String> property : viewProperties.entrySet()) {
      String key = property.getKey();
      if (StringUtils.isBlank(key)) {
        blankKey = true;
        continue;
      }
      if (property.getValue() == null) {
        nullValueKeys.add(key);
      }
      if (HouseTableSerdeUtils.IS_OH_PREFIXED.test(key) || POLICIES_PROPERTY_KEY.equals(key)) {
        reservedKeys.add(key);
      }
    }
    if (blankKey) {
      failures.addGeneric("viewProperties : property keys cannot be blank");
    }
    if (!nullValueKeys.isEmpty()) {
      failures.addGeneric(
          String.format(
              "viewProperties : property values cannot be null, keys: %s",
              String.join(", ", nullValueKeys)));
    }
    if (!reservedKeys.isEmpty()) {
      failures.addGeneric(
          String.format(
              "viewProperties : reserved keys are not allowed: %s",
              String.join(", ", reservedKeys)));
    }
  }

  /** Counts UTF-8 bytes, not UTF-16 characters. */
  private static int utf8Size(String value) {
    return value.getBytes(StandardCharsets.UTF_8).length;
  }

  /**
   * POST accepts an omitted base version or the table-style {@code INITIAL_VERSION} token, matching
   * both the Iceberg client, which always sends the initial token on create, and callers that omit
   * the field entirely.
   */
  private void validateCreateBaseViewVersion(
      String baseViewVersion, ViewValidationFailures failures) {
    if (baseViewVersion != null && !INITIAL_TABLE_VERSION.equals(baseViewVersion)) {
      failures.addGeneric(
          "baseViewVersion : must be omitted or " + INITIAL_TABLE_VERSION + " on POST create");
    }
  }

  /**
   * PUT requires a base version but treats it as fully opaque: no path, scheme, suffix or length
   * rule is applied, so the service alone decides whether the token is current.
   */
  private void validateUpdateBaseViewVersion(
      String baseViewVersion, ViewValidationFailures failures) {
    if (StringUtils.isBlank(baseViewVersion)) {
      failures.addGeneric("baseViewVersion : is required and cannot be blank on PUT");
    }
  }

  private void validateDatabaseId(String databaseId, ViewValidationFailures failures) {
    if (StringUtils.isEmpty(databaseId)) {
      failures.addGeneric("databaseId : Cannot be empty");
    } else if (!databaseId.matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
      failures.addGeneric(
          String.format(
              "databaseId : provided %s, %s", databaseId, ALPHA_NUM_UNDERSCORE_ERROR_MSG));
    } else if (databaseId.length() > MAX_VIEW_IDENTIFIER_LENGTH) {
      failures.addGeneric(identifierTooLong("databaseId"));
    }
  }

  private void validateViewId(String viewId, ViewValidationFailures failures) {
    if (StringUtils.isEmpty(viewId)) {
      failures.addGeneric("viewId : Cannot be empty");
    } else if (!viewId.matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
      failures.addGeneric(
          String.format("viewId : provided %s, %s", viewId, ALPHA_NUM_UNDERSCORE_ERROR_MSG));
    } else if (viewId.length() > MAX_VIEW_IDENTIFIER_LENGTH) {
      failures.addGeneric(identifierTooLong("viewId"));
    }
  }

  /**
   * Deliberately omits the offending value. Every other identifier message echoes it, but an
   * over-long identifier is by definition large and the message is copied into the error body and
   * into service audit events.
   */
  private static String identifierTooLong(String field) {
    return String.format(
        "%s : exceeds the maximum length of %d characters", field, MAX_VIEW_IDENTIFIER_LENGTH);
  }

  /**
   * Accumulates failure messages in discovery order while separately remembering whether a schema
   * or dialect rule failed, so the thrown exception can carry the most specific internal code.
   *
   * <p>Precedence is schema, then dialect, then the generic definition code. All three map to 400,
   * so the choice is observable only to internal callers and tests.
   */
  private static final class ViewValidationFailures {
    private final List<String> messages = new ArrayList<>();
    private boolean schemaFailure;
    private boolean dialectFailure;

    private List<String> getMessages() {
      return messages;
    }

    private void addGeneric(String message) {
      messages.add(message);
    }

    private void addSchema(String message) {
      schemaFailure = true;
      messages.add(message);
    }

    private void addDialect(String message) {
      dialectFailure = true;
      messages.add(message);
    }

    private void throwIfPresent() {
      if (messages.isEmpty()) {
        return;
      }
      throw new ViewRequestValidationFailureException(errorCode(), messages);
    }

    private ViewErrorCode errorCode() {
      if (schemaFailure) {
        return ViewErrorCode.UNSUPPORTED_VIEW_SCHEMA;
      }
      if (dialectFailure) {
        return ViewErrorCode.UNSUPPORTED_VIEW_DIALECT;
      }
      return ViewErrorCode.INVALID_VIEW_DEFINITION;
    }
  }
}
