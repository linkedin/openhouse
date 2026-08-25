package com.linkedin.openhouse.tables.api.spec;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

/**
 * Freezes the M1 wire surface of {@code /v2/databases/{databaseId}/views}.
 *
 * <p>This test exists to satisfy the BDP-108397 acceptance criterion: "A contract test pins the M1
 * wire surface, so adding the admission service, the polymorphic lookup or /versions later changes
 * no field this ships." Every assertion is an exact set equality, so the test fails when a field is
 * added as well as when one is removed.
 *
 * <p>It deliberately runs as a plain JUnit 5 test with reflection and a bare Jackson {@link
 * ObjectMapper}: no Spring context is loaded, so the contract stays pinned even if application
 * wiring changes.
 */
public class ViewApiContractTest {

  /**
   * Bare mapper with default configuration. Assertions run on the Jackson path because Jackson is
   * what actually serializes responses over the wire; Gson {@code toJson()} on these models is a
   * convenience helper only.
   *
   * <p>TODO: this is a bare mapper, not the Spring MVC message converter. {@code
   * TablesMvcConfigurer} customizes no converters today, so the two are equivalent, but a
   * MockMvc-path serialization assertion belongs in the views controller-test slice to keep that
   * equivalence honest.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testCreateUpdateViewRequestBodyFieldsAreFrozen() {
    Set<String> expected =
        setOf(
            "viewId",
            "databaseId",
            "clusterId",
            "schema",
            "representations",
            "sourceDialect",
            "defaultCatalog",
            "defaultNamespace",
            "viewProperties",
            "baseViewVersion");

    Assertions.assertEquals(
        expected,
        contractFieldNames(CreateUpdateViewRequestBody.class),
        "CreateUpdateViewRequestBody is a frozen M1 request contract; adding or removing a field is"
            + " a wire-visible change that needs an explicit contract review.");

    Assertions.assertEquals(
        expected,
        jacksonPropertyNames(CreateUpdateViewRequestBody.class),
        "The Jackson-visible property set is the true wire surface. It must not drift from the"
            + " declared fields, which a computed or inherited getter would silently do.");
  }

  @Test
  public void testViewRepresentationFieldsAreFrozen() {
    Set<String> expected = setOf("type", "sql", "dialect");

    Assertions.assertEquals(
        expected,
        contractFieldNames(ViewRepresentation.class),
        "ViewRepresentation is a frozen M1 request component.");

    Assertions.assertEquals(
        expected,
        jacksonPropertyNames(ViewRepresentation.class),
        "The Jackson-visible property set is the true wire surface for the nested component.");
  }

  @Test
  public void testGetViewResponseBodyFieldsAreFrozen() {
    Set<String> expected =
        setOf(
            "viewId",
            "databaseId",
            "clusterId",
            "viewUri",
            "metadataLocation",
            "viewVersion",
            "creationTime");

    Assertions.assertEquals(
        expected,
        contractFieldNames(GetViewResponseBody.class),
        "GetViewResponseBody is pointer-only. SQL, schema, representations, history, UUID,"
            + " properties and resolution context must stay in the metadata file.");

    Assertions.assertEquals(
        expected,
        jacksonPropertyNames(GetViewResponseBody.class),
        "A getter-only property would leak onto the wire without adding a declared field, so the"
            + " Jackson property set is pinned as well.");
  }

  @Test
  public void testGetAllViewsResponseBodyFieldsAreFrozen() {
    Assertions.assertEquals(
        setOf("pageResults"),
        contractFieldNames(GetAllViewsResponseBody.class),
        "GetAllViewsResponseBody is paginated from the first release; there is deliberately no"
            + " unpaginated legacy 'results' field.");
  }

  @Test
  public void testFullyPopulatedRequestSerializesExactKeys() {
    JsonNode json = MAPPER.valueToTree(ViewModelConstants.fullyPopulatedRequest());

    Assertions.assertEquals(
        setOf(
            "viewId",
            "databaseId",
            "clusterId",
            "schema",
            "representations",
            "sourceDialect",
            "defaultCatalog",
            "defaultNamespace",
            "viewProperties",
            "baseViewVersion"),
        keysOf(json));

    Assertions.assertEquals(ViewModelConstants.VIEW_ID, json.get("viewId").asText());
    Assertions.assertEquals(ViewModelConstants.DATABASE_ID, json.get("databaseId").asText());
    Assertions.assertEquals(ViewModelConstants.CLUSTER_ID, json.get("clusterId").asText());
    Assertions.assertEquals(ViewModelConstants.SOURCE_DIALECT, json.get("sourceDialect").asText());
    Assertions.assertEquals(
        ViewModelConstants.METADATA_LOCATION, json.get("baseViewVersion").asText());

    Assertions.assertTrue(json.get("representations").isArray());
    Assertions.assertEquals(1, json.get("representations").size());
    JsonNode representation = json.get("representations").get(0);
    Assertions.assertEquals(setOf("type", "sql", "dialect"), keysOf(representation));
    Assertions.assertEquals(
        ViewModelConstants.SQL_REPRESENTATION_TYPE, representation.get("type").asText());
    Assertions.assertEquals(ViewModelConstants.VIEW_SQL, representation.get("sql").asText());
    Assertions.assertEquals(
        ViewModelConstants.SOURCE_DIALECT, representation.get("dialect").asText());

    Assertions.assertTrue(json.get("defaultNamespace").isArray());
    Assertions.assertEquals(
        ViewModelConstants.DATABASE_ID, json.get("defaultNamespace").get(0).asText());
    Assertions.assertEquals(
        setOf("owner"), keysOf(json.get("viewProperties")), "viewProperties is a free-form map");
  }

  @Test
  public void testCreateRequestOmitsNullBaseViewVersion() {
    CreateUpdateViewRequestBody request = ViewModelConstants.createRequestWithoutBaseVersion();
    Assertions.assertNull(request.getBaseViewVersion());

    JsonNode json = MAPPER.valueToTree(request);

    Assertions.assertFalse(
        json.has("baseViewVersion"),
        "An omitted baseViewVersion must be absent from the payload, not present as JSON null,"
            + " so the server can distinguish 'not supplied' on create.");
    Assertions.assertEquals(
        setOf(
            "viewId",
            "databaseId",
            "clusterId",
            "schema",
            "representations",
            "sourceDialect",
            "defaultCatalog",
            "defaultNamespace",
            "viewProperties"),
        keysOf(json));

    // The Gson helper on the model is configured to agree with @JsonInclude(NON_NULL): unlike
    // CreateUpdateTableRequestBody, it does not call serializeNulls().
    Assertions.assertFalse(
        request.toJson().contains("baseViewVersion"),
        "toJson() must not disagree with the Jackson wire representation.");
  }

  @Test
  public void testCreateRequestSerializesInitialBaseViewVersion() {
    CreateUpdateViewRequestBody request = ViewModelConstants.createRequestWithInitialBaseVersion();

    JsonNode json = MAPPER.valueToTree(request);

    Assertions.assertTrue(json.has("baseViewVersion"));
    Assertions.assertEquals("INITIAL_VERSION", json.get("baseViewVersion").asText());
    Assertions.assertEquals(
        INITIAL_TABLE_VERSION,
        json.get("baseViewVersion").asText(),
        "The create token reuses the existing INITIAL_VERSION literal rather than minting a"
            + " view-specific value.");
  }

  @Test
  public void testPointerResponseSerializesExactKeysAndNoDefinition() {
    // Uses distinct metadataLocation/viewVersion sentinels so a swap of the two Jackson property
    // associations cannot pass. Production keeps them equal; see ViewModelConstants.
    JsonNode json = MAPPER.valueToTree(ViewModelConstants.pointerResponseWithDistinctPointers());

    Assertions.assertEquals(
        setOf(
            "viewId",
            "databaseId",
            "clusterId",
            "viewUri",
            "metadataLocation",
            "viewVersion",
            "creationTime"),
        keysOf(json));

    List<String> definitionFields =
        Arrays.asList(
            "sql",
            "schema",
            "representations",
            "sourceDialect",
            "defaultCatalog",
            "defaultNamespace",
            "viewProperties",
            "viewUUID",
            "history",
            "versions",
            "properties",
            "tableType");
    for (String forbidden : definitionFields) {
      Assertions.assertFalse(
          json.has(forbidden), "Pointer response leaked definition field '" + forbidden + "'.");
    }

    Assertions.assertEquals(ViewModelConstants.VIEW_URI, json.get("viewUri").asText());
    Assertions.assertEquals(
        ViewModelConstants.DISTINCT_METADATA_LOCATION, json.get("metadataLocation").asText());
    Assertions.assertEquals(
        ViewModelConstants.DISTINCT_VIEW_VERSION, json.get("viewVersion").asText());
    Assertions.assertNotEquals(
        json.get("metadataLocation").asText(),
        json.get("viewVersion").asText(),
        "The fixture must keep the two pointers distinct, otherwise this test cannot detect a"
            + " swapped property association.");
    Assertions.assertTrue(json.get("creationTime").isNumber());
    Assertions.assertEquals(ViewModelConstants.CREATION_TIME, json.get("creationTime").asLong());
  }

  @Test
  public void testSparseListResponseUsesGetViewResponseBodyElementsAndPageMetadata() {
    GetAllViewsResponseBody listResponse = ViewModelConstants.listResponse();

    Assertions.assertTrue(
        listResponse.getPageResults().getContent().stream()
            .allMatch(element -> element instanceof GetViewResponseBody),
        "List elements are the full response type populated sparsely, not a separate identifier"
            + " response type.");

    JsonNode json = MAPPER.valueToTree(listResponse);
    Assertions.assertEquals(setOf("pageResults"), keysOf(json));

    JsonNode page = json.get("pageResults");

    // Exact key-set equality, not has(): GetAllTablesResponseBody already ships a Spring Data Page
    // on the wire today, so this documents the real shipped shape. A Spring Data upgrade that adds,
    // removes or renames a page-level key is a client-visible wire change and must be reviewed
    // here rather than silently absorbed.
    Assertions.assertEquals(
        setOf(
            "content",
            "pageable",
            "totalPages",
            "totalElements",
            "last",
            "sort",
            "number",
            "size",
            "numberOfElements",
            "first",
            "empty"),
        keysOf(page),
        "The serialized Page shape is part of the view list contract.");

    Assertions.assertEquals(1, page.get("totalPages").asInt());
    Assertions.assertEquals(2L, page.get("totalElements").asLong());
    Assertions.assertEquals(2, page.get("numberOfElements").asInt());
    Assertions.assertEquals(0, page.get("number").asInt());
    Assertions.assertEquals(50, page.get("size").asInt());
    Assertions.assertTrue(page.get("first").asBoolean());
    Assertions.assertTrue(page.get("last").asBoolean());
    Assertions.assertFalse(page.get("empty").asBoolean());

    Assertions.assertEquals(
        setOf("empty", "sorted", "unsorted"),
        keysOf(page.get("sort")),
        "The nested sort descriptor is client-visible too.");
    Assertions.assertTrue(page.get("sort").get("empty").asBoolean());
    Assertions.assertFalse(page.get("sort").get("sorted").asBoolean());
    Assertions.assertTrue(page.get("sort").get("unsorted").asBoolean());

    JsonNode pageable = page.get("pageable");
    Assertions.assertEquals(
        setOf("sort", "offset", "pageNumber", "pageSize", "paged", "unpaged"),
        keysOf(pageable),
        "The nested pageable descriptor is client-visible too.");
    Assertions.assertEquals(0L, pageable.get("offset").asLong());
    Assertions.assertEquals(0, pageable.get("pageNumber").asInt());
    Assertions.assertEquals(50, pageable.get("pageSize").asInt());
    Assertions.assertTrue(pageable.get("paged").asBoolean());
    Assertions.assertFalse(pageable.get("unpaged").asBoolean());
    Assertions.assertEquals(setOf("empty", "sorted", "unsorted"), keysOf(pageable.get("sort")));

    JsonNode content = page.get("content");
    Assertions.assertEquals(2, content.size());
    for (JsonNode element : content) {
      Assertions.assertEquals(
          setOf(
              "viewId",
              "databaseId",
              "clusterId",
              "viewUri",
              "metadataLocation",
              "viewVersion",
              "creationTime"),
          keysOf(element),
          "List elements must expose exactly the pointer contract.");
      Assertions.assertFalse(element.get("viewId").isNull());
      Assertions.assertEquals(ViewModelConstants.DATABASE_ID, element.get("databaseId").asText());
      List<String> unpopulatedPointerFields =
          Arrays.asList("clusterId", "viewUri", "metadataLocation", "viewVersion");
      for (String unpopulated : unpopulatedPointerFields) {
        Assertions.assertTrue(
            element.get(unpopulated).isNull(),
            "List results are identifier-only, so '" + unpopulated + "' must stay unpopulated.");
      }
      Assertions.assertEquals(0L, element.get("creationTime").asLong());
    }
  }

  @Test
  public void testViewErrorCodeNamesAndStatusesAreFrozen() {
    Map<ViewErrorCode, HttpStatus> expected = new EnumMap<>(ViewErrorCode.class);
    expected.put(ViewErrorCode.NO_SUCH_VIEW, HttpStatus.NOT_FOUND);
    expected.put(ViewErrorCode.VIEW_ALREADY_EXISTS, HttpStatus.CONFLICT);
    expected.put(ViewErrorCode.NAME_ALREADY_EXISTS_AS_TABLE, HttpStatus.CONFLICT);
    expected.put(ViewErrorCode.CONCURRENT_VIEW_MODIFICATION, HttpStatus.CONFLICT);
    expected.put(ViewErrorCode.DATABASE_NOT_FOUND, HttpStatus.NOT_FOUND);
    expected.put(ViewErrorCode.VIEWS_DISABLED, HttpStatus.NOT_FOUND);
    expected.put(ViewErrorCode.INVALID_VIEW_DEFINITION, HttpStatus.BAD_REQUEST);
    expected.put(ViewErrorCode.UNSUPPORTED_VIEW_DIALECT, HttpStatus.BAD_REQUEST);
    expected.put(ViewErrorCode.UNSUPPORTED_VIEW_SCHEMA, HttpStatus.BAD_REQUEST);
    expected.put(ViewErrorCode.VIEW_ADMISSION_FAILED, HttpStatus.UNPROCESSABLE_ENTITY);
    expected.put(ViewErrorCode.REQUIRED_REPRESENTATION_MISSING, HttpStatus.UNPROCESSABLE_ENTITY);
    expected.put(ViewErrorCode.DEPENDENCY_CYCLE, HttpStatus.UNPROCESSABLE_ENTITY);
    expected.put(ViewErrorCode.MAX_VIEW_DEPTH_EXCEEDED, HttpStatus.UNPROCESSABLE_ENTITY);
    expected.put(ViewErrorCode.ADMISSION_SERVICE_UNAVAILABLE, HttpStatus.SERVICE_UNAVAILABLE);

    Assertions.assertEquals(
        14, ViewErrorCode.values().length, "ViewErrorCode ships exactly 14 values.");

    Assertions.assertEquals(
        setOf(
            "NO_SUCH_VIEW",
            "VIEW_ALREADY_EXISTS",
            "NAME_ALREADY_EXISTS_AS_TABLE",
            "CONCURRENT_VIEW_MODIFICATION",
            "DATABASE_NOT_FOUND",
            "VIEWS_DISABLED",
            "INVALID_VIEW_DEFINITION",
            "UNSUPPORTED_VIEW_DIALECT",
            "UNSUPPORTED_VIEW_SCHEMA",
            "VIEW_ADMISSION_FAILED",
            "REQUIRED_REPRESENTATION_MISSING",
            "DEPENDENCY_CYCLE",
            "MAX_VIEW_DEPTH_EXCEEDED",
            "ADMISSION_SERVICE_UNAVAILABLE"),
        Arrays.stream(ViewErrorCode.values())
            .map(Enum::name)
            .collect(Collectors.toCollection(LinkedHashSet::new)),
        "Reserved codes ship now so later milestones add behavior without an enum change.");

    Assertions.assertEquals(expected.size(), ViewErrorCode.values().length);
    for (ViewErrorCode code : ViewErrorCode.values()) {
      Assertions.assertEquals(
          expected.get(code),
          code.getHttpStatus(),
          "ViewErrorCode." + code.name() + " must keep its HTTP status.");
      Assertions.assertEquals(
          expected.get(code).value(),
          code.getHttpStatus().value(),
          "ViewErrorCode." + code.name() + " must keep its numeric HTTP status.");
    }

    // The enum only selects an HTTP status; it is never serialized into the error body.
    Assertions.assertEquals(
        setOf("httpStatus"),
        contractFieldNames(ViewErrorCode.class),
        "ViewErrorCode carries only an HttpStatus. A wire-facing code field would change the"
            + " unchanged error response contract.");
  }

  /**
   * Declared instance fields that form the contract. Static fields (including the enum constants
   * themselves and {@code $VALUES}), synthetic fields, and instrumentation artifacts such as
   * JaCoCo's {@code $jacocoData} or Lombok-generated members are excluded so the assertion stays
   * stable under coverage instrumentation.
   */
  private static Set<String> contractFieldNames(Class<?> type) {
    return Arrays.stream(type.getDeclaredFields())
        .filter(field -> !field.isSynthetic())
        .filter(field -> !Modifier.isStatic(field.getModifiers()))
        .map(Field::getName)
        .filter(name -> !name.contains("$"))
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /**
   * Property names Jackson will actually serialize for the type. Unlike {@link
   * #contractFieldNames}, this sees inherited and getter-only (computed) properties, so it pins the
   * true wire surface rather than the declared source shape.
   */
  private static Set<String> jacksonPropertyNames(Class<?> type) {
    BeanDescription description =
        MAPPER.getSerializationConfig().introspect(MAPPER.getTypeFactory().constructType(type));
    return description.findProperties().stream()
        .map(BeanPropertyDefinition::getName)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static Set<String> keysOf(JsonNode node) {
    Set<String> keys = new LinkedHashSet<>();
    Iterator<String> fieldNames = node.fieldNames();
    while (fieldNames.hasNext()) {
      keys.add(fieldNames.next());
    }
    return keys;
  }

  private static Set<String> setOf(String... values) {
    List<String> asList = Arrays.asList(values);
    Set<String> set = new LinkedHashSet<>(asList);
    Assertions.assertEquals(asList.size(), set.size(), "Duplicate expectation in test fixture.");
    return set;
  }
}
