package com.linkedin.openhouse.tables.api.spec;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationFeature;
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
import java.util.Collections;
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
 * Pins the wire surface of {@code /v1/databases/{databaseId}/views}. Exact field-set assertions
 * detect additions as well as removals.
 *
 * <p>It deliberately runs as a plain JUnit 5 test with reflection and a bare Jackson {@link
 * ObjectMapper}: no Spring context is loaded, so the contract stays pinned even if application
 * wiring changes.
 */
public class ViewApiContractTest {

  /**
   * Default Jackson mapper for wire-shape assertions. Controller tests cover binding through the
   * application's MVC converter separately.
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
            "baseMetadataLocation");

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
    Set<String> expected = setOf("results", "nextPageToken");

    Assertions.assertEquals(
        expected,
        contractFieldNames(GetAllViewsResponseBody.class),
        "The list envelope carries the result array and the optional continuation token only."
            + " Numeric page metadata is not part of the contract.");

    Assertions.assertEquals(
        expected,
        jacksonPropertyNames(GetAllViewsResponseBody.class),
        "The Jackson-visible property set is the true wire surface, so a computed page counter"
            + " would be caught here as well.");
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
            "baseMetadataLocation"),
        keysOf(json));

    Assertions.assertEquals(ViewModelConstants.VIEW_ID, json.get("viewId").asText());
    Assertions.assertEquals(ViewModelConstants.DATABASE_ID, json.get("databaseId").asText());
    Assertions.assertEquals(ViewModelConstants.CLUSTER_ID, json.get("clusterId").asText());
    Assertions.assertEquals(ViewModelConstants.SOURCE_DIALECT, json.get("sourceDialect").asText());
    Assertions.assertEquals(
        ViewModelConstants.METADATA_LOCATION, json.get("baseMetadataLocation").asText());

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
  public void testCreateRequestOmitsNullBaseMetadataLocation() {
    CreateUpdateViewRequestBody request = ViewModelConstants.createRequestWithoutBaseVersion();
    Assertions.assertNull(request.getBaseMetadataLocation());

    JsonNode json = MAPPER.valueToTree(request);

    Assertions.assertFalse(
        json.has("baseMetadataLocation"),
        "An omitted baseMetadataLocation must be absent from the payload, not present as JSON null,"
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
        request.toJson().contains("baseMetadataLocation"),
        "toJson() must not disagree with the Jackson wire representation.");
  }

  @Test
  public void testCreateRequestSerializesInitialBaseMetadataLocation() {
    CreateUpdateViewRequestBody request = ViewModelConstants.createRequestWithInitialBaseVersion();

    JsonNode json = MAPPER.valueToTree(request);

    Assertions.assertTrue(json.has("baseMetadataLocation"));
    Assertions.assertEquals("INITIAL_VERSION", json.get("baseMetadataLocation").asText());
    Assertions.assertEquals(
        INITIAL_TABLE_VERSION,
        json.get("baseMetadataLocation").asText(),
        "The create token reuses the existing INITIAL_VERSION literal rather than minting a"
            + " view-specific value.");
  }

  /**
   * Gson serializes declared fields rather than Jackson properties, so this pins that the Java
   * field itself carries the wire name. A rename applied only as a Jackson annotation would leave
   * the Gson helper — and with it the audited request payload — on the old key.
   */
  @Test
  public void testGsonPayloadCarriesTheSameBaseMetadataLocationKey() {
    CreateUpdateViewRequestBody request = ViewModelConstants.fullyPopulatedRequest();

    JsonNode gsonPayload = Assertions.assertDoesNotThrow(() -> MAPPER.readTree(request.toJson()));

    Assertions.assertTrue(
        gsonPayload.has("baseMetadataLocation"),
        "The Gson helper must emit the renamed field, which it only can once the declared field is"
            + " renamed rather than annotated.");
    Assertions.assertEquals(
        ViewModelConstants.METADATA_LOCATION, gsonPayload.get("baseMetadataLocation").asText());
    Assertions.assertFalse(
        gsonPayload.has("baseViewVersion"),
        "The field is renamed, not aliased, so the pre-rename key must not survive on the wire.");
    Assertions.assertEquals(
        keysOf(MAPPER.valueToTree(request)),
        keysOf(gsonPayload),
        "toJson() must not disagree with the Jackson wire representation.");
  }

  /**
   * Deserialization is the direction a caller actually exercises, and it is the direction the
   * serialization assertions above cannot cover. The bare mapper keeps its default {@code
   * FAIL_ON_UNKNOWN_PROPERTIES}, so a key that does not bind fails loudly here rather than becoming
   * a silent null.
   */
  @Test
  public void testRequestBindsBaseMetadataLocationFromTheWireKey() {
    String payload =
        "{\"viewId\": \""
            + ViewModelConstants.VIEW_ID
            + "\", \"databaseId\": \""
            + ViewModelConstants.DATABASE_ID
            + "\", \"baseMetadataLocation\": \""
            + ViewModelConstants.METADATA_LOCATION
            + "\"}";

    CreateUpdateViewRequestBody request =
        Assertions.assertDoesNotThrow(
            () -> MAPPER.readValue(payload, CreateUpdateViewRequestBody.class),
            "baseMetadataLocation is the key a caller sends, so it must bind onto the request"
                + " model.");

    Assertions.assertEquals(
        ViewModelConstants.VIEW_ID,
        request.getViewId(),
        "Precondition: the model binds from JSON at all.");
    Assertions.assertEquals(
        ViewModelConstants.METADATA_LOCATION, request.getBaseMetadataLocation());
  }

  /**
   * The value is an opaque metadata pointer rather than a number, so the whole populated shape has
   * to survive a round trip unchanged: no normalization, no coercion and no field dropped.
   */
  @Test
  public void testFullyPopulatedRequestRoundTripsThroughJackson() {
    CreateUpdateViewRequestBody request = ViewModelConstants.fullyPopulatedRequest();

    JsonNode json = MAPPER.valueToTree(request);
    Assertions.assertTrue(
        json.has("baseMetadataLocation"),
        "Precondition: the serialized form carries the wire key.");

    CreateUpdateViewRequestBody roundTripped =
        Assertions.assertDoesNotThrow(
            () -> MAPPER.treeToValue(json, CreateUpdateViewRequestBody.class));

    Assertions.assertEquals(
        request, roundTripped, "Serializing and reading back must preserve every field.");
  }

  /**
   * The rename ships without a compatibility alias, so the pre-rename key must leave the property
   * unset rather than silently populating it. Read with unknown properties ignored, which is how
   * the application's converter is configured; the shared bare mapper deliberately keeps its strict
   * default and is left alone.
   */
  @Test
  public void testLegacyBaseViewVersionKeyDoesNotPopulateBaseMetadataLocation() {
    ObjectMapper lenientMapper =
        new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    String payload =
        "{\"viewId\": \""
            + ViewModelConstants.VIEW_ID
            + "\", \"baseViewVersion\": \""
            + ViewModelConstants.METADATA_LOCATION
            + "\"}";

    CreateUpdateViewRequestBody request =
        Assertions.assertDoesNotThrow(
            () -> lenientMapper.readValue(payload, CreateUpdateViewRequestBody.class));

    Assertions.assertEquals(
        ViewModelConstants.VIEW_ID,
        request.getViewId(),
        "Precondition: the rest of a legacy payload still binds; only the renamed key is unknown.");
    Assertions.assertNull(
        request.getBaseMetadataLocation(),
        "The old key is not an alias: a caller that has not migrated leaves the field unset.");
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

  /**
   * The list envelope is a plain array plus an optional continuation token. This fixture is a
   * terminal response, so the token must be absent from the document rather than present as JSON
   * null: a client stops because the key is missing, never because the array looks short.
   */
  @Test
  public void testSparseListResponseSerializesResultsArrayAndOmitsAbsentToken() {
    GetAllViewsResponseBody listResponse = ViewModelConstants.listResponse();

    Assertions.assertNull(
        listResponse.getNextPageToken(),
        "Only null represents exhaustion internally: no empty string, no \"null\" literal and no"
            + " synthetic terminal token.");
    Assertions.assertTrue(
        listResponse.getResults().stream()
            .allMatch(element -> element instanceof GetViewResponseBody),
        "List elements are the full response type populated sparsely, not a separate identifier"
            + " response type.");

    JsonNode json = MAPPER.valueToTree(listResponse);

    Assertions.assertEquals(
        setOf("results"),
        keysOf(json),
        "A terminal list response carries results only: no nextPageToken key, and none of the"
            + " Spring Page metadata (pageResults, pageable, number, size, totalElements, last)"
            + " that numeric pagination published.");
    Assertions.assertFalse(
        json.has("nextPageToken"),
        "Absence is the terminal signal, so the key must not survive as an explicit JSON null.");

    JsonNode results = json.get("results");
    Assertions.assertTrue(results.isArray(), "results is always a JSON array.");
    Assertions.assertEquals(
        2,
        results.size(),
        "The fixture is deliberately non-empty, so the per-element assertions below cannot pass"
            + " vacuously on an empty array.");
    Assertions.assertEquals(
        "my_view", results.get(0).get("viewId").asText(), "Service order is preserved.");
    Assertions.assertEquals("my_other_view", results.get(1).get("viewId").asText());

    for (JsonNode element : results) {
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

    // Gson omits nulls by default, which is what the field-level Jackson omission is chosen to
    // agree with, so both serializers must publish the same envelope. The nested item difference
    // (Gson drops the null pointer keys inside an element) is pre-existing and is not asserted.
    JsonNode gsonPayload = parse(listResponse.toJson());
    Assertions.assertEquals(
        setOf("results"),
        keysOf(gsonPayload),
        "toJson() must not disagree with the Jackson envelope, which it would if the declared"
            + " field still carried page metadata.");
    Assertions.assertEquals(
        2, gsonPayload.get("results").size(), "Both serializers preserve every element.");
    Assertions.assertEquals("my_view", gsonPayload.get("results").get(0).get("viewId").asText());
    Assertions.assertEquals(
        "my_other_view", gsonPayload.get("results").get(1).get("viewId").asText());
  }

  /**
   * The non-terminal shape. The token is the service's own value carried verbatim, and it is the
   * only signal a client may use to decide whether to make another request.
   */
  @Test
  public void testNonTerminalListResponseCarriesTheServiceTokenVerbatim() {
    GetAllViewsResponseBody listResponse = ViewModelConstants.listResponseWithNextPageToken();

    Assertions.assertEquals(ViewModelConstants.NEXT_PAGE_TOKEN, listResponse.getNextPageToken());

    JsonNode json = MAPPER.valueToTree(listResponse);
    Assertions.assertEquals(
        setOf("results", "nextPageToken"),
        keysOf(json),
        "A continuing response adds exactly one key. No count, offset, hasMore or page number"
            + " accompanies it.");
    Assertions.assertEquals(
        ViewModelConstants.NEXT_PAGE_TOKEN,
        json.get("nextPageToken").asText(),
        "The token is opaque: it is serialized as the string the service produced, unencoded and"
            + " untrimmed.");
    Assertions.assertEquals(2, json.get("results").size());

    JsonNode gsonPayload = parse(listResponse.toJson());
    Assertions.assertEquals(setOf("results", "nextPageToken"), keysOf(gsonPayload));
    Assertions.assertEquals(
        ViewModelConstants.NEXT_PAGE_TOKEN, gsonPayload.get("nextPageToken").asText());
  }

  /**
   * An empty page is a legitimate response, and it is not by itself terminal. Both serializers must
   * emit the empty array rather than dropping the key or writing null, so a client can always read
   * {@code results} without a null check.
   */
  @Test
  public void testEmptyResultsStaySerializedAsAnArrayInBothSerializers() {
    GetAllViewsResponseBody emptyNonTerminal =
        GetAllViewsResponseBody.builder()
            .results(Collections.emptyList())
            .nextPageToken(ViewModelConstants.NEXT_PAGE_TOKEN)
            .build();

    JsonNode json = MAPPER.valueToTree(emptyNonTerminal);
    Assertions.assertEquals(setOf("results", "nextPageToken"), keysOf(json));
    Assertions.assertTrue(json.get("results").isArray());
    Assertions.assertEquals(
        0,
        json.get("results").size(),
        "An empty page must serialize as [], never as an omitted key or a JSON null.");
    Assertions.assertEquals(
        ViewModelConstants.NEXT_PAGE_TOKEN,
        json.get("nextPageToken").asText(),
        "Emptiness is not exhaustion: the token still tells the client to continue.");

    JsonNode gsonPayload = parse(emptyNonTerminal.toJson());
    Assertions.assertEquals(setOf("results", "nextPageToken"), keysOf(gsonPayload));
    Assertions.assertTrue(gsonPayload.get("results").isArray());
    Assertions.assertEquals(0, gsonPayload.get("results").size());
  }

  /**
   * {@code results} is required, so a missing list is a construction error rather than a response
   * that serializes as null and forces every client into a null check.
   */
  @Test
  public void testListResponseCannotBeBuiltWithoutResults() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> GetAllViewsResponseBody.builder().build(),
        "Omitting results entirely must fail loudly at construction.");
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            GetAllViewsResponseBody.builder()
                .results(null)
                .nextPageToken(ViewModelConstants.NEXT_PAGE_TOKEN)
                .build(),
        "An explicitly null results list is the same defect and must not build either.");
  }

  /** Reads a Gson payload back through Jackson so both serializers can be compared as trees. */
  private static JsonNode parse(String payload) {
    return Assertions.assertDoesNotThrow(() -> MAPPER.readTree(payload));
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
