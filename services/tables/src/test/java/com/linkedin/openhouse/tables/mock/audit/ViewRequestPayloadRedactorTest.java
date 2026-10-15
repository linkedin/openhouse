package com.linkedin.openhouse.tables.mock.audit;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.linkedin.openhouse.common.audit.ServiceAuditPayloadRedactor;
import com.linkedin.openhouse.tables.audit.ViewRequestPayloadRedactor;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.util.ServletRequestPathUtils;

/**
 * Unit coverage of {@link ViewRequestPayloadRedactor}. The controller-level proof that the redactor
 * is actually wired into {@link com.linkedin.openhouse.common.audit.ServiceAuditAspect} lives in
 * {@code ViewsControllerTest}; this pins the route scoping and the shape of the rewrite, including
 * the payload shapes a caller can send that are not a well-formed view request.
 */
public class ViewRequestPayloadRedactorTest {

  private final ViewRequestPayloadRedactor redactor = new ViewRequestPayloadRedactor();

  private static MockHttpServletRequest requestFor(String uri) {
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.setRequestURI(uri);
    return request;
  }

  /**
   * The payload redactor claims exactly the request paths MVC routes to the view collection or
   * item, whether MVC has already parsed the request path or not. Classification leaves the request
   * untouched: the raw URI and attributes are unchanged, and a standalone request gains no parsed
   * path.
   */
  @ParameterizedTest
  @MethodSource("com.linkedin.openhouse.tables.mock.audit.ViewAuditRouteCases#routes")
  public void claimsExactlyTheMvcViewRoutes(
      String contextPath, String rawRequestUri, boolean collection, boolean viewRoute) {
    for (boolean cached : new boolean[] {false, true}) {
      MockHttpServletRequest request =
          ViewAuditRouteCases.request(contextPath, rawRequestUri, cached);
      Set<String> attributesBefore = ViewAuditRouteCases.attributeNames(request);

      Assertions.assertEquals(
          viewRoute, redactor.appliesTo(request), rawRequestUri + " cached=" + cached);

      Assertions.assertEquals(rawRequestUri, request.getRequestURI());
      Assertions.assertEquals(attributesBefore, ViewAuditRouteCases.attributeNames(request));
      Assertions.assertEquals(cached, ServletRequestPathUtils.hasParsedRequestPath(request));
    }
  }

  /** A view route reached through MVC's parsed servlet mapping is classified by MVC's path. */
  @Test
  public void claimsAViewRouteWithinMvcsParsedServletMapping() {
    Assertions.assertTrue(
        redactor.appliesTo(ViewAuditRouteCases.servletPrefixedCollectionRequest()));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "/v1/databases/my_database/views",
        "/v1/databases/my_database/views/my_view",
        "/v1/databases/d200/views"
      })
  public void appliesToTheViewRoutes(String uri) {
    Assertions.assertTrue(redactor.appliesTo(requestFor(uri)));
  }

  /**
   * A table create carries a {@code schema} too, so the redactor must match only the declared
   * {@code /v1} view routes and leave other resources and prefixes untouched.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "/v1/databases/my_database/tables",
        "/v1/databases/my_database/tables/my_table",
        "/v1/databases",
        "/v1/databases/my_database/tables/my_table/aclPolicies",
        "/v1/databases/my_database/views/my_view/extra",
        "/v1/databases/views",
        "/v2/databases/my_database/tables/search",
        "/v2/databases/my_database/views",
        "/v2/databases/my_database/views/my_view"
      })
  public void declinesEveryOtherRoute(String uri) {
    Assertions.assertFalse(redactor.appliesTo(requestFor(uri)));
  }

  /**
   * Request audit keeps the supplied view definition: schema, every representation and all other
   * fields stay exactly as sent. Only the write CAS token is replaced, keeping its key.
   */
  @Test
  public void keepsTheSuppliedDefinitionAndReplacesOnlyTheCasToken() {
    JsonElement payload =
        JsonParser.parseString(
            "{\"viewId\": \"my_view\", \"databaseId\": \"my_database\","
                + " \"baseMetadataLocation\": \"file:/secret/base-token.metadata.json\","
                + " \"schema\": \"exact schema\","
                + " \"representations\": ["
                + "{\"type\": \"sql\", \"sql\": \"exact sql one\", \"dialect\": \"spark\"},"
                + "{\"type\": \"sql\", \"sql\": \"exact sql two\", \"dialect\": \"trino\"}],"
                + " \"viewProperties\": {\"openhouse.view.sourceDialect\": \"spark\"}}");
    JsonObject expected = payload.deepCopy().getAsJsonObject();
    expected.addProperty("baseMetadataLocation", ServiceAuditPayloadRedactor.REDACTED_VALUE);

    JsonElement redacted = redactor.redact(payload);

    Assertions.assertEquals(expected, redacted);
    Assertions.assertFalse(redacted.toString().contains("base-token"));
  }

  /** The CAS key is kept and its value replaced whatever JSON shape the caller sent for it. */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "\"file:/secret/base-token.metadata.json\"",
        "null",
        "{\"nested\": \"base-token\"}",
        "[\"base-token\"]",
        "7"
      })
  public void replacesTheCasTokenOfAnyJsonShape(String casValue) {
    JsonElement payload =
        JsonParser.parseString(
            "{\"schema\": \"exact schema\", \"baseMetadataLocation\": " + casValue + "}");

    JsonObject redacted = redactor.redact(payload).getAsJsonObject();

    Assertions.assertEquals(
        new JsonPrimitive(ServiceAuditPayloadRedactor.REDACTED_VALUE),
        redacted.get("baseMetadataLocation"));
    Assertions.assertEquals("exact schema", redacted.get("schema").getAsString());
    Assertions.assertEquals(2, redacted.size());
  }

  /** An absent CAS token is not introduced, and a payload without one is an equal copy. */
  @Test
  public void addsNoCasFieldWhenNoneWasSent() {
    JsonElement payload =
        JsonParser.parseString(
            "{\"schema\": \"exact schema\","
                + " \"representations\": [{\"type\": \"sql\", \"sql\": \"exact sql\"}]}");

    JsonElement redacted = redactor.redact(payload);

    Assertions.assertEquals(payload, redacted);
    Assertions.assertFalse(redacted.getAsJsonObject().has("baseMetadataLocation"));
  }

  @Test
  public void leavesTheArgumentUntouched() {
    JsonElement payload =
        JsonParser.parseString(
            "{\"schema\": \"exact schema\", \"baseMetadataLocation\": \"base-token\","
                + " \"representations\": [{\"type\": \"sql\", \"sql\": \"exact sql\"}]}");
    String before = payload.toString();

    JsonElement redacted = redactor.redact(payload);

    Assertions.assertEquals(
        before,
        payload.toString(),
        "Redacting must return a copy so one redactor cannot observe another's rewrite.");
    Assertions.assertNotSame(payload, redacted);
  }

  /**
   * A request whose body is absent or not an object still reaches the redactor, because the aspect
   * audits whatever the caller sent; it carries no CAS field and is returned unchanged.
   */
  @Test
  public void toleratesPayloadsThatAreNotObjects() {
    Assertions.assertNull(redactor.redact(null));
    Assertions.assertEquals(JsonNull.INSTANCE, redactor.redact(JsonNull.INSTANCE));
    Assertions.assertEquals(
        new JsonPrimitive("not an object"), redactor.redact(new JsonPrimitive("not an object")));
    JsonArray array = JsonParser.parseString("[\"SELECT kept\"]").getAsJsonArray();
    Assertions.assertEquals(array, redactor.redact(array));

    JsonObject withoutDefinition = new JsonObject();
    withoutDefinition.addProperty("viewId", "my_view");
    Assertions.assertEquals(
        withoutDefinition,
        redactor.redact(withoutDefinition),
        "An object carrying no CAS field is returned as an equal copy.");
  }

  /**
   * Request audit is provenance of what the caller sent, so a representations subtree that is not a
   * well-formed array of representation objects (rejected later by validation) is kept exactly as
   * supplied, not reshaped or replaced.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"representations\": {\"sql\": \"SQL_KEPT\"}, \"schema\": \"SCHEMA_KEPT\"}",
        "{\"representations\": \"SQL_KEPT\", \"schema\": \"SCHEMA_KEPT\"}",
        "{\"representations\": [\"SELECT SQL_KEPT\"], \"schema\": \"SCHEMA_KEPT\"}",
        "{\"representations\": [[{\"sql\": \"SELECT SQL_KEPT\"}]], \"schema\": \"SCHEMA_KEPT\"}",
        "{\"representations\": [{\"type\": \"sql\", \"sql\": \"ok\"},"
            + " \"SELECT SQL_KEPT\", null, 3], \"schema\": \"SCHEMA_KEPT\"}",
        "{\"representations\": [\"not an object\", {\"type\": \"sql\"}, null],"
            + " \"schema\": {\"type\": \"struct\"}}"
      })
  public void keepsMalformedDefinitionShapesExactlyAsSupplied(String body) {
    JsonElement payload = JsonParser.parseString(body);

    Assertions.assertEquals(payload, redactor.redact(payload));
  }
}
