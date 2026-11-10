package com.linkedin.openhouse.tables.mock.audit;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.linkedin.openhouse.common.audit.ServiceAuditPayloadRedactor;
import com.linkedin.openhouse.tables.audit.ViewRequestPayloadRedactor;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletMapping;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.MappingMatch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.web.MockHttpServletMapping;
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

  /**
   * Raw request paths and whether Spring MVC routes each to the view collection or item templates
   * ({@code /v1/databases/{databaseId}/views[/{viewId}]}) under the service's default matching:
   * single per-segment decoding, matrix parameters ignored, case-sensitive literals, and an
   * optional trailing separator. Arguments: context path, raw request URI, is a view route.
   */
  static Stream<Arguments> routes() {
    return Stream.of(
        // View collection.
        Arguments.of("", "/v1/databases/db/views", true),
        Arguments.of("", "/v1/databases/db/views/", true),
        Arguments.of("", "/v1/databases/db/%76iews", true),
        Arguments.of("", "/v1/databases/db/%76iews/", true),
        Arguments.of("", "/v1/databases/db/views;mode=x", true),
        Arguments.of("", "/v1/d%61tabases/db/views", true),
        Arguments.of("/ctx", "/ctx/v1/databases/db/views", true),
        Arguments.of("/ctx", "/ctx/v1/databases/db/%76iews/", true),
        // View item.
        Arguments.of("", "/v1/databases/db/views/v", true),
        Arguments.of("", "/v1/databases/db/views/v/", true),
        Arguments.of("", "/v1/databases/db/%76iews/v", true),
        Arguments.of("", "/v1/databases/db/%76iews/v/", true),
        Arguments.of("", "/v1/databases/db/views/v;mode=x", true),
        Arguments.of("/ctx", "/ctx/v1/databases/db/%76iews/v", true),
        // Not a view route: a table create carries a schema too.
        Arguments.of("", "/v1/databases/db/tables", false),
        Arguments.of("", "/v1/databases/db/tables/t", false),
        Arguments.of("", "/v1/databases/db/%74ables", false),
        Arguments.of("", "/v1/databases/db/%74ables/t", false),
        Arguments.of("", "/v1/databases", false),
        Arguments.of("", "/v2/databases/db/views", false),
        Arguments.of("", "/other/v1/databases/db/views", false),
        Arguments.of("", "/v1/databases/db/VIEWS", false),
        Arguments.of("", "/v1/databases/db/viewsExtra", false),
        Arguments.of("", "/v1/databases/views", false),
        Arguments.of("", "/v1/databases/db/views/v/extra", false),
        Arguments.of("", "/v1/databases/db/%2576iews", false),
        Arguments.of("", "/v1/databases/db%2Fviews", false),
        Arguments.of("", "/v1/databases/db/views%2Fv", false),
        Arguments.of("", "/v1/databases/db/views//", false),
        Arguments.of("/ctx", "/ctx/v1/databases/db/tables", false));
  }

  /**
   * The payload redactor claims exactly the request paths MVC routes to the view collection or
   * item, whether MVC has already parsed the request path or not. Classification leaves the request
   * untouched: the raw URI and attributes are unchanged, and a standalone request gains no parsed
   * path.
   */
  @ParameterizedTest
  @MethodSource("routes")
  public void claimsExactlyTheMvcViewRoutes(
      String contextPath, String rawRequestUri, boolean viewRoute) {
    for (boolean cached : new boolean[] {false, true}) {
      MockHttpServletRequest request = new MockHttpServletRequest("GET", rawRequestUri);
      request.setContextPath(contextPath);
      if (cached) {
        ServletRequestPathUtils.parseAndCache(request);
      }
      Set<String> attributesBefore = attributeNames(request);

      Assertions.assertEquals(
          viewRoute, redactor.appliesTo(request), rawRequestUri + " cached=" + cached);

      Assertions.assertEquals(rawRequestUri, request.getRequestURI());
      Assertions.assertEquals(attributesBefore, attributeNames(request));
      Assertions.assertEquals(cached, ServletRequestPathUtils.hasParsedRequestPath(request));
    }
  }

  /**
   * A view route reached through MVC's parsed servlet mapping is classified by MVC's path: under a
   * DispatcherServlet mapped at {@code /api/*}, {@code /api/v1/databases/db/views} is a view route.
   */
  @Test
  public void claimsAViewRouteWithinMvcsParsedServletMapping() {
    MockHttpServletRequest base = new MockHttpServletRequest("GET", "/api/v1/databases/db/views");
    base.setServletPath("/api");
    base.setPathInfo("/v1/databases/db/views");
    HttpServletRequest request =
        new HttpServletRequestWrapper(base) {
          @Override
          public HttpServletMapping getHttpServletMapping() {
            return new MockHttpServletMapping(
                "/v1/databases/db/views", "/api/*", "dispatcher", MappingMatch.PATH);
          }
        };
    ServletRequestPathUtils.parseAndCache(request);

    Assertions.assertTrue(redactor.appliesTo(request));
  }

  private static Set<String> attributeNames(HttpServletRequest request) {
    return new HashSet<>(Collections.list(request.getAttributeNames()));
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
                + " \"sourceDialect\": \"spark\"}");
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
