package com.linkedin.openhouse.tables.mock.audit;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.linkedin.openhouse.common.audit.ServiceAuditPayloadRedactor;
import com.linkedin.openhouse.tables.audit.ViewRequestPayloadRedactor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.web.MockHttpServletRequest;

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

  @Test
  public void preservesDefinitionAndRedactsOnlyTheCasToken() {
    JsonElement payload =
        JsonParser.parseString(
            "{\"viewId\": \"my_view\", \"databaseId\": \"my_database\","
                + " \"schema\": \"secret schema\","
                + " \"representations\": ["
                + "{\"type\": \"sql\", \"sql\": \"exact sql one\", \"dialect\": \"spark\"},"
                + "{\"type\": \"sql\", \"sql\": \"exact sql two\", \"dialect\": \"trino\"}],"
                + " \"viewProperties\": {\"openhouse.view.sourceDialect\": \"spark\"},"
                + " \"baseMetadataLocation\": \"file:/secret/base-token.metadata.json\"}");
    JsonObject expected = payload.deepCopy().getAsJsonObject();
    expected.addProperty("baseMetadataLocation", ServiceAuditPayloadRedactor.REDACTED_VALUE);

    JsonElement redacted = redactor.redact(payload);

    Assertions.assertEquals(expected, redacted);
    Assertions.assertFalse(redacted.toString().contains("base-token"));
    Assertions.assertTrue(redacted.toString().contains("secret schema"));
    Assertions.assertTrue(redacted.toString().contains("exact sql one"));
    Assertions.assertTrue(redacted.toString().contains("exact sql two"));
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
        ServiceAuditPayloadRedactor.REDACTED_VALUE,
        redacted.get("baseMetadataLocation").getAsString());
    Assertions.assertEquals("exact schema", redacted.get("schema").getAsString());
    Assertions.assertFalse(redacted.toString().contains("base-token"));
  }

  @Test
  public void leavesTheArgumentUntouched() {
    JsonElement payload =
        JsonParser.parseString(
            "{\"schema\": \"secret schema\","
                + " \"representations\": [{\"type\": \"sql\", \"sql\": \"secret sql\"}]}");
    String before = payload.toString();

    redactor.redact(payload);

    Assertions.assertEquals(
        before,
        payload.toString(),
        "Redacting must return a copy so one redactor cannot observe another's rewrite.");
  }

  /**
   * A request whose body is absent, malformed or simply not shaped like a view request still
   * reaches the redactor, because the aspect audits whatever the caller sent.
   */
  @Test
  public void toleratesPayloadsThatCarryNoViewDefinition() {
    Assertions.assertNull(redactor.redact(null));
    Assertions.assertEquals(JsonNull.INSTANCE, redactor.redact(JsonNull.INSTANCE));
    Assertions.assertEquals(
        new JsonPrimitive("not an object"), redactor.redact(new JsonPrimitive("not an object")));
    JsonObject withoutDefinition = new JsonObject();
    withoutDefinition.addProperty("viewId", "my_view");
    Assertions.assertEquals(
        withoutDefinition,
        redactor.redact(withoutDefinition),
        "An object carrying neither field is returned as an equal copy.");
  }

}
