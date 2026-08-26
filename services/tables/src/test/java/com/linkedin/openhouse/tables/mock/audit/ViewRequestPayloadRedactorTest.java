package com.linkedin.openhouse.tables.mock.audit;

import com.google.gson.JsonArray;
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
        "/v2/databases/my_database/views",
        "/v2/databases/my_database/views/my_view",
        "/v2/databases/d200/views"
      })
  public void appliesToTheViewRoutes(String uri) {
    Assertions.assertTrue(redactor.appliesTo(requestFor(uri)));
  }

  /**
   * The scoping that protects the existing resources. A table create carries a {@code schema} too,
   * so the redactor must decline every route but views.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "/v1/databases/my_database/tables",
        "/v1/databases/my_database/tables/my_table",
        "/v1/databases",
        "/v1/databases/my_database/tables/my_table/aclPolicies",
        "/v2/databases/my_database/views/my_view/extra",
        "/v2/databases/views",
        "/v2/databases/my_database/tables"
      })
  public void declinesEveryOtherRoute(String uri) {
    Assertions.assertFalse(redactor.appliesTo(requestFor(uri)));
  }

  @Test
  public void redactsSchemaAndEverySqlRepresentation() {
    JsonElement payload =
        JsonParser.parseString(
            "{\"viewId\": \"my_view\", \"databaseId\": \"my_database\","
                + " \"schema\": \"secret schema\","
                + " \"representations\": ["
                + "{\"type\": \"sql\", \"sql\": \"secret sql one\", \"dialect\": \"spark\"},"
                + "{\"type\": \"sql\", \"sql\": \"secret sql two\", \"dialect\": \"trino\"}],"
                + " \"sourceDialect\": \"spark\"}");

    JsonObject redacted = redactor.redact(payload).getAsJsonObject();

    Assertions.assertEquals(
        ServiceAuditPayloadRedactor.REDACTED_VALUE, redacted.get("schema").getAsString());
    JsonArray representations = redacted.getAsJsonArray("representations");
    for (JsonElement representation : representations) {
      Assertions.assertEquals(
          ServiceAuditPayloadRedactor.REDACTED_VALUE,
          representation.getAsJsonObject().get("sql").getAsString(),
          "Every representation is redacted, not only the first.");
    }
    Assertions.assertFalse(redacted.toString().contains("secret"));

    // Identifiers and dialect metadata survive.
    Assertions.assertEquals("my_view", redacted.get("viewId").getAsString());
    Assertions.assertEquals("my_database", redacted.get("databaseId").getAsString());
    Assertions.assertEquals("spark", redacted.get("sourceDialect").getAsString());
    Assertions.assertEquals(
        "trino", representations.get(1).getAsJsonObject().get("dialect").getAsString());
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
    Assertions.assertEquals(new JsonArray(), redactor.redact(new JsonArray()));

    JsonObject withoutDefinition = new JsonObject();
    withoutDefinition.addProperty("viewId", "my_view");
    Assertions.assertEquals(
        withoutDefinition,
        redactor.redact(withoutDefinition),
        "An object carrying neither field is returned as an equal copy.");
  }

  /**
   * {@code representations} is caller-supplied JSON, so it can be any element. Redaction must skip
   * what it cannot interpret rather than fail and lose the whole payload to the aspect's
   * fail-closed handler.
   */
  @Test
  public void skipsRepresentationsThatAreNotSqlObjects() {
    JsonElement payload =
        JsonParser.parseString(
            "{\"schema\": \"secret schema\","
                + " \"representations\": [\"not an object\", {\"type\": \"sql\"}, null]}");

    JsonObject redacted = redactor.redact(payload).getAsJsonObject();

    Assertions.assertEquals(
        ServiceAuditPayloadRedactor.REDACTED_VALUE, redacted.get("schema").getAsString());
    Assertions.assertEquals(3, redacted.getAsJsonArray("representations").size());
  }

  @Test
  public void redactsAnObjectRepresentationsFieldThatIsNotAnArray() {
    JsonElement payload =
        JsonParser.parseString(
            "{\"schema\": \"secret schema\", \"representations\": \"nonsense\"}");

    JsonObject redacted = redactor.redact(payload).getAsJsonObject();

    Assertions.assertEquals(
        ServiceAuditPayloadRedactor.REDACTED_VALUE,
        redacted.get("schema").getAsString(),
        "A malformed representations field must not stop the schema from being redacted.");
  }
}
