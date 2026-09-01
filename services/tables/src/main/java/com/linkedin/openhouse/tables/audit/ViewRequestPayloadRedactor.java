package com.linkedin.openhouse.tables.audit;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.linkedin.openhouse.common.audit.ServiceAuditPayloadRedactor;
import javax.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;

/**
 * Keeps view definitions out of service audit events.
 *
 * <p>{@link com.linkedin.openhouse.common.audit.ServiceAuditAspect} audits the complete cached
 * request body of every controller call, which for the view create and replace routes would retain
 * the caller's full SQL text and schema document. This replaces {@code schema} and every {@code
 * representations[*].sql} value with {@link #REDACTED_VALUE} before the event is built. The keys
 * are kept, so an auditor still sees that the fields were sent.
 *
 * <p>Scoped by request URI rather than by field name on purpose. {@code
 * CreateUpdateTableRequestBody} also carries a {@code schema}, and redacting by name alone would
 * silently change table, database and snapshot audit payloads. Matching the view routes leaves
 * every other route's payload exactly as it was.
 *
 * <p>Every field that is not part of the view definition — {@code viewId}, {@code databaseId},
 * {@code clusterId}, {@code sourceDialect}, {@code defaultCatalog}, {@code defaultNamespace},
 * {@code viewProperties} and {@code baseViewVersion} — is left intact, so an audit event still
 * identifies what was operated on and by whom.
 */
@Component
public class ViewRequestPayloadRedactor implements ServiceAuditPayloadRedactor {

  static final String SCHEMA_FIELD = "schema";
  static final String REPRESENTATIONS_FIELD = "representations";
  static final String SQL_FIELD = "sql";

  /** The view collection route, which POST creates against. */
  private static final String VIEW_COLLECTION_PATTERN = "/v1/databases/*/views";

  /** The view item route, which PUT replaces against. */
  private static final String VIEW_ITEM_PATTERN = "/v1/databases/*/views/*";

  private static final AntPathMatcher PATH_MATCHER = new AntPathMatcher();

  @Override
  public boolean appliesTo(HttpServletRequest request) {
    String uri = request.getRequestURI();
    return uri != null
        && (PATH_MATCHER.match(VIEW_COLLECTION_PATTERN, uri)
            || PATH_MATCHER.match(VIEW_ITEM_PATTERN, uri));
  }

  @Override
  public JsonElement redact(JsonElement requestPayload) {
    if (requestPayload == null || !requestPayload.isJsonObject()) {
      // A bodyless request parses to JsonNull, and a malformed body can be any other element.
      // Neither carries a view definition, so there is nothing to remove.
      return requestPayload;
    }
    JsonObject redacted = requestPayload.deepCopy().getAsJsonObject();
    if (redacted.has(SCHEMA_FIELD)) {
      redacted.add(SCHEMA_FIELD, new JsonPrimitive(REDACTED_VALUE));
    }
    JsonElement representations = redacted.get(REPRESENTATIONS_FIELD);
    if (representations != null && representations.isJsonArray()) {
      for (JsonElement representation : representations.getAsJsonArray()) {
        if (representation.isJsonObject() && representation.getAsJsonObject().has(SQL_FIELD)) {
          representation.getAsJsonObject().add(SQL_FIELD, new JsonPrimitive(REDACTED_VALUE));
        }
      }
    }
    return redacted;
  }
}
