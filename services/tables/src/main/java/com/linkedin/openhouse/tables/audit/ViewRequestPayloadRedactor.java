package com.linkedin.openhouse.tables.audit;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.linkedin.openhouse.common.audit.ServiceAuditPayloadRedactor;
import javax.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Component;

/**
 * Keeps the write CAS token out of service audit events and marks view requests as diagnostic-
 * sensitive.
 *
 * <p>{@link com.linkedin.openhouse.common.audit.ServiceAuditAspect} audits the complete cached
 * request body of every controller call. For the view create and replace routes this replaces only
 * {@code baseMetadataLocation} with {@link #REDACTED_VALUE}, keeping the key. The supplied {@code
 * schema}, every {@code representations} entry (including its SQL) and all other fields stay in the
 * event exactly as parsed, so a custom audit handler records which definition was requested.
 *
 * <p>The built-in {@link com.linkedin.openhouse.common.audit.DummyServiceAuditHandler} only logs,
 * and request definitions must stay out of ordinary logs, so {@link #suppressDiagnosticDetails()}
 * is true: for these routes it logs a fixed summary and the aspect's failure logs omit details.
 * That means the default handler does not retain definitions; a deployment that needs them must
 * supply its own handler, which must in turn keep them out of diagnostic logs.
 *
 * <p>Scoped by request route rather than by field name on purpose: {@code
 * CreateUpdateTableRequestBody} has a field that must keep its current audit behavior. The view
 * routes (the collection POST creates against and the item PUT replaces against) are classified
 * like MVC by {@link ViewRequestRouteMatcher}, so every other route's payload is left exactly as it
 * was.
 */
@Component
public class ViewRequestPayloadRedactor implements ServiceAuditPayloadRedactor {

  static final String BASE_METADATA_LOCATION_FIELD = "baseMetadataLocation";

  @Override
  public boolean appliesTo(HttpServletRequest request) {
    return ViewRequestRouteMatcher.isViewCollectionOrItem(request);
  }

  @Override
  public boolean suppressDiagnosticDetails() {
    return true;
  }

  @Override
  public JsonElement redact(JsonElement requestPayload) {
    if (requestPayload == null || !requestPayload.isJsonObject()) {
      // A bodyless request parses to JsonNull, and a malformed body can be any other element.
      // Neither carries a CAS token, so there is nothing to remove.
      return requestPayload;
    }
    JsonObject redacted = requestPayload.deepCopy().getAsJsonObject();
    if (redacted.has(BASE_METADATA_LOCATION_FIELD)) {
      redacted.add(BASE_METADATA_LOCATION_FIELD, new JsonPrimitive(REDACTED_VALUE));
    }
    return redacted;
  }
}
