package com.linkedin.openhouse.common.audit;

import com.google.gson.JsonElement;
import javax.servlet.http.HttpServletRequest;

/**
 * Extension point that removes sensitive values from a request payload before {@link
 * ServiceAuditAspect} writes it into a {@link
 * com.linkedin.openhouse.common.audit.model.ServiceAuditEvent}.
 *
 * <p>The aspect audits the complete cached request body of every controller call, so a route whose
 * body carries content that must not be retained has to opt out here. Only the mechanism lives in
 * {@code services/common}: each service contributes its own beans and names its own fields, and a
 * service that contributes none has its payload audited exactly as before.
 *
 * <p>Implementations are handed the parsed payload and must not mutate it. Returning a modified
 * copy keeps a redactor from observing another redactor's half-rewritten tree, and keeps the
 * decision to drop the payload entirely with the aspect.
 */
public interface ServiceAuditPayloadRedactor {

  /**
   * Marker written in place of a redacted value. Implementations replace the value and keep the
   * key, so an auditor can still see that the field was present in the request.
   */
  String REDACTED_VALUE = "[REDACTED]";

  /** @return whether this redactor owns {@code request}. */
  boolean appliesTo(HttpServletRequest request);

  /**
   * @param requestPayload the parsed request body, which may be any {@link JsonElement} the caller
   *     sent, including a non-object or {@link com.google.gson.JsonNull}.
   * @return a redacted copy, or the argument unchanged when there is nothing to redact.
   */
  JsonElement redact(JsonElement requestPayload);

  /**
   * Whether diagnostic output (the built-in {@link DummyServiceAuditHandler} log and the audit
   * aspect's own failure logs) must omit request details for the requests selected by {@link
   * #appliesTo(HttpServletRequest)}. This concerns diagnostics only: it neither drops fields from
   * the audit event nor stops the event from being delivered in full to the configured {@link
   * AuditHandler}. The built-in handler only logs, so it does not retain such an event; a service
   * that must keep these events needs a custom handler that honors this rule itself.
   *
   * @return false by default, which keeps existing diagnostic behavior.
   */
  default boolean suppressDiagnosticDetails() {
    return false;
  }
}
