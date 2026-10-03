package com.linkedin.openhouse.common.audit;

import javax.servlet.http.HttpServletRequest;

/**
 * Extension point that removes sensitive values from a request URI (including its query string)
 * before {@link ServiceAuditAspect} writes it into a {@link
 * com.linkedin.openhouse.common.audit.model.ServiceAuditEvent}.
 *
 * <p>The aspect audits the raw request URI and query string of every controller call, so a route
 * whose query carries content that must not be retained has to opt out here. Only the mechanism
 * lives in {@code services/common}: each service contributes its own beans, and a service that
 * contributes none has its URI audited exactly as before.
 *
 * <p>Implementations must not mutate the argument; they return a redacted copy (or the argument
 * unchanged when there is nothing to redact).
 */
public interface ServiceAuditUriRedactor {

  /** Marker written in place of a redacted value. */
  String REDACTED_VALUE = ServiceAuditPayloadRedactor.REDACTED_VALUE;

  /** @return whether this redactor owns {@code request}. */
  boolean appliesTo(HttpServletRequest request);

  /**
   * @param uriAndQueryString the request URI, plus {@code ?} and the raw query string when one was
   *     sent.
   * @return a redacted copy, or the argument unchanged when there is nothing to redact.
   */
  String redact(String uriAndQueryString);
}
