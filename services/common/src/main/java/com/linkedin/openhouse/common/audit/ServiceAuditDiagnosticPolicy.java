package com.linkedin.openhouse.common.audit;

import java.util.List;
import javax.servlet.http.HttpServletRequest;

/**
 * Decides whether diagnostic output about a request must omit request details, using the existing
 * {@link ServiceAuditPayloadRedactor} extension point so no route knowledge lives in this module.
 * Shared by {@link ServiceAuditAspect} and {@link DummyServiceAuditHandler}.
 */
final class ServiceAuditDiagnosticPolicy {

  private ServiceAuditDiagnosticPolicy() {}

  /**
   * @param redactors the installed payload redactors; may be empty
   * @param request the request being audited, or null when no request context is available
   * @return true when a redactor that suppresses diagnostic details claims the request, or when
   *     such a redactor is installed and the request cannot be established (fail closed). Route
   *     classification failures propagate to the caller.
   */
  static boolean suppressDiagnosticDetails(
      List<ServiceAuditPayloadRedactor> redactors, HttpServletRequest request) {
    if (redactors == null) {
      return false;
    }
    for (ServiceAuditPayloadRedactor redactor : redactors) {
      if (redactor.suppressDiagnosticDetails()
          && (request == null || redactor.appliesTo(request))) {
        return true;
      }
    }
    return false;
  }
}
