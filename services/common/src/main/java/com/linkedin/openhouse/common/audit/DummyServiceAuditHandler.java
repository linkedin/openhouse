package com.linkedin.openhouse.common.audit;

import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * A dummy service audit handler which is used in only unit-tests and local docker-environments. It
 * only logs and retains nothing. For a request that a payload redactor marks as diagnostic-
 * sensitive (see {@link ServiceAuditPayloadRedactor#suppressDiagnosticDetails()}), or when called
 * without a request context while such a redactor is installed, it logs a fixed summary instead of
 * the event, so request details never reach ordinary logs.
 */
@Slf4j
@Component
public class DummyServiceAuditHandler implements AuditHandler<ServiceAuditEvent> {

  @Autowired(required = false)
  private List<ServiceAuditPayloadRedactor> payloadRedactors = Collections.emptyList();

  @Override
  public void audit(ServiceAuditEvent event) {
    RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
    HttpServletRequest request =
        attributes instanceof ServletRequestAttributes
            ? ((ServletRequestAttributes) attributes).getRequest()
            : null;
    if (ServiceAuditDiagnosticPolicy.suppressDiagnosticDetails(payloadRedactors, request)) {
      log.info(
          "Service audit summary: method={} status={} sessionId={} (request details omitted)",
          event.getMethod(),
          event.getStatusCode(),
          event.getSessionId());
      return;
    }
    log.info("Service audit event: \n" + event.toJson());
  }
}
