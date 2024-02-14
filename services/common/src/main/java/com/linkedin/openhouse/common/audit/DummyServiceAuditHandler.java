package com.linkedin.openhouse.common.audit;

import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** A dummy service audit handler which is used in only unit-tests and local docker-environments. */
@Slf4j
@Component
public class DummyServiceAuditHandler implements AuditHandler<ServiceAuditEvent> {
  @Override
  public void audit(ServiceAuditEvent event) {
    log.info("Service audit event: \n" + event.toJson());
  }
}
