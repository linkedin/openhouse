package com.linkedin.openhouse.tables.audit;

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** A dummy view audit handler which is used in only unit-tests and local docker-environments. */
@Slf4j
@Component
public class DummyViewAuditHandler implements AuditHandler<ViewAuditEvent> {
  @Override
  public void audit(ViewAuditEvent event) {
    log.info("View audit event: \n" + event.toJson());
  }
}
