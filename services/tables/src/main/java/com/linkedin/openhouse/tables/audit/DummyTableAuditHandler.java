package com.linkedin.openhouse.tables.audit;

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.tables.audit.model.TableAuditEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** A dummy table audit handler which is used in only unit-tests and local docker-environments. */
@Slf4j
@Component
public class DummyTableAuditHandler implements AuditHandler<TableAuditEvent> {
  @Override
  public void audit(TableAuditEvent event) {
    log.info("Table audit event: \n" + event.toJson());
  }
}
