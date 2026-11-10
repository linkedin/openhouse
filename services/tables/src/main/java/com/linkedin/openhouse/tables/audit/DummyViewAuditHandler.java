package com.linkedin.openhouse.tables.audit;

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import lombok.extern.slf4j.Slf4j;

/**
 * The default view audit handler, used whenever a deployment does not supply its own {@link
 * AuditHandler} for {@link ViewAuditEvent}. It is deliberately not a {@code @Component}: {@link
 * ViewOperationAuditEmitter} takes it through {@code ObjectProvider}, so a deployment-supplied
 * handler replaces it instead of competing with it.
 *
 * <p>It logs at {@code debug} only, so view audit events stay out of default logs. {@link
 * ViewAuditEvent} carries no view definition, SQL, or schema, so this stays definition-free even
 * with debug logging enabled.
 */
@Slf4j
public class DummyViewAuditHandler implements AuditHandler<ViewAuditEvent> {
  @Override
  public void audit(ViewAuditEvent event) {
    if (log.isDebugEnabled()) {
      log.debug("View audit event: \n" + event.toJson());
    }
  }
}
