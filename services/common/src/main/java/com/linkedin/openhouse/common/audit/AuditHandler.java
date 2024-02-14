package com.linkedin.openhouse.common.audit;

/** Handler interface injected in the audit aspect to send an event. */
public interface AuditHandler<BaseAuditEvent> {
  /**
   * Logic to display or send the event given an audit event object.
   *
   * @param event
   */
  void audit(BaseAuditEvent event);
}
