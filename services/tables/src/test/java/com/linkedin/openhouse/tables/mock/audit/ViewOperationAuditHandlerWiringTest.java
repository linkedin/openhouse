package com.linkedin.openhouse.tables.mock.audit;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.tables.audit.DummyViewAuditHandler;
import com.linkedin.openhouse.tables.audit.ViewOperationAuditEmitter;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Real Spring constructor injection of the operation-audit sink. The default handler is not a bean,
 * so a deployment-supplied handler replaces it through {@code ObjectProvider} rather than competing
 * with it, and the override does not depend on the bean being named {@code viewAuditHandler}.
 */
public class ViewOperationAuditHandlerWiringTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(ViewOperationAuditEmitter.class);

  @Test
  public void aDeploymentViewAuditHandlerReplacesTheDefaultWhateverTheBeanIsNamed() {
    contextRunner
        .withUserConfiguration(CustomViewAuditHandlerConfig.class)
        .run(
            context -> {
              assertNull(
                  context.getStartupFailure(),
                  "a deployment handler must not collide with an OSS default bean");
              @SuppressWarnings("unchecked")
              AuditHandler<ViewAuditEvent> custom =
                  (AuditHandler<ViewAuditEvent>) context.getBean("someDeploymentAuditSink");
              ViewOperationAuditEmitter emitter = context.getBean(ViewOperationAuditEmitter.class);
              assertSame(custom, ReflectionTestUtils.getField(emitter, "auditHandler"));

              emitter.emit(
                  ViewAuditEvent.builder().databaseName("d").viewName("v").user("alice").build(),
                  OperationStatus.FAILED);

              verify(custom, times(1)).audit(any(ViewAuditEvent.class));
            });
  }

  @Test
  public void defaultViewAuditHandlerIsUsedWhenNoCustomHandlerIsDefined() {
    contextRunner.run(
        context -> {
          assertNull(context.getStartupFailure());
          assertInstanceOf(
              DummyViewAuditHandler.class,
              ReflectionTestUtils.getField(
                  context.getBean(ViewOperationAuditEmitter.class), "auditHandler"));
        });
  }

  @Configuration
  static class CustomViewAuditHandlerConfig {
    @Bean
    @SuppressWarnings("unchecked")
    AuditHandler<ViewAuditEvent> someDeploymentAuditSink() {
      return mock(AuditHandler.class);
    }
  }
}
