package com.linkedin.openhouse.tables.mock.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.tables.api.handler.ViewsApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
import com.linkedin.openhouse.tables.audit.ViewAuditAspect;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import com.linkedin.openhouse.tables.audit.model.ViewOperationType;
import com.linkedin.openhouse.tables.mock.logging.Log4j2LogCapture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;

/**
 * Real Spring wiring of the view operation-audit sink behind an auto-proxied handler. The default
 * handler is not a bean, so a deployment-supplied handler replaces it through {@code
 * ObjectProvider} rather than competing with it, and the override does not depend on the bean being
 * named {@code viewAuditHandler}.
 */
public class ViewOperationAuditHandlerWiringTest {

  private static final String CLUSTER = "wiring-cluster";

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(AopAutoConfiguration.class))
          .withUserConfiguration(HandlerConfig.class, ViewAuditAspect.class);

  @Test
  public void aDeploymentViewAuditHandlerReplacesTheDefaultWhateverTheBeanIsNamed() {
    contextRunner
        .withUserConfiguration(CustomViewAuditHandlerConfig.class)
        .run(
            context -> {
              assertNull(
                  context.getStartupFailure(),
                  "a deployment handler must not collide with an OSS default bean");
              ViewsApiHandler handler = context.getBean(ViewsApiHandler.class);
              assertTrue(AopUtils.isAopProxy(handler), "the handler must be advised");

              handler.deleteView("d", "v", "alice");

              @SuppressWarnings("unchecked")
              AuditHandler<ViewAuditEvent> custom =
                  (AuditHandler<ViewAuditEvent>) context.getBean("someDeploymentAuditSink");
              ArgumentCaptor<ViewAuditEvent> event = ArgumentCaptor.forClass(ViewAuditEvent.class);
              verify(custom, times(1)).audit(event.capture());
              assertEquals(ViewOperationType.DROP, event.getValue().getOperationType());
              assertEquals(OperationStatus.SUCCESS, event.getValue().getOperationStatus());
              assertEquals(CLUSTER, event.getValue().getClusterName());
            });
  }

  @Test
  public void defaultViewAuditHandlerIsUsedWhenNoCustomHandlerIsDefined() {
    contextRunner.run(
        context -> {
          assertNull(context.getStartupFailure());
          ViewsApiHandler handler = context.getBean(ViewsApiHandler.class);
          assertTrue(AopUtils.isAopProxy(handler), "the handler must be advised");

          String logs;
          try (Log4j2LogCapture capture = new Log4j2LogCapture()) {
            handler.deleteView("d", "v", "alice");
            logs = capture.renderedEvents();
          }

          // DummyViewAuditHandler's debug-only rendering of the delivered event.
          assertTrue(logs.contains("View audit event:"), logs);
          assertTrue(logs.contains("\"operationType\":\"DROP\""), logs);
        });
  }

  /**
   * Plain bean-method classes, not {@code @Configuration}: the mock application scans this package
   * without the test-type exclude filter, so annotated nested configurations would leak into other
   * test contexts.
   */
  static class HandlerConfig {
    @Bean
    ClusterProperties clusterProperties() {
      ClusterProperties clusterProperties = mock(ClusterProperties.class);
      when(clusterProperties.getClusterName()).thenReturn(CLUSTER);
      return clusterProperties;
    }

    @Bean
    ViewsApiHandler viewsApiHandler() {
      return new DroppingViewsApiHandler();
    }
  }

  static class CustomViewAuditHandlerConfig {
    @Bean
    @SuppressWarnings("unchecked")
    AuditHandler<ViewAuditEvent> someDeploymentAuditSink() {
      return mock(AuditHandler.class);
    }
  }

  /** Only the drop route is exercised; it succeeds with no body. */
  static class DroppingViewsApiHandler implements ViewsApiHandler {
    @Override
    public ApiResponse<GetViewResponseBody> getView(
        String databaseId, String viewId, String actingPrincipal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiResponse<GetAllViewsResponseBody> getAllViews(
        String databaseId, String pageToken, int size, String sortBy, String actingPrincipal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiResponse<GetViewResponseBody> createView(
        CreateUpdateViewRequestBody requestBody, String actingPrincipal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiResponse<GetViewResponseBody> updateView(
        CreateUpdateViewRequestBody requestBody, String actingPrincipal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApiResponse<Void> deleteView(String databaseId, String viewId, String actingPrincipal) {
      return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
    }
  }
}
