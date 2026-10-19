package com.linkedin.openhouse.tables.mock.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

public class Log4j2LogCaptureTest {

  private static final Logger APPLICATION_LOG =
      LogManager.getLogger("com.linkedin.openhouse.tables.capture.PositiveControl");
  private static final Logger FRAMEWORK_LOG =
      LogManager.getLogger("org.springframework.web.servlet.DispatcherServlet");

  @Test
  public void capturesApplicationDebugAndNestedThrowablesWithoutRaisingFrameworkLevels() {
    Level rootBefore = LogManager.getRootLogger().getLevel();
    Level applicationBefore = APPLICATION_LOG.getLevel();
    boolean frameworkDebugBefore = FRAMEWORK_LOG.isDebugEnabled();

    try (Log4j2LogCapture capture = new Log4j2LogCapture()) {
      assertEquals(rootBefore, LogManager.getRootLogger().getLevel());
      assertEquals(frameworkDebugBefore, FRAMEWORK_LOG.isDebugEnabled());

      APPLICATION_LOG.debug("positive-control-debug-message");
      APPLICATION_LOG.error(
          "positive-control-message",
          new IllegalStateException(
              "positive-control-outer", new RuntimeException("positive-control-nested-cause")));

      String rendered = capture.renderedEvents();
      assertTrue(rendered.contains("positive-control-debug-message"), rendered);
      assertTrue(rendered.contains("positive-control-message"), rendered);
      assertTrue(rendered.contains("positive-control-outer"), rendered);
      assertTrue(rendered.contains("positive-control-nested-cause"), rendered);
      assertFalse(capture.isEmpty());
    }

    assertEquals(rootBefore, LogManager.getRootLogger().getLevel());
    assertEquals(applicationBefore, APPLICATION_LOG.getLevel());
    assertEquals(frameworkDebugBefore, FRAMEWORK_LOG.isDebugEnabled());
  }

  @Test
  public void stopsCapturingAfterClose() {
    Log4j2LogCapture capture = new Log4j2LogCapture();
    capture.close();

    APPLICATION_LOG.error("emitted-after-close");

    assertFalse(capture.renderedEvents().contains("emitted-after-close"));
  }
}
