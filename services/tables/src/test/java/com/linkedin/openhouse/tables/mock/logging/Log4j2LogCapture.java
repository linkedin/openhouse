package com.linkedin.openhouse.tables.mock.logging;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * Captures log events for the duration of a test. Every event reaching the root logger config is
 * captured at the configured framework levels, which stay untouched. Application loggers under
 * {@link #APPLICATION_LOGGER} are temporarily opened to all levels so their debug output is also
 * inspected; their events propagate to the root config, where the capturing appender is attached.
 */
public class Log4j2LogCapture implements AutoCloseable {

  public static final String APPLICATION_LOGGER = "com.linkedin.openhouse";

  private final LoggerContext context;
  private final Configuration configuration;
  private final LoggerConfig rootConfig;
  private final CapturingAppender appender;
  private final LoggerConfig applicationConfig;
  private final boolean createdApplicationConfig;
  private final Level originalApplicationLevel;

  public Log4j2LogCapture() {
    context = (LoggerContext) LogManager.getContext(false);
    configuration = context.getConfiguration();
    rootConfig = configuration.getRootLogger();
    appender = new CapturingAppender();
    appender.start();
    rootConfig.addAppender(appender, null, null);

    LoggerConfig existing = configuration.getLoggerConfig(APPLICATION_LOGGER);
    if (APPLICATION_LOGGER.equals(existing.getName())) {
      applicationConfig = existing;
      createdApplicationConfig = false;
      originalApplicationLevel = existing.getLevel();
      applicationConfig.setLevel(Level.ALL);
    } else {
      applicationConfig = new LoggerConfig(APPLICATION_LOGGER, Level.ALL, true);
      createdApplicationConfig = true;
      originalApplicationLevel = null;
      configuration.addLogger(APPLICATION_LOGGER, applicationConfig);
    }
    context.updateLoggers();
  }

  public String renderedEvents() {
    return appender.events().stream()
        .map(Log4j2LogCapture::render)
        .collect(Collectors.joining("\n"));
  }

  public boolean isEmpty() {
    return appender.events().isEmpty();
  }

  @Override
  public void close() {
    rootConfig.removeAppender(appender.getName());
    if (createdApplicationConfig) {
      configuration.removeLogger(APPLICATION_LOGGER);
    } else {
      applicationConfig.setLevel(originalApplicationLevel);
    }
    appender.stop();
    context.updateLoggers();
  }

  private static String render(LogEvent event) {
    StringBuilder builder = new StringBuilder(event.getMessage().getFormattedMessage());
    if (event.getThrown() != null) {
      builder.append('\n').append(event.getThrown());
    }
    if (event.getThrownProxy() != null) {
      builder.append('\n').append(event.getThrownProxy().getExtendedStackTraceAsString());
    }
    return builder.toString();
  }

  private static class CapturingAppender extends AbstractAppender {
    private final List<LogEvent> events = Collections.synchronizedList(new ArrayList<>());

    private CapturingAppender() {
      super(
          "openhouse-test-log-capture",
          (Filter) null,
          (Layout<? extends Serializable>) PatternLayout.createDefaultLayout(),
          false,
          Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }

    private List<LogEvent> events() {
      synchronized (events) {
        return new ArrayList<>(events);
      }
    }
  }
}
