package com.linkedin.openhouse.logging;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.ILoggerFactory;
import org.slf4j.IMarkerFactory;
import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.helpers.AbstractLogger;
import org.slf4j.helpers.BasicMarkerFactory;
import org.slf4j.helpers.MessageFormatter;
import org.slf4j.helpers.NOPMDCAdapter;
import org.slf4j.spi.MDCAdapter;
import org.slf4j.spi.SLF4JServiceProvider;

/**
 * Private SLF4J provider for the vendored runtime.
 *
 * <p>The provider delegates to {@link System.Logger}, the only logging boundary shared with a
 * consumer. The provider and SLF4J API are relocated in the published JAR, so they cannot discover
 * or interfere with a consumer's SLF4J provider.
 */
public final class SystemLoggerServiceProvider implements SLF4JServiceProvider {
  private static final String REQUESTED_API_VERSION = "2.0.99";

  private final ILoggerFactory loggerFactory = new SystemLoggerFactory();
  private final IMarkerFactory markerFactory = new BasicMarkerFactory();
  private final MDCAdapter mdcAdapter = new NOPMDCAdapter();

  @Override
  public ILoggerFactory getLoggerFactory() {
    return loggerFactory;
  }

  @Override
  public IMarkerFactory getMarkerFactory() {
    return markerFactory;
  }

  @Override
  public MDCAdapter getMDCAdapter() {
    return mdcAdapter;
  }

  @Override
  public String getRequestedApiVersion() {
    return REQUESTED_API_VERSION;
  }

  @Override
  public void initialize() {}

  private static final class SystemLoggerFactory implements ILoggerFactory {
    private final Map<String, Logger> loggers = new ConcurrentHashMap<>();

    @Override
    public Logger getLogger(String name) {
      return loggers.computeIfAbsent(name, SystemLoggerAdapter::new);
    }
  }

  private static final class SystemLoggerAdapter extends AbstractLogger {
    private static final long serialVersionUID = 1L;

    private final transient System.Logger delegate;

    private SystemLoggerAdapter(String name) {
      this.name = name;
      this.delegate = System.getLogger(name);
    }

    @Override
    public boolean isTraceEnabled() {
      return delegate.isLoggable(System.Logger.Level.TRACE);
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
      return isTraceEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
      return delegate.isLoggable(System.Logger.Level.DEBUG);
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
      return isDebugEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
      return delegate.isLoggable(System.Logger.Level.INFO);
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
      return isInfoEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
      return delegate.isLoggable(System.Logger.Level.WARNING);
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
      return isWarnEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
      return delegate.isLoggable(System.Logger.Level.ERROR);
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
      return isErrorEnabled();
    }

    @Override
    protected String getFullyQualifiedCallerName() {
      return SystemLoggerAdapter.class.getName();
    }

    @Override
    protected void handleNormalizedLoggingCall(
        Level level,
        Marker marker,
        String messagePattern,
        Object[] arguments,
        Throwable throwable) {
      System.Logger.Level systemLevel = toSystemLevel(level);
      if (!delegate.isLoggable(systemLevel)) {
        return;
      }

      String message = MessageFormatter.basicArrayFormat(messagePattern, arguments);
      if (marker != null) {
        message = "[" + marker.getName() + "] " + message;
      }
      if (throwable == null) {
        delegate.log(systemLevel, message);
      } else {
        delegate.log(systemLevel, message, throwable);
      }
    }

    private static System.Logger.Level toSystemLevel(Level level) {
      switch (level) {
        case TRACE:
          return System.Logger.Level.TRACE;
        case DEBUG:
          return System.Logger.Level.DEBUG;
        case INFO:
          return System.Logger.Level.INFO;
        case WARN:
          return System.Logger.Level.WARNING;
        case ERROR:
          return System.Logger.Level.ERROR;
        default:
          throw new IllegalArgumentException("Unsupported SLF4J level: " + level);
      }
    }
  }
}
