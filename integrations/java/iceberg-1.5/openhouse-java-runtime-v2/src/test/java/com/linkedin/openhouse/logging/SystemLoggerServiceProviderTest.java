package com.linkedin.openhouse.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

class SystemLoggerServiceProviderTest {
  private static final String RELOCATED_LOGGER_FACTORY =
      "com.linkedin.openhouse.relocated.org.slf4j.LoggerFactory";

  @Test
  void loadsPrivateProviderAndEmitsThroughSystemLogger() throws Exception {
    Path runtimeJar = Path.of(System.getProperty("vendoredRuntimeJar"));
    assertTrue(Files.isRegularFile(runtimeJar), "vendored runtime JAR must exist");

    String loggerName = getClass().getName() + ".isolated";
    Logger systemLogger = Logger.getLogger(loggerName);
    List<LogRecord> records = new CopyOnWriteArrayList<>();
    Handler capturingHandler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            records.add(record);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };

    Level previousLevel = systemLogger.getLevel();
    boolean previousUseParentHandlers = systemLogger.getUseParentHandlers();
    systemLogger.setLevel(Level.ALL);
    systemLogger.setUseParentHandlers(false);
    systemLogger.addHandler(capturingHandler);

    try (URLClassLoader isolatedClassLoader =
        new URLClassLoader(
            new URL[] {runtimeJar.toUri().toURL()}, ClassLoader.getPlatformClassLoader())) {
      assertThrows(
          ClassNotFoundException.class,
          () -> Class.forName("org.slf4j.LoggerFactory", true, isolatedClassLoader));

      Class<?> loggerFactory = Class.forName(RELOCATED_LOGGER_FACTORY, true, isolatedClassLoader);
      Object logger = loggerFactory.getMethod("getLogger", String.class).invoke(null, loggerName);
      assertEquals(
          "com.linkedin.openhouse.logging.SystemLoggerServiceProvider$SystemLoggerAdapter",
          logger.getClass().getName());

      logger
          .getClass()
          .getMethod("warn", String.class, Object.class)
          .invoke(logger, "private provider {}", "works");

      assertEquals(1, records.size());
      assertEquals(Level.WARNING, records.get(0).getLevel());
      assertEquals("private provider works", records.get(0).getMessage());
    } finally {
      systemLogger.removeHandler(capturingHandler);
      systemLogger.setUseParentHandlers(previousUseParentHandlers);
      systemLogger.setLevel(previousLevel);
    }
  }
}
