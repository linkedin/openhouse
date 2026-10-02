package com.linkedin.openhouse.tablestest;

import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Standalone embedded OH server that can be started and stopped from any Java code (to be used for
 * testing). Users have an option of providing a custom portNo, otherwise it will automatically be
 * determined. Once the server has started with {@link OpenHouseLocalServer#start()}, portNo can be
 * queried using {@link OpenHouseLocalServer#getPort()}. The server can be stopped with {@link
 * OpenHouseLocalServer#stop()}.
 */
public class OpenHouseLocalServer {

  /**
   * Oldest client release this server admits for tables opted into column defaults. A test that
   * reads or writes such a table identifies its catalog as client-name {@code spark} with this
   * client-version.
   */
  public static final String COLUMN_DEFAULT_MINIMUM_CLIENT_VERSION = "0.5.100";

  private int port;
  private ConfigurableApplicationContext appContext;

  /** Create server with OS-assigned port (determined at startup time). */
  public OpenHouseLocalServer() {
    this.port = 0;
    this.appContext = null;
  }

  public OpenHouseLocalServer(int port) {
    this.port = port;
    this.appContext = null;
  }

  /** Start the embedded OH server with tomcat fix */
  public void start() {
    start(true);
  }

  /** Start the embedded OH server */
  public synchronized void start(boolean applyTomcatFix) {
    if (appContext == null || !appContext.isActive()) {
      SpringApplication application = new SpringApplication(SpringH2TestApplication.class);
      Map<String, Object> defaults = new HashMap<>();
      defaults.put("server.port", String.valueOf(port));
      defaults.put(
          "cluster.read-bridge.column-default.minimum-client-version",
          COLUMN_DEFAULT_MINIMUM_CLIENT_VERSION);
      application.setDefaultProperties(defaults);
      if (applyTomcatFix) {
        fixTomcatInstantiation();
      }
      appContext = application.run();
      this.port = ((WebServerApplicationContext) appContext).getWebServer().getPort();
    } else {
      throw new IllegalArgumentException(
          "OpenHouse test server has already been started, please stop the application first with OpenHouseJavaItestService#Start");
    }
  }

  /** Stop the embedded OH server */
  public synchronized void stop() {
    if (appContext != null && appContext.isActive()) {
      SpringApplication.exit(appContext);
    } else {
      throw new IllegalArgumentException(
          "OpenHouse test server has not been started yet, please start the application first with OpenHouseJavaItestService#Stop");
    }
  }

  /**
   * URLStreamHandlerFactory can be set by Spark or any other libraries. This method ensures that
   * the URLStreamHandlerFactory is set to Tomcat's implementation before starting the embedded
   * Tomcat, otherwise it will instantiate the implementation without setting default
   * URLStreamHandlerFactory.
   *
   * <p>This is springboot's recommended fix: please see {@link
   * https://github.com/spring-projects/spring-boot/issues/21535}
   */
  private void fixTomcatInstantiation() {
    try {
      org.apache.catalina.webresources.TomcatURLStreamHandlerFactory.register();
    } catch (Error e) {
      org.apache.catalina.webresources.TomcatURLStreamHandlerFactory.disable();
    }
  }

  /**
   * get port number in localhost where OH server is started
   *
   * @return int port number
   */
  public int getPort() {
    return port;
  }
}
