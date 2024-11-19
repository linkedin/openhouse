package com.linkedin.openhouse.tablestest;

import java.util.Collections;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.util.TestSocketUtils;

/**
 * Standalone embedded OH server that can be started and stopped from any Java code (to be used for
 * testing). Users have an option of providing a custom portNo, otherwise it will automatically be
 * determined. Once the server has started with {@link OpenHouseLocalServer#start()}, portNo can be
 * queried using {@link OpenHouseLocalServer#getPort()}. The server can be stopped with {@link
 * OpenHouseLocalServer#stop()}.
 */
public class OpenHouseLocalServer {

  private int port;
  private ConfigurableApplicationContext appContext;

  public OpenHouseLocalServer() {
    this.port = TestSocketUtils.findAvailableTcpPort();
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
      application.setDefaultProperties(
          Collections.singletonMap("server.port", String.valueOf(port)));
      if (applyTomcatFix) {
        fixTomcatInstantiation();
      }
      appContext = application.run();
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
