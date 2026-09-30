package com.linkedin.openhouse.tablestest;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
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
      application.setDefaultProperties(
          Collections.singletonMap("server.port", String.valueOf(port)));
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
   * Rewrites a table's committed metadata without {@code key}, bypassing tables-service, as for a
   * table committed before tables-service started setting that property. Test support only.
   */
  public synchronized void removeCommittedProperty(String databaseId, String tableId, String key) {
    TableOperations ops =
        ((HasTableOperations)
                appContext
                    .getBean(Catalog.class)
                    .loadTable(TableIdentifier.of(databaseId, tableId)))
            .operations();
    TableMetadata committed = ops.current();
    String location =
        committed
            .metadataFileLocation()
            .replace(".metadata.json", "-" + UUID.randomUUID() + ".metadata.json");
    Map<String, String> properties = new HashMap<>(committed.properties());
    properties.remove(key);
    properties.put(getCanonicalFieldName("tableLocation"), location);
    TableMetadataParser.write(
        committed.replaceProperties(properties), ops.io().newOutputFile(location));

    HouseTableRepository houseTables = appContext.getBean(HouseTableRepository.class);
    HouseTable row =
        houseTables
            .findById(
                HouseTablePrimaryKey.builder().databaseId(databaseId).tableId(tableId).build())
            .orElseThrow(() -> new IllegalArgumentException(databaseId + "." + tableId));
    houseTables.save(row.toBuilder().tableLocation(location).build());
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
