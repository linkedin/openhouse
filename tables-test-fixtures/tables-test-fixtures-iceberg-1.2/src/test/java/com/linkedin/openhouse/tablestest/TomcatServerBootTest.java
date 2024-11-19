package com.linkedin.openhouse.tablestest;

import java.lang.reflect.Field;
import java.net.URL;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.ReflectionUtils;

public class TomcatServerBootTest {

  @BeforeEach
  public void setUpTest() {
    // each test shares same jvm, inorder to properly test each scenario we need to reset the URL.
    resetJVMStaticInstance();
    resetClassLoaderStaticInstance();
  }
  /**
   * Test setting {@link FsUrlStreamHandlerFactory} before starting the embedded Tomcat
   *
   * <p>Without the fix of {@link OpenHouseLocalServer#fixTomcatInstantiation()}, the following
   * exception will be thrown: Caused by: java.lang.Error: factory already defined at
   * java.net.URL.setURLStreamHandlerFactory(URL.java:1135)
   */
  @Test
  public void testTomcatFix() {
    FsUrlStreamHandlerFactory factory = new FsUrlStreamHandlerFactory();
    URL.setURLStreamHandlerFactory(factory);

    OpenHouseLocalServer openHouseLocalServer1 = new OpenHouseLocalServer();
    Assertions.assertDoesNotThrow(() -> openHouseLocalServer1.start());
  }

  @Test
  public void testNoTomcatFixThrowsError() {
    FsUrlStreamHandlerFactory factory = new FsUrlStreamHandlerFactory();
    URL.setURLStreamHandlerFactory(factory);

    OpenHouseLocalServer openHouseLocalServer1 = new OpenHouseLocalServer();
    Exception exception =
        Assertions.assertThrows(Exception.class, () -> openHouseLocalServer1.start(false));
    Assertions.assertTrue(exception.getMessage().contains("Unable to start embedded Tomcat"));
  }

  private void resetJVMStaticInstance() {
    Field factoryField = ReflectionUtils.findField(URL.class, "factory");
    ReflectionUtils.makeAccessible(factoryField);
    ReflectionUtils.setField(factoryField, null, null);
  }

  private void resetClassLoaderStaticInstance() {
    Field instanceField = null;
    try {
      instanceField =
          ReflectionUtils.findField(
              Class.forName("org.apache.catalina.webresources.TomcatURLStreamHandlerFactory"),
              "instance");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    ReflectionUtils.makeAccessible(instanceField);
    ReflectionUtils.setField(instanceField, null, null);
  }
}
