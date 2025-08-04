package com.linkedin.openhouse.tablestest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.util.SocketUtils;

public class OpenHouseLocalServerTest {

  @Test
  public void testServerStart() {
    OpenHouseLocalServer openHouseLocalServer = new OpenHouseLocalServer();
    Assertions.assertDoesNotThrow(() -> openHouseLocalServer.start());
    Assertions.assertNotEquals(openHouseLocalServer.getPort(), 0);
    Assertions.assertDoesNotThrow(openHouseLocalServer::stop);
  }

  @Test
  public void testServerStartCustomPortNo() {
    // Use a specific high port range to avoid conflicts with other test infrastructure
    // Try multiple ports in case the first one is occupied
    int portNo = SocketUtils.findAvailableTcpPort(59000, 59100);
    OpenHouseLocalServer openHouseLocalServer = new OpenHouseLocalServer(portNo);
    Assertions.assertDoesNotThrow(() -> openHouseLocalServer.start());
    Assertions.assertEquals(openHouseLocalServer.getPort(), portNo);
    Assertions.assertDoesNotThrow(openHouseLocalServer::stop);
  }

  @Test
  public void testMultipleServers() {
    OpenHouseLocalServer openHouseLocalServer1 = new OpenHouseLocalServer();
    OpenHouseLocalServer openHouseLocalServer2 = new OpenHouseLocalServer();
    Assertions.assertDoesNotThrow(() -> openHouseLocalServer1.start());
    Assertions.assertDoesNotThrow(() -> openHouseLocalServer2.start());
    Assertions.assertNotEquals(openHouseLocalServer1.getPort(), 0);
    Assertions.assertNotEquals(openHouseLocalServer2.getPort(), 0);
    Assertions.assertNotEquals(openHouseLocalServer1.getPort(), openHouseLocalServer2.getPort());
    Assertions.assertDoesNotThrow(openHouseLocalServer1::stop);
    Assertions.assertDoesNotThrow(openHouseLocalServer2::stop);
  }

  @Test
  public void testStoppingUnstartedServer() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new OpenHouseLocalServer().stop());

    OpenHouseLocalServer openHouseLocalServer1 = new OpenHouseLocalServer();
    Assertions.assertDoesNotThrow(() -> openHouseLocalServer1.start());
    Assertions.assertDoesNotThrow(openHouseLocalServer1::stop);
    Assertions.assertThrows(IllegalArgumentException.class, openHouseLocalServer1::stop);
  }

  @Test
  public void testStartingStoppedServer() {
    OpenHouseLocalServer openHouseLocalServer1 = new OpenHouseLocalServer();
    Assertions.assertDoesNotThrow(() -> openHouseLocalServer1.start());
    Assertions.assertDoesNotThrow(openHouseLocalServer1::stop);
    Assertions.assertDoesNotThrow(() -> openHouseLocalServer1.start());
  }

  @Test
  public void testStartingStartedServer() {
    OpenHouseLocalServer openHouseLocalServer1 = new OpenHouseLocalServer();
    Assertions.assertDoesNotThrow(() -> openHouseLocalServer1.start());
    Assertions.assertThrows(IllegalArgumentException.class, openHouseLocalServer1::start);
  }
}
