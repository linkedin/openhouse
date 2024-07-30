package com.linkedin.openhouse.client.ssl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;

import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import io.netty.handler.ssl.SslContext;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TablesApiClientFactoryTest {

  static File tmpCert;

  static TablesApiClientFactory tablesApiClientFactorySpy;

  @BeforeAll
  public static void startUp() throws IOException {
    SslContext mockSslContext = Mockito.mock(SslContext.class);
    tmpCert = File.createTempFile("tmpcacert", ".crt");
    tablesApiClientFactorySpy = Mockito.spy(TablesApiClientFactory.getInstance());
    Mockito.doReturn(mockSslContext).when(tablesApiClientFactorySpy).createSslContext(anyString());
  }

  @AfterAll
  public static void tearDown() {
    tmpCert.deleteOnExit();
    ;
  }

  @Test
  public void testInvalidTransportProtocol() {
    assertThrows(
        MalformedURLException.class,
        () ->
            TablesApiClientFactory.getInstance()
                .createApiClient("httpp://test.openhouse.com", "", ""));
  }

  @Test
  public void testInvalidUrl() {
    assertThrows(
        RuntimeException.class,
        () ->
            TablesApiClientFactory.getInstance()
                .createApiClient("ftp://test.openhouse.com", "", ""));
  }

  @Test
  public void testTruststoreLocationNotPresent() throws Exception {
    assertThrows(
        RuntimeException.class,
        () ->
            TablesApiClientFactory.getInstance()
                .createApiClient("https://test.openhouse.com", "", ""));
  }

  @Test
  public void testInvalidTruststoreLocation() throws Exception {
    assertThrows(
        RuntimeException.class,
        () ->
            TablesApiClientFactory.getInstance()
                .createApiClient("https://test.openhouse.com", "", "location"));
  }

  @Test
  public void testWebClientForHttpUrl() throws Exception {
    ApiClient apiClient =
        TablesApiClientFactory.getInstance()
            .createApiClient("http://test.openhouse.com", "", tmpCert.getAbsolutePath());
    assertNotNull(apiClient);
  }

  @Test
  public void testWebClientForHttpsUrl() throws Exception {
    ArgumentCaptor<String> truststoreLocationCapture = ArgumentCaptor.forClass(String.class);

    ApiClient apiClient =
        tablesApiClientFactorySpy.createApiClient(
            "https://test.openhouse.com", "", tmpCert.getAbsolutePath());

    Mockito.verify(tablesApiClientFactorySpy, Mockito.times(1))
        .createSslContext(truststoreLocationCapture.capture());
    assertNotNull(apiClient);
    assertEquals(tmpCert.getAbsolutePath(), truststoreLocationCapture.getValue());
  }

  @Test
  public void testSetClientNameCalled() throws Exception {
    ArgumentCaptor<String> clientNameCapture = ArgumentCaptor.forClass(String.class);

    tablesApiClientFactorySpy.setClientName("trino");
    tablesApiClientFactorySpy.createApiClient(
        "https://test.openhouse.com", "", tmpCert.getAbsolutePath());
    Mockito.verify(tablesApiClientFactorySpy, Mockito.times(1))
        .setClientName(clientNameCapture.capture());
    assertEquals("trino", clientNameCapture.getValue());
  }
}
