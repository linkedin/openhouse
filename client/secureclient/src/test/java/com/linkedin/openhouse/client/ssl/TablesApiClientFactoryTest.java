package com.linkedin.openhouse.client.ssl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;

import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import io.netty.handler.ssl.SslContext;
import java.io.File;
import java.net.MalformedURLException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TablesApiClientFactoryTest {

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
    File tmpCert = File.createTempFile("tmpcacert", ".crt");
    tmpCert.deleteOnExit();
    ApiClient apiClient =
        TablesApiClientFactory.getInstance()
            .createApiClient("http://test.openhouse.com", "", tmpCert.getAbsolutePath());
    assertNotNull(apiClient);
  }

  @Test
  public void testWebClientForHttpsUrl() throws Exception {
    SslContext mockSslContext = Mockito.mock(SslContext.class);
    File tmpCert = File.createTempFile("tmpcacert", ".crt");
    tmpCert.deleteOnExit();
    ArgumentCaptor<String> truststoreLocationCapture = ArgumentCaptor.forClass(String.class);
    TablesApiClientFactory tablesApiClientFactorySpy =
        Mockito.spy(TablesApiClientFactory.getInstance());
    Mockito.doReturn(mockSslContext).when(tablesApiClientFactorySpy).createSslContext(anyString());

    ApiClient apiClient =
        tablesApiClientFactorySpy.createApiClient(
            "https://test.openhouse.com", "", tmpCert.getAbsolutePath());
    Mockito.verify(tablesApiClientFactorySpy, Mockito.times(1))
        .createSslContext(truststoreLocationCapture.capture());
    assertNotNull(apiClient);
    assertEquals(tmpCert.getAbsolutePath(), truststoreLocationCapture.getValue());
  }
}
