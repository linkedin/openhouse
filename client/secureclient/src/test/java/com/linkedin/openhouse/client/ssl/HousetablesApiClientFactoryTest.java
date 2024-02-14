package com.linkedin.openhouse.client.ssl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;

import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import io.netty.handler.ssl.SslContext;
import java.io.File;
import java.net.MalformedURLException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class HousetablesApiClientFactoryTest {

  @Test
  public void testInvalidTransportProtocol() {
    assertThrows(
        MalformedURLException.class,
        () ->
            HousetablesApiClientFactory.getInstance()
                .createApiClient("httpps://test.openhouse.com", "", ""));
  }

  @Test
  public void testInvalidUrl() {
    assertThrows(
        RuntimeException.class,
        () ->
            HousetablesApiClientFactory.getInstance()
                .createApiClient("ftp://test.openhouse.com", "", ""));
  }

  @Test
  public void testTruststoreLocationNotPresent() throws Exception {
    assertThrows(
        RuntimeException.class,
        () ->
            HousetablesApiClientFactory.getInstance()
                .createApiClient("https://test.openhouse.com", "", ""));
  }

  @Test
  public void testInvalidTruststoreLocation() throws Exception {
    assertThrows(
        RuntimeException.class,
        () ->
            HousetablesApiClientFactory.getInstance()
                .createApiClient("https://test.openhouse.com", "", "location"));
  }

  @Test
  public void testWebClientForHttpUrl() throws Exception {
    File tmpCert = File.createTempFile("tmpcacert", ".crt");
    tmpCert.deleteOnExit();
    ApiClient apiClient =
        HousetablesApiClientFactory.getInstance()
            .createApiClient("http://test.openhouse.com", "", tmpCert.getAbsolutePath());
    assertNotNull(apiClient);
  }

  @Test
  public void testWebClientForHttpsUrl() throws Exception {
    SslContext mockSslContext = Mockito.mock(SslContext.class);
    File tmpCert = File.createTempFile("tmpcacert", ".crt");
    tmpCert.deleteOnExit();
    ArgumentCaptor<String> truststoreLocationCapture = ArgumentCaptor.forClass(String.class);
    HousetablesApiClientFactory housetablesApiClientFactorySpy =
        Mockito.spy(HousetablesApiClientFactory.getInstance());
    Mockito.doReturn(mockSslContext)
        .when(housetablesApiClientFactorySpy)
        .createSslContext(anyString());

    ApiClient apiClient =
        housetablesApiClientFactorySpy.createApiClient(
            "https://test.openhouse.com", "", tmpCert.getAbsolutePath());
    Mockito.verify(housetablesApiClientFactorySpy, Mockito.times(1))
        .createSslContext(truststoreLocationCapture.capture());
    assertNotNull(apiClient);
    assertEquals(tmpCert.getAbsolutePath(), truststoreLocationCapture.getValue());
  }
}
