package com.linkedin.openhouse.client.ssl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;

import com.linkedin.openhouse.jobs.client.invoker.ApiClient;
import io.netty.handler.ssl.SslContext;
import java.io.File;
import java.net.MalformedURLException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class JobsApiClientFactoryTest {

  @Test
  public void testInvalidTransportProtocol() {
    assertThrows(
        MalformedURLException.class,
        () ->
            JobsApiClientFactory.getInstance()
                .createApiClient("httpp://test.openhouse.com", "", ""));
  }

  @Test
  public void testInvalidUrl() {
    assertThrows(
        RuntimeException.class,
        () ->
            JobsApiClientFactory.getInstance().createApiClient("ftp://test.openhouse.com", "", ""));
  }

  @Test
  public void testTruststoreLocationNotPresent() throws Exception {
    assertThrows(
        RuntimeException.class,
        () ->
            JobsApiClientFactory.getInstance()
                .createApiClient("https://test.openhouse.com", "", ""));
  }

  @Test
  public void testInvalidTruststoreLocation() throws Exception {
    assertThrows(
        RuntimeException.class,
        () ->
            JobsApiClientFactory.getInstance()
                .createApiClient("https://test.openhouse.com", "location", ""));
  }

  @Test
  public void testWebClientForHttpUrl() throws Exception {
    File tmpCert = File.createTempFile("tmpcacert", ".crt");
    tmpCert.deleteOnExit();
    ApiClient apiClient =
        JobsApiClientFactory.getInstance()
            .createApiClient("http://test.openhouse.com", "", tmpCert.getAbsolutePath());
    assertNotNull(apiClient);
  }

  @Test
  public void testWebClientForHttpsUrl() throws Exception {
    SslContext mockSslContext = Mockito.mock(SslContext.class);
    File tmpCert = File.createTempFile("tmpcacert", ".crt");
    tmpCert.deleteOnExit();
    ArgumentCaptor<String> truststoreLocationCapture = ArgumentCaptor.forClass(String.class);
    JobsApiClientFactory jobsApiClientFactorySpy = Mockito.spy(JobsApiClientFactory.getInstance());
    Mockito.doReturn(mockSslContext).when(jobsApiClientFactorySpy).createSslContext(anyString());

    ApiClient apiClient =
        jobsApiClientFactorySpy.createApiClient(
            "https://test.openhouse.com", "", tmpCert.getAbsolutePath());
    Mockito.verify(jobsApiClientFactorySpy, Mockito.times(1))
        .createSslContext(truststoreLocationCapture.capture());
    assertNotNull(apiClient);
    assertEquals(tmpCert.getAbsolutePath(), truststoreLocationCapture.getValue());
  }
}
