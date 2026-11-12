package com.linkedin.openhouse.client.ssl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;

import com.linkedin.openhouse.tables.client.api.NamespaceApi;
import com.linkedin.openhouse.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.NamespaceResponse;
import com.sun.net.httpserver.HttpServer;
import io.netty.handler.ssl.SslContext;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.core.codec.DecodingException;
import org.springframework.http.MediaType;

public class TablesApiClientFactoryTest {

  static File tmpCert;

  static TablesApiClientFactory tablesApiClientFactorySpy;

  @BeforeAll
  public static void startUp() throws IOException {
    SslContext mockSslContext = Mockito.mock(SslContext.class);
    tmpCert = File.createTempFile("tmpcacert", ".crt");
    tmpCert.deleteOnExit();
    tablesApiClientFactorySpy = Mockito.spy(TablesApiClientFactory.getInstance());
    Mockito.doReturn(mockSslContext).when(tablesApiClientFactorySpy).createSslContext(anyString());
  }

  @Test
  public void namespaceDecoderRetainsOptionalProperties() throws Exception {
    NamespaceResponse response =
        withResponse(
            "{\"namespace\":[\"Foo\"]}",
            client -> new NamespaceApi(client).loadNamespaceMetadataV1("catalog", "foo").block());
    assertEquals(Collections.singletonList("Foo"), response.getNamespace());
    assertNull(response.getProperties());
  }

  @Test
  public void namespaceDecoderRejectsNumericStrings() {
    assertThrows(
        DecodingException.class,
        () ->
            withResponse(
                "{\"namespace\":[\"foo\"],\"properties\":{\"a\":1}}",
                client ->
                    new NamespaceApi(client).loadNamespaceMetadataV1("catalog", "foo").block()));
  }

  @Test
  public void tableDecoderRetainsNumericStringCoercion() throws Exception {
    GetTableResponseBody response =
        withResponse(
            "{\"tableId\":1}",
            client ->
                client
                    .getWebClient()
                    .get()
                    .uri("/table")
                    .retrieve()
                    .bodyToMono(GetTableResponseBody.class)
                    .block());
    assertEquals("1", response.getTableId());
  }

  private <T> T withResponse(String body, Function<ApiClient, T> assertion) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          byte[] content = body.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", MediaType.APPLICATION_JSON_VALUE);
          exchange.sendResponseHeaders(200, content.length);
          try {
            exchange.getResponseBody().write(content);
          } finally {
            exchange.close();
          }
        });
    server.start();
    try {
      return assertion.apply(
          TablesApiClientFactory.getInstance()
              .createApiClient("http://127.0.0.1:" + server.getAddress().getPort(), "", ""));
    } finally {
      server.stop(0);
    }
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

  @Test
  public void testSetClientVersionCalled() throws Exception {
    ArgumentCaptor<String> clientVersionCapture = ArgumentCaptor.forClass(String.class);

    tablesApiClientFactorySpy.setClientVersion("1.2.3");
    tablesApiClientFactorySpy.createApiClient(
        "https://test.openhouse.com", "", tmpCert.getAbsolutePath());
    Mockito.verify(tablesApiClientFactorySpy, Mockito.times(1))
        .setClientVersion(clientVersionCapture.capture());
    assertEquals("1.2.3", clientVersionCapture.getValue());
  }
}
