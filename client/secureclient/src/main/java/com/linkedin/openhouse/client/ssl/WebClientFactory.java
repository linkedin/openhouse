package com.linkedin.openhouse.client.ssl;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.internal.StringUtil;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.UUID;
import javax.net.ssl.SSLException;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

/** Abstract Factory for creating Secure WebClient {@link WebClient}. */
@Slf4j
public abstract class WebClientFactory {

  private static final String HTTP = "http";
  private static final String HTTPS = "https";
  private static final String SESSION_ID = "session-id";

  private static final String HTTPHEADER_CLIENT_NAME = "X-Client-Name";
  private static final int IN_MEMORY_BUFFER_SIZE = 10 * 1000 * 1024;
  // The maximum number of connections per connection pool
  private static final int MAX_CONNECTION_POOL_SIZE = 500;
  // Max time to keep requests in pending queue before acquiring a connection
  private static final int MAX_PENDING_TIME_SECONDS = 60;
  // Max wait time for idle state before closing channel
  private static final int MAX_IDLE_TIME_SECONDS = 240;

  // Every 5 mins the connection pool is regularly checked for connections that are applicable for
  // removal
  private static final int EVICT_IN_BACKGROUND_TIME_SECONDS = 300;

  // Set the default HttpConnectionStrategy as POOLED connection
  private HttpConnectionStrategy strategy = HttpConnectionStrategy.POOLED;

  @Setter private String sessionId = null;

  @Setter private String clientName = null;

  protected WebClientFactory() {
    setStrategy();
  }

  /**
   * Creates WebClient
   *
   * @param baseUrl
   * @param token - Auth token in JWT (JSON Web Token) format
   * @param truststoreLocation - The truststore can be null for http url. For https url
   *     truststore/cacert location must be specified. This location can also be specified using
   *     environment variable i.e. TRUSTSTORE_LOCATION
   * @return WebClient
   * @throws MalformedURLException
   * @throws SSLException
   */
  protected WebClient createWebClient(
      @NonNull String baseUrl, String token, String truststoreLocation)
      throws MalformedURLException, SSLException {
    String transportProtocol = getTransportProtocol(baseUrl);
    HttpClient httpClient = null;
    if (HTTPS.equals(transportProtocol)) {
      httpClient = createSecureHttpClient(truststoreLocation);
    } else if (HTTP.equals(transportProtocol)) {
      httpClient = createHttpClient(baseUrl);
    } else {
      throw new RuntimeException("The transport protocol must be https/http");
    }
    return getWebClientBuilder(baseUrl)
        .clientConnector(new ReactorClientHttpConnector(httpClient))
        .build();
  }

  /**
   * Gets protocol from the URL
   *
   * @param baseUrl
   * @return String
   * @throws MalformedURLException
   */
  private String getTransportProtocol(String baseUrl) throws MalformedURLException {
    URL url = new URL(baseUrl);
    return url.getProtocol();
  }

  protected abstract WebClient.Builder createWebClientBuilder();

  /**
   * Method to create one way SSL context. The client accepts server cert by providing cacert or
   * truststore location
   *
   * @param truststoreLocation
   * @return SslContext
   */
  protected SslContext createSslContext(String truststoreLocation) {
    try {
      log.info("Creating client SslContext using truststore location: {}", truststoreLocation);
      return SslContextBuilder.forClient().trustManager(new File(truststoreLocation)).build();
    } catch (SSLException | IllegalArgumentException e) {
      throw new RuntimeException("Could not create SSL context", e);
    }
  }

  /**
   * Create WebClient with the base url
   *
   * @param baseUrl
   * @return WebClient
   */
  private HttpClient createHttpClient(String baseUrl) {
    HttpClient client = null;
    if (HttpConnectionStrategy.NEW.equals(strategy)) {
      log.info("Using new connection strategy");
      client = HttpClient.newConnection();
    } else {
      log.info("Using connection pool strategy");
      client = HttpClient.create(getCustomConnectionProvider());
    }
    return client;
  }

  /**
   * Creates one way SSL based WebClient. The client trusts the server certificate, but client
   * certificate not verified.
   *
   * @param truststoreLocation - location of the cacert/truststore. If not specified, get the
   *     location from environment variable TRUSTSTORE_LOCATION, else throws exception
   * @return WebClient
   * @throws SSLException
   */
  private HttpClient createSecureHttpClient(String truststoreLocation) {
    String truststore =
        StringUtil.isNullOrEmpty(truststoreLocation)
            ? System.getenv("TRUSTSTORE_LOCATION")
            : truststoreLocation;
    if (StringUtil.isNullOrEmpty(truststore)) {
      throw new IllegalArgumentException("Must specify cacert/turststore location");
    }
    HttpClient client = null;
    if (HttpConnectionStrategy.NEW.equals(strategy)) {
      log.info("Using new connection strategy");
      client = HttpClient.newConnection().secure(t -> t.sslContext(createSslContext(truststore)));
    } else {
      log.info("Using connection pool strategy");
      client =
          HttpClient.create(getCustomConnectionProvider())
              .secure(t -> t.sslContext(createSslContext(truststore)));
    }

    return client;
  }

  private WebClient.Builder getWebClientBuilder(String baseUrl) {
    WebClient.Builder webClientBuilder = createWebClientBuilder();
    setSessionIdInWebClientHeader(webClientBuilder);
    setClientNameInWebClientHeader(webClientBuilder);
    return webClientBuilder
        .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(IN_MEMORY_BUFFER_SIZE))
        .baseUrl(baseUrl);
  }

  /** Set strategy from environment variable if provided */
  private void setStrategy() {
    // Get the http connection strategy from environment
    String envStrategy = System.getenv("HTTP_CONNECTION_STRATEGY");
    // If http connection strategy is not specified default to POOLED HTTP connection
    strategy = HttpConnectionStrategy.fromString(envStrategy);
  }

  /**
   * Set HttpConnectionStrategy
   *
   * @param httpConnectionStrategy
   */
  public void setStrategy(HttpConnectionStrategy httpConnectionStrategy) {
    if (httpConnectionStrategy != null) {
      strategy = httpConnectionStrategy;
    }
  }
  /**
   * Set sessionId in the header of webClient. If a sessionId is not provided, generate UUID.
   *
   * @param webClientBuilder
   */
  private void setSessionIdInWebClientHeader(WebClient.Builder webClientBuilder) {
    if (sessionId == null) {
      sessionId = UUID.randomUUID().toString();
    }
    log.info("Client session id: {}", sessionId);
    webClientBuilder.defaultHeaders(h -> h.add(SESSION_ID, sessionId));
  }

  /**
   * Set clientName in the header of webClient. If a clientName is not provided, do nothing.
   *
   * @param webClientBuilder
   */
  private void setClientNameInWebClientHeader(WebClient.Builder webClientBuilder) {
    if (clientName != null) {
      webClientBuilder.defaultHeaders(h -> h.add(HTTPHEADER_CLIENT_NAME, clientName));
    }
    log.info("Client name: {}", clientName);
  }

  /**
   * Returns custom connection provider
   *
   * @return ConnectionProvider
   */
  private ConnectionProvider getCustomConnectionProvider() {
    log.info("Creating custom connection provider");
    return ConnectionProvider.builder("client-custom-connection-provider")
        .maxConnections(MAX_CONNECTION_POOL_SIZE)
        .maxIdleTime(Duration.ofSeconds(MAX_IDLE_TIME_SECONDS))
        .pendingAcquireTimeout(Duration.ofSeconds(MAX_PENDING_TIME_SECONDS))
        .evictInBackground(Duration.ofSeconds(EVICT_IN_BACKGROUND_TIME_SECONDS))
        .build();
  }
}
