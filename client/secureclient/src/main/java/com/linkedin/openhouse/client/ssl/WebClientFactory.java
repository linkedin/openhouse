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
import org.springframework.http.HttpHeaders;
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

  public static final String HTTP_HEADER_CLIENT_NAME = "X-Client-Name";
  // Product token advertised in the User-Agent header so the server can observe the client version.
  // The resulting header looks like "openhouse-java-client/1.5.2".
  public static final String USER_AGENT_CLIENT_PRODUCT = "openhouse-java-client";
  // Fallback when the client jar manifest carries no Implementation-Version (e.g. running from
  // classes during local/dev/tests) and no explicit version was set.
  private static final String CLIENT_VERSION_UNKNOWN = "unknown";
  private static final int IN_MEMORY_BUFFER_SIZE = 40 * 1024 * 1024;
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

  // When null, the version is resolved from the client jar manifest's Implementation-Version,
  // falling back to {@link #CLIENT_VERSION_UNKNOWN}. Callers may set an explicit value to override.
  @Setter private String clientVersion = null;

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
    return getWebClient(baseUrl, httpClient);
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

  private WebClient getWebClient(String baseUrl, HttpClient httpClient) {
    WebClient.Builder webClientBuilder = createWebClientBuilder();
    setSessionIdInWebClientHeader(webClientBuilder);
    setClientNameInWebClientHeader(webClientBuilder);
    setUserAgentInWebClientHeader(webClientBuilder);
    return webClientBuilder
        .baseUrl(baseUrl)
        .clientConnector(new ReactorClientHttpConnector(httpClient))
        .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(IN_MEMORY_BUFFER_SIZE))
        .build();
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
      webClientBuilder.defaultHeaders(h -> h.add(HTTP_HEADER_CLIENT_NAME, clientName));
    }
    log.info("Client name: {}", clientName);
  }

  /**
   * Set the User-Agent header on the webClient so the server can observe the client version on
   * every request, e.g. "openhouse-java-client/1.5.2". The version is resolved via {@link
   * #resolveClientVersion()}. Setting our own default User-Agent replaces the transport's default
   * (e.g. "ReactorNetty/x.y.z").
   *
   * @param webClientBuilder
   */
  private void setUserAgentInWebClientHeader(WebClient.Builder webClientBuilder) {
    String userAgent = USER_AGENT_CLIENT_PRODUCT + "/" + resolveClientVersion();
    webClientBuilder.defaultHeaders(h -> h.set(HttpHeaders.USER_AGENT, userAgent));
    log.info("Client User-Agent: {}", userAgent);
  }

  /**
   * Resolve the client version to advertise: an explicitly set {@link #clientVersion} takes
   * precedence, otherwise the Implementation-Version stamped into this jar's manifest, otherwise
   * {@link #CLIENT_VERSION_UNKNOWN}.
   *
   * @return the resolved client version, never null
   */
  private String resolveClientVersion() {
    if (!StringUtil.isNullOrEmpty(clientVersion)) {
      return clientVersion;
    }
    String implementationVersion = WebClientFactory.class.getPackage().getImplementationVersion();
    return StringUtil.isNullOrEmpty(implementationVersion)
        ? CLIENT_VERSION_UNKNOWN
        : implementationVersion;
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
