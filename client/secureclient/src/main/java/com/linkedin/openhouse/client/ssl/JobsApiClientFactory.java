package com.linkedin.openhouse.client.ssl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.jobs.client.invoker.ApiClient;
import java.net.MalformedURLException;
import java.text.DateFormat;
import javax.net.ssl.SSLException;
import lombok.NonNull;
import org.springframework.web.reactive.function.client.WebClient;

/** Factory to create jobs specific ApiClient {@link ApiClient}. */
public final class JobsApiClientFactory extends WebClientFactory {

  private static JobsApiClientFactory instance;

  private JobsApiClientFactory() {
    super();
  }

  public static synchronized JobsApiClientFactory getInstance() {
    if (null == instance) {
      instance = new JobsApiClientFactory();
    }
    return instance;
  }

  /**
   * Creates default WebClient.Builder
   *
   * @return WebClient.Builder
   */
  @Override
  protected WebClient.Builder createWebClientBuilder() {
    DateFormat defaultDateFormat = ApiClient.createDefaultDateFormat();
    ObjectMapper defaultObjectMapper = ApiClient.createDefaultObjectMapper(defaultDateFormat);
    return ApiClient.buildWebClientBuilder(defaultObjectMapper);
  }

  /**
   * Creates ApiClient specific to jobs
   *
   * @param baseUrl
   * @param token - Auth token in JWT (JSON Web Token) format
   * @param truststoreLocation
   * @return ApiClient - jobs specific ApiClient
   * @throws MalformedURLException
   * @throws SSLException
   */
  public ApiClient createApiClient(@NonNull String baseUrl, String token, String truststoreLocation)
      throws MalformedURLException, SSLException {
    WebClient webClient = createWebClient(baseUrl, token, truststoreLocation);
    ApiClient apiClient = new ApiClient(webClient);
    if (token != null && !token.isEmpty()) {
      apiClient.addDefaultHeader(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", token));
    }
    apiClient.setBasePath(baseUrl);
    return apiClient;
  }
}
