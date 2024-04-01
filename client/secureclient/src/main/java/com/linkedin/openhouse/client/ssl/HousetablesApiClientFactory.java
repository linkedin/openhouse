package com.linkedin.openhouse.client.ssl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import java.net.MalformedURLException;
import java.text.DateFormat;
import javax.net.ssl.SSLException;
import lombok.NonNull;
import org.springframework.web.reactive.function.client.WebClient;

/** Factory to create housetables specific ApiClient {@link ApiClient}. */
public final class HousetablesApiClientFactory extends WebClientFactory {

  private static HousetablesApiClientFactory instance;

  private HousetablesApiClientFactory() {
    super();
  }

  public static synchronized HousetablesApiClientFactory getInstance() {
    if (null == instance) {
      instance = new HousetablesApiClientFactory();
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
   * Creates ApiClient specific to housetables
   *
   * @param baseUrl
   * @param token - Auth token in JWT (JSON Web Token) format
   * @param truststoreLocation
   * @return ApiClient - tables specific ApiClient
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
