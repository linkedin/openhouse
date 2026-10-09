package com.linkedin.openhouse.tables.toggle.repository;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.common.exception.DependencyUnavailableException;
import com.linkedin.openhouse.housetables.client.api.ToggleStatusApi;
import com.linkedin.openhouse.tables.toggle.model.ToggleStatusKey;
import java.net.ConnectException;
import java.net.URI;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/** A failed HouseTables call is never read as a toggle state; only outages are retryable. */
@ExtendWith(MockitoExtension.class)
public class ToggleStatusesRepositoryImplTest {

  private static final ToggleStatusKey KEY =
      ToggleStatusKey.builder().databaseId("db").tableId("tbl").featureId("feature").build();

  @Mock private ToggleStatusApi apiInstance;

  @InjectMocks private ToggleStatusesRepositoryImpl repository;

  @Test
  public void serverErrorIsDependencyUnavailable() {
    WebClientResponseException outage = response(503);
    failWith(outage);

    DependencyUnavailableException thrown =
        assertThrows(DependencyUnavailableException.class, () -> repository.findById(KEY));

    assertSame(outage, thrown.getCause());
    assertTrue(thrown.getMessage().contains("db.tbl"));
  }

  @Test
  public void unreachableIsDependencyUnavailable() {
    WebClientRequestException unreachable =
        new WebClientRequestException(
            new ConnectException("connection refused"),
            HttpMethod.GET,
            URI.create("http://housetables/v1/toggle"),
            HttpHeaders.EMPTY);
    failWith(unreachable);

    assertSame(
        unreachable,
        assertThrows(DependencyUnavailableException.class, () -> repository.findById(KEY))
            .getCause());
  }

  /** A 4xx means OpenHouse made a bad call: retrying cannot help, so it is not an outage. */
  @Test
  public void clientErrorPropagatesAsItself() {
    WebClientResponseException badCall = response(400);
    failWith(badCall);

    assertSame(
        badCall, assertThrows(WebClientResponseException.class, () -> repository.findById(KEY)));
  }

  private void failWith(Exception failure) {
    when(apiInstance.getTableToggleStatus("db", "tbl", "feature")).thenReturn(Mono.error(failure));
  }

  private static WebClientResponseException response(int status) {
    return WebClientResponseException.create(
        status, "status " + status, HttpHeaders.EMPTY, null, null);
  }
}
