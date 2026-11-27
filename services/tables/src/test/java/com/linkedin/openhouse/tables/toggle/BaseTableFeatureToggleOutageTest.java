package com.linkedin.openhouse.tables.toggle;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.common.exception.DependencyUnavailableException;
import com.linkedin.openhouse.tables.toggle.model.ToggleStatusKey;
import com.linkedin.openhouse.tables.toggle.repository.ToggleStatusesRepository;
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

/** A failed HouseTables call is never read as a toggle state; only outages are retryable. */
@ExtendWith(MockitoExtension.class)
public class BaseTableFeatureToggleOutageTest {

  private static final ToggleStatusKey KEY =
      ToggleStatusKey.builder().databaseId("db").tableId("tbl").featureId("feature").build();

  @Mock private ToggleStatusesRepository toggleStatusesRepository;

  @InjectMocks private BaseTableFeatureToggle toggle;

  @Test
  public void serverErrorIsDependencyUnavailable() {
    WebClientResponseException outage = response(503);
    when(toggleStatusesRepository.findById(KEY)).thenThrow(outage);

    DependencyUnavailableException thrown =
        assertThrows(DependencyUnavailableException.class, this::lookup);

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
    when(toggleStatusesRepository.findById(KEY)).thenThrow(unreachable);

    assertSame(
        unreachable, assertThrows(DependencyUnavailableException.class, this::lookup).getCause());
  }

  /** A 4xx means OpenHouse made a bad call: retrying cannot help, so it is not an outage. */
  @Test
  public void clientErrorPropagatesAsItself() {
    WebClientResponseException badCall = response(400);
    when(toggleStatusesRepository.findById(KEY)).thenThrow(badCall);

    assertSame(badCall, assertThrows(WebClientResponseException.class, this::lookup));
  }

  private boolean lookup() {
    return toggle.isFeatureActivated("db", "tbl", "feature");
  }

  private static WebClientResponseException response(int status) {
    return WebClientResponseException.create(
        status, "status " + status, HttpHeaders.EMPTY, null, null);
  }
}
