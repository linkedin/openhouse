package com.linkedin.openhouse.jobs.mock;

import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.common.exception.JobEngineException;
import com.linkedin.openhouse.jobs.config.JobLaunchConf;
import com.linkedin.openhouse.jobs.model.EngineType;
import com.linkedin.openhouse.jobs.services.livy.LivyJobHandle;
import com.linkedin.openhouse.jobs.services.livy.LivyJobsCoordinator;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

public class LivyJobsCoordinatorTest {
  @Test
  void testSubmitSuccess() {
    final String testExecutionId = "test-id";
    final JsonObject responseBody = new JsonObject();
    final Map<String, String> executionTags = Maps.newHashMap();
    executionTags.put("pool", "dev");
    final JobLaunchConf conf =
        new JobLaunchConf(
            "test",
            "org.test.Test",
            "test_user",
            executionTags,
            Collections.emptyList(),
            "test.jar",
            Collections.emptyList(),
            Maps.newHashMap(),
            "livy");
    responseBody.addProperty("id", testExecutionId);
    ExchangeFunction exchangeFunction =
        request -> {
          Assertions.assertEquals(HttpMethod.POST, request.method());
          return Mono.just(
              ClientResponse.create(HttpStatus.CREATED).body(responseBody.toString()).build());
        };
    WebClient client = WebClient.builder().exchangeFunction(exchangeFunction).build();
    LivyJobsCoordinator coordinator = new LivyJobsCoordinator(client);
    LivyJobHandle handle = coordinator.submit(conf);
    Assertions.assertEquals(testExecutionId, handle.getExecutionId());
  }

  @Test
  void testSubmitFailure() {
    final JobLaunchConf conf =
        new JobLaunchConf(
            "test",
            "org.test.Test",
            "test_user",
            Maps.newHashMap(),
            Collections.emptyList(),
            "test.jar",
            Collections.emptyList(),
            Maps.newHashMap(),
            "livy");
    ExchangeFunction exchangeFunction =
        request -> {
          Assertions.assertEquals(HttpMethod.POST, request.method());
          return Mono.just(ClientResponse.create(HttpStatus.SERVICE_UNAVAILABLE).build());
        };
    WebClient client = WebClient.builder().exchangeFunction(exchangeFunction).build();
    LivyJobsCoordinator coordinator = new LivyJobsCoordinator(client);
    Assertions.assertThrows(
        JobEngineException.class,
        () -> {
          coordinator.submit(conf);
        });
  }
}
