package com.linkedin.openhouse.jobs.mock;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.common.exception.JobEngineException;
import com.linkedin.openhouse.jobs.services.JobInfo;
import com.linkedin.openhouse.jobs.services.livy.LivyJobHandle;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

public class LivyJobHandleTest {
  @Test
  void testCancelSuccess() {
    final String testExecutionId = "test-id";
    ExchangeFunction exchangeFunction =
        request -> {
          Assertions.assertEquals(HttpMethod.DELETE, request.method());
          String[] parts = request.url().getPath().split("/");
          Assertions.assertEquals(testExecutionId, parts[parts.length - 1]);
          return Mono.just(ClientResponse.create(HttpStatus.OK).build());
        };
    WebClient client = WebClient.builder().exchangeFunction(exchangeFunction).build();
    LivyJobHandle handle = new LivyJobHandle(client, testExecutionId);
    handle.cancel();
  }

  @Test
  void testCancelFailure() {
    final String testExecutionId = "test-id";
    ExchangeFunction exchangeFunction =
        request -> {
          Assertions.assertEquals(HttpMethod.DELETE, request.method());
          String[] parts = request.url().getPath().split("/");
          Assertions.assertEquals(testExecutionId, parts[parts.length - 1]);
          return Mono.just(ClientResponse.create(HttpStatus.SERVICE_UNAVAILABLE).build());
        };
    WebClient client = WebClient.builder().exchangeFunction(exchangeFunction).build();
    LivyJobHandle handle = new LivyJobHandle(client, testExecutionId);
    Assertions.assertThrows(JobEngineException.class, handle::cancel);
  }

  @Test
  void testGetDataSuccess() {
    final String testExecutionId = "test-id";
    JsonObject responseBody = new JsonObject();
    responseBody.addProperty("id", testExecutionId);
    responseBody.addProperty("appId", "application_id");
    List<String> logs = Arrays.asList("log1", "log2");
    responseBody.add("log", new Gson().toJsonTree(logs).getAsJsonArray());
    responseBody.addProperty("state", "running");
    JsonObject appInfo = new JsonObject();
    appInfo.addProperty("appUrl", "test_url1");
    appInfo.addProperty("logUrl", "test_url2");
    responseBody.add("appInfo", appInfo);
    ExchangeFunction exchangeFunction =
        request -> {
          Assertions.assertEquals(HttpMethod.GET, request.method());
          String[] parts = request.url().getPath().split("/");
          Assertions.assertEquals(testExecutionId, parts[parts.length - 1]);
          return Mono.just(
              ClientResponse.create(HttpStatus.OK).body(responseBody.toString()).build());
        };
    WebClient client = WebClient.builder().exchangeFunction(exchangeFunction).build();
    LivyJobHandle handle = new LivyJobHandle(client, testExecutionId);
    JobInfo info = handle.getInfo();
    Assertions.assertEquals(
        responseBody.getAsJsonPrimitive("id").getAsString(), info.getExecutionId());
    Assertions.assertEquals(
        responseBody.getAsJsonPrimitive("appId").getAsString(), info.getAppId());
    // running maps to ACTIVE
    Assertions.assertEquals(JobState.RUNNING, info.getState());
  }

  @Test
  void testGetDataFailure() {
    ExchangeFunction exchangeFunction =
        request -> Mono.just(ClientResponse.create(HttpStatus.SERVICE_UNAVAILABLE).build());
    WebClient client = WebClient.builder().exchangeFunction(exchangeFunction).build();
    LivyJobHandle handle = new LivyJobHandle(client, "test-id");
    Assertions.assertThrows(JobEngineException.class, handle::getInfo);
  }
}
