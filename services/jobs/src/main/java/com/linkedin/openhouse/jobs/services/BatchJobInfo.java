package com.linkedin.openhouse.jobs.services;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.common.JobState;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class BatchJobInfo implements JobInfo {
  private final String executionId;
  private final String appId;
  private final Map<String, String> appInfo;
  private final JobState state;

  public static BatchJobInfo of(JsonObject data) {
    Map<String, String> appInfo = new HashMap<>();
    JsonObject appInfoJson = data.getAsJsonObject("appInfo");
    for (String fieldName : appInfoJson.keySet()) {
      JsonElement value = appInfoJson.get(fieldName);
      if (value.isJsonPrimitive()) {
        appInfo.put(fieldName, value.getAsString());
      }
    }
    JsonElement appId = data.get("appId");
    BatchJobInfoBuilder builder = new BatchJobInfoBuilder();
    builder =
        builder
            .executionId(data.getAsJsonPrimitive("id").getAsString())
            .state(parseState(data.getAsJsonPrimitive("state").getAsString()));
    if (appId.isJsonPrimitive()) {
      builder = builder.appId(appId.getAsString());
    }
    builder = builder.appInfo(appInfo);
    return builder.build();
  }

  private static JobState parseState(String state) {
    /*
    https://github.com/apache/incubator-livy/blob/4d8a912699683b973eee76d4e91447d769a0cb0d/core/src/main/scala/org/apache/livy/sessions/SessionState.scala#L30
    case "not_started" => NotStarted
    case "starting" => Starting
    case "recovering" => Recovering
    case "idle" => Idle
    case "running" => Running
    case "busy" => Busy
    case "shutting_down" => ShuttingDown
    case "error" => Error()
    case "dead" => Dead()
    case "killed" => Killed()
    case "success" => Success()
     */
    switch (state) {
      case "not_started":
        return JobState.QUEUED;
      case "error":
      case "dead":
      case "killed":
        return JobState.FAILED;
      case "success":
        return JobState.SUCCEEDED;
      default:
        return JobState.RUNNING;
    }
  }
}
