package com.linkedin.openhouse.jobs.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Builder(toBuilder = true)
@EqualsAndHashCode
public class JobsEngineProperties {
  private String type;
  private String coordinatorClassName;
  private String engineUri;
  private String jarPath;
  private List<String> dependencies = new ArrayList<>();
  private Map<String, String> executionTags = new HashMap<>();
}
