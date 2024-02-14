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
public class JobLaunchConf {
  private String type;
  private String className;
  private String proxyUser;
  @Builder.Default private Map<String, String> executionTags = new HashMap<>();
  @Builder.Default private List<String> args = new ArrayList<>();
  private String jarPath;
  @Builder.Default private List<String> dependencies = new ArrayList<>();
  @Builder.Default private Map<String, String> sparkProperties = new HashMap<>();
}
