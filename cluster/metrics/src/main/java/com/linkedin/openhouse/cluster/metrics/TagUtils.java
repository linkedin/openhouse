package com.linkedin.openhouse.cluster.metrics;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import io.micrometer.core.instrument.Tag;
import java.util.Arrays;

public final class TagUtils {

  public static final String COMMON_TAG_CLUSTER_NAME = "clusterName";
  public static final String COMMON_TAG_APP_NAME = "application";

  private TagUtils() {}

  public static Iterable<Tag> buildCommonTag(ClusterProperties clusterProperties, String appName) {
    final Tag[] tags = {
      Tag.of(COMMON_TAG_CLUSTER_NAME, clusterProperties.getClusterName()),
      Tag.of(COMMON_TAG_APP_NAME, appName)
    };
    return () -> Arrays.stream(tags).iterator();
  }
}
