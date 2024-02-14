package com.linkedin.openhouse.common.config;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import org.springframework.beans.factory.annotation.Autowired;

public class BaseApplicationConfig {
  @Autowired protected ClusterProperties clusterProperties;
}
