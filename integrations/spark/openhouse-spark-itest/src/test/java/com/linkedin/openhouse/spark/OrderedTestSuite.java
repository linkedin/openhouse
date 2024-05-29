package com.linkedin.openhouse.spark;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.runner.RunWith;

/**
 * Multiple SparkSessions are created in the test suite, so we need to run the tests in order to
 * avoid conflicts.
 */
@RunWith(JUnitPlatform.class)
@SelectPackages({
  "com.linkedin.openhouse.spark.catalogtest",
  "com.linkedin.openhouse.spark.statementtest",
  "com.linkedin.openhouse"
})
public class OrderedTestSuite {}
