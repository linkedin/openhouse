package com.linkedin.openhouse.spark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;

/**
 * A copy of com.linkedin.openhouse.common.test.schema.ResourceIoHelper to avoid this client module
 * to depend on server module's code. The implementation of this helper should be straightforward
 * and tends to be unchanged.
 */
public class ResourceIoHelper {
  private ResourceIoHelper() {
    // utility constructor noop
  }

  public static String getSchemaJsonFromResource(String resourceName) throws IOException {
    return getSchemaJsonFromResource(ResourceIoHelper.class, resourceName);
  }

  public static String getSchemaJsonFromResource(Class klazz, String resourceName)
      throws IOException {
    InputStream inputStream = klazz.getClassLoader().getResourceAsStream(resourceName);
    return IOUtils.toString(inputStream, StandardCharsets.UTF_8.name());
  }
}
