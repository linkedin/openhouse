package com.linkedin.openhouse.cluster.configs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.support.DefaultPropertySourceFactory;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.lang.Nullable;

/**
 * Like documented here,
 * https://www.baeldung.com/spring-yaml-propertysource#custom-propertysourcefactory To be able to
 * convert a YAML file into java.util.Properties object we use YamlPropertiesFactoryBean.
 */
public class YamlPropertySourceFactory extends DefaultPropertySourceFactory {

  /**
   * There is a open issue with using YamlPropertySourceFactory,
   * https://github.com/spring-projects/spring-framework/issues/22276 with ignoreResourceNotFound =
   * true and YAML file path does not exists. Referenced workaround from Blog
   * https://deinum.biz/2018-07-04-PropertySource-with-yaml-files/
   *
   * @param name
   * @param resource
   * @return
   * @throws IOException
   */
  @Override
  public PropertySource<?> createPropertySource(@Nullable String name, EncodedResource resource)
      throws IOException {
    Properties propertiesFromYaml = loadYamlIntoProperties(resource);
    String sourceName = name != null ? name : resource.getResource().getFilename();
    if (sourceName == null || propertiesFromYaml == null) {
      throw new FileNotFoundException();
    }
    return new PropertiesPropertySource(sourceName, propertiesFromYaml);
  }

  private Properties loadYamlIntoProperties(EncodedResource resource) throws FileNotFoundException {
    try {
      YamlPropertiesFactoryBean factory = new YamlPropertiesFactoryBean();
      factory.setResources(resource.getResource());
      factory.afterPropertiesSet();
      return factory.getObject();
    } catch (IllegalStateException e) {
      // for ignoreResourceNotFound
      Throwable cause = e.getCause();
      if (cause instanceof FileNotFoundException) {
        throw (FileNotFoundException) e.getCause();
      }
      throw e;
    }
  }
}
