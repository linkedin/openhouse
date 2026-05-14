package com.linkedin.openhouse.tables.controller;

import java.util.List;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Registers the {@link IcebergRestHttpMessageConverter} so that Spring MVC can serialize typed
 * Iceberg REST responses ({@code ConfigResponse}, {@code LoadTableResponse}, etc.) returned by
 * {@link IcebergRestCatalogController}.
 */
@Configuration
public class IcebergRestSerdeConfig implements WebMvcConfigurer {

  @Override
  public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
    converters.add(0, new IcebergRestHttpMessageConverter());
  }
}
