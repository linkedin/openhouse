package com.linkedin.openhouse.tables.controller;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.rest.RESTResponse;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;

/**
 * Spring {@link org.springframework.http.converter.HttpMessageConverter} for Iceberg {@link
 * RESTResponse} types. Uses {@link IcebergRestSerde} (Iceberg custom serializers, kebab-case) to
 * write JSON because Iceberg REST types do not follow JavaBean conventions and cannot be serialized
 * by Spring's default Jackson converter.
 *
 * <p>This converter only handles writes (responses). Deserialization is not supported because we do
 * not accept Iceberg REST request bodies through Spring MVC.
 */
public class IcebergRestHttpMessageConverter extends AbstractHttpMessageConverter<RESTResponse> {

  public IcebergRestHttpMessageConverter() {
    super(MediaType.APPLICATION_JSON);
  }

  @Override
  protected boolean supports(Class<?> clazz) {
    return RESTResponse.class.isAssignableFrom(clazz);
  }

  @Override
  protected RESTResponse readInternal(
      Class<? extends RESTResponse> clazz, HttpInputMessage inputMessage) {
    throw new UnsupportedOperationException("Iceberg REST request deserialization not supported");
  }

  @Override
  protected void writeInternal(RESTResponse response, HttpOutputMessage outputMessage)
      throws IOException {
    OutputStream body = outputMessage.getBody();
    body.write(IcebergRestSerde.toJson(response).getBytes(StandardCharsets.UTF_8));
    body.flush();
  }
}
