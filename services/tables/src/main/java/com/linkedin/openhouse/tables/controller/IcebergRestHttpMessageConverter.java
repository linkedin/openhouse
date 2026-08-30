package com.linkedin.openhouse.tables.controller;

import com.linkedin.openhouse.tables.generated.iceberg.model.CatalogConfig;
import com.linkedin.openhouse.tables.generated.iceberg.model.ListTablesResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.rest.RESTResponse;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;

/**
 * Spring {@link org.springframework.http.converter.HttpMessageConverter} for generated and runtime
 * Iceberg response types. Uses {@link IcebergRestSerde} (Iceberg custom serializers, kebab-case) to
 * write JSON because runtime Iceberg types do not follow JavaBean conventions.
 *
 * <p>This converter only handles writes (responses). Deserialization is not supported because we do
 * not accept Iceberg REST request bodies through Spring MVC.
 */
public class IcebergRestHttpMessageConverter extends AbstractHttpMessageConverter<Object> {

  public IcebergRestHttpMessageConverter() {
    super(MediaType.APPLICATION_JSON);
  }

  @Override
  protected boolean supports(Class<?> clazz) {
    return RESTResponse.class.isAssignableFrom(clazz)
        || CatalogConfig.class.equals(clazz)
        || ListTablesResponse.class.equals(clazz);
  }

  @Override
  public boolean canRead(Class<?> clazz, MediaType mediaType) {
    return false;
  }

  @Override
  protected Object readInternal(Class<?> clazz, HttpInputMessage inputMessage) {
    throw new UnsupportedOperationException("Iceberg REST request deserialization not supported");
  }

  @Override
  protected void writeInternal(Object response, HttpOutputMessage outputMessage)
      throws IOException {
    OutputStream body = outputMessage.getBody();
    body.write(IcebergRestSerde.toJson(response).getBytes(StandardCharsets.UTF_8));
    body.flush();
  }
}
