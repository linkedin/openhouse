package com.linkedin.openhouse.tables.config;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import java.text.ParseException;
import org.codehaus.jettison.json.JSONException;
import org.springframework.boot.test.autoconfigure.web.servlet.MockMvcBuilderCustomizer;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.web.servlet.setup.ConfigurableMockMvcBuilder;

@Configuration
public class MockMvcBuilderConfig implements MockMvcBuilderCustomizer {

  @Override
  public void customize(ConfigurableMockMvcBuilder<?> builder) {
    DummyTokenInterceptor.DummySecurityJWT dummySecurityJWT =
        new DummyTokenInterceptor.DummySecurityJWT("testUser");
    try {
      String jwtAccessToken = dummySecurityJWT.buildNoopJWT();
      builder.defaultRequest(
          get("/") // the URI does not matter
              .header("Authorization", "Bearer " + jwtAccessToken));
    } catch (ParseException | JSONException e) {
      throw new RuntimeException(e);
    }
  }
}
