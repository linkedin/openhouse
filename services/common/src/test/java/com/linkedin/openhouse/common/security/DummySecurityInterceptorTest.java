package com.linkedin.openhouse.common.security;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import io.jsonwebtoken.Claims;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import org.codehaus.jettison.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DummySecurityInterceptorTest {

  // If this test is broken our dummy security interceptor used for validating JWT tokens is
  // broken in local environments. Update dummy.token in case you are changing internal dummy
  // parameters.
  @Test
  public void testDummySecurityJwt()
      throws NoSuchAlgorithmException, InvalidKeySpecException, ParseException, JSONException {
    DummyTokenInterceptor.DummySecurityJWT dummySecurityJWT =
        new DummyTokenInterceptor.DummySecurityJWT("DUMMY_ANONYMOUS_USER");
    String jwtStr = dummySecurityJWT.buildNoopJWT();
    Claims claims = dummySecurityJWT.decodeJWT(jwtStr);
    Assertions.assertTrue(claims.getId().startsWith("DUMMY_ANONYMOUS_USER"));
    assertThat(
        claims.getSubject(),
        anyOf(
            equalTo("{\"USER-ID\":\"DUMMY_ANONYMOUS_USER\",\"CODE\":\"DUMMY_CODE\"}"),
            equalTo("{\"CODE\":\"DUMMY_CODE\",\"USER-ID\":\"DUMMY_ANONYMOUS_USER\"}")));
  }
}
