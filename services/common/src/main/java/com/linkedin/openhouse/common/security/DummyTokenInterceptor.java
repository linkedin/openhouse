package com.linkedin.openhouse.common.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureAlgorithm;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.DatatypeConverter;
import lombok.AllArgsConstructor;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * A dummy access token interceptor which is used in only unit-tests and local docker-environments.
 */
public class DummyTokenInterceptor implements HandlerInterceptor {

  private static final String TOKEN_TYPE = "Bearer";
  private static final String AUTHORIZATION_HEADER = "Authorization";

  /**
   * Called before every HTTP Request enters the main request processing logic in the Controller. On
   * successful access token validation, we extract the principal and set it in the request as an
   * attribute. This information is later leveraged in checking if the principal has permission to
   * access the given resource. If unsuccessful, we promptly return error on the {@link
   * HttpServletResponse} parameter
   *
   * @param request current HTTP request
   * @param response current HTTP response
   * @param handler chosen handler to execute, for type and/or instance evaluation
   * @return true if the execution chain should proceed with the next interceptor or the handler
   *     itself, false otherwise.
   */
  public boolean preHandle(
      HttpServletRequest request, HttpServletResponse response, Object handler) {
    String authorizationHeader = request.getHeader(AUTHORIZATION_HEADER);
    if (authorizationHeader == null || !authorizationHeader.startsWith(TOKEN_TYPE)) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      return false;
    }
    String token = authorizationHeader.substring(TOKEN_TYPE.length() + 1);
    try {
      Claims claims = DummySecurityJWT.decodeJWT(token);
      JSONObject jsonObject = new JSONObject(claims.getSubject());
      if (!jsonObject.get("CODE").equals(DummySecurityJWT.DUMMY_CODE)) {
        throw new SecurityException("Unauthorized Access");
      }

      /**
       * The {@link SecurityContextHolder} is where Spring Security stores the details of who is
       * authenticated. This is persisted in a thread local variable. Assumption is we use the same
       * thread to process authorization for a RPC request that we use for authentication.
       */
      SecurityContext context = SecurityContextHolder.createEmptyContext();
      context.setAuthentication(
          DummyAuthenticationContext.builder()
              .principal(new User(jsonObject.getString("USER-ID"), "", Collections.emptyList()))
              .isAuthenticated(true)
              .build());
      SecurityContextHolder.setContext(context);

      return true;
    } catch (MalformedJwtException
        | NoSuchAlgorithmException
        | InvalidKeySpecException
        | JSONException e) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      return false;
    }
  }

  /** Helper class to build a dummy JWT token from some fixed DUMMY parameters. */
  @AllArgsConstructor
  public static class DummySecurityJWT {
    public static final SignatureAlgorithm SIGNATURE_ALGORITHM = SignatureAlgorithm.HS256;
    public static final String DUMMY_SECURITY_PRIVATE_KEY = "DUMMY_SECURITY_PRIVATE_KEY";
    public static final String DUMMY_CODE = "DUMMY_CODE";
    public static final String DUMMY_ISSUE_DATE = "2022-08-05";

    public String userName;

    public String buildNoopJWT() throws ParseException, JSONException {
      return Jwts.builder()
          .setIssuedAt(new SimpleDateFormat("yyyy-MM-dd").parse(DUMMY_ISSUE_DATE))
          .setId(buildId())
          .setSubject(buildSubject())
          .signWith(SIGNATURE_ALGORITHM, buildSecretKey())
          .compact();
    }

    public static Claims decodeJWT(String noopJwt)
        throws NoSuchAlgorithmException, InvalidKeySpecException {
      return Jwts.parser().setSigningKey(buildSecretKey()).parseClaimsJws(noopJwt).getBody();
    }

    private static Key buildSecretKey() {
      return new SecretKeySpec(
          DatatypeConverter.parseBase64Binary(DUMMY_SECURITY_PRIVATE_KEY),
          SIGNATURE_ALGORITHM.getJcaName());
    }

    private String buildId() {
      return this.userName + UUID.randomUUID();
    }

    private String buildSubject() throws JSONException {
      JSONObject jsonObject = new JSONObject();
      jsonObject.put("CODE", DUMMY_CODE);
      jsonObject.put("USER-ID", this.userName);
      return jsonObject.toString();
    }
  }
}
