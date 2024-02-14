package com.linkedin.openhouse.common.security;

import java.util.Collection;
import lombok.Builder;
import lombok.Getter;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

/**
 * Dummy authentication context for the authentication request that has been processed. Class holds
 * information about the principal authenticated.
 */
@Builder
@Getter
public class DummyAuthenticationContext implements Authentication {

  private Object principal;
  private boolean isAuthenticated;

  @Override
  public Collection<? extends GrantedAuthority> getAuthorities() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getCredentials() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getDetails() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getPrincipal() {
    return principal;
  }

  @Override
  public boolean isAuthenticated() {
    return isAuthenticated;
  }

  @Override
  public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException {
    this.isAuthenticated = isAuthenticated;
  }

  @Override
  public String getName() {
    return "dummy";
  }
}
