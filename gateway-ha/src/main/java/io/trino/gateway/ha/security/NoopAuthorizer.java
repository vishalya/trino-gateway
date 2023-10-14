package io.trino.gateway.ha.security;

import io.dropwizard.auth.Authorizer;
import jakarta.ws.rs.container.ContainerRequestContext;
import org.checkerframework.checker.nullness.qual.Nullable;


public class NoopAuthorizer implements Authorizer<LbPrincipal> {
  @Override
  public boolean authorize(LbPrincipal principal,
                           String role,
                           @Nullable ContainerRequestContext ctx) {
    return true;
  }


}
