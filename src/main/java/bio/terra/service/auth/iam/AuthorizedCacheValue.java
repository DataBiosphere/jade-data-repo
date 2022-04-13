package bio.terra.service.auth.iam;

import java.time.Instant;

public class AuthorizedCacheValue {
  // a timeout and a boolean
  private Instant timeout;
  private boolean authorized;

  public AuthorizedCacheValue(Instant timeout, boolean authorized) {
    this.timeout = timeout;
    this.authorized = authorized;
  }

  public Instant getTimeout() {
    return timeout;
  }

  public boolean isAuthorized() {
    return authorized;
  }
}
