package bio.terra.service.policy.exception;

import bio.terra.common.exception.ForbiddenException;

public class PolicyServiceAuthorizationException extends ForbiddenException {

  public PolicyServiceAuthorizationException(String message, Throwable cause) {
    super(message, cause);
  }
}
