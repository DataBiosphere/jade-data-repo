package bio.terra.service.auth.ras.exception;

import bio.terra.common.exception.UnauthorizedException;

public class InvalidAuthorizationMethod extends UnauthorizedException {
  public InvalidAuthorizationMethod(String message) {
    super(message);
  }
}
