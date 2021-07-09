package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.UnauthorizedException;

public class BufferServiceAuthorizationException extends UnauthorizedException {

  public BufferServiceAuthorizationException(String message, Throwable cause) {
    super(message, cause);
  }
}
