package bio.terra.datarepo.service.resourcemanagement.exception;

import bio.terra.datarepo.common.exception.UnauthorizedException;

public class BufferServiceAuthorizationException extends UnauthorizedException {

  public BufferServiceAuthorizationException(String message, Throwable cause) {
    super(message, cause);
  }
}
