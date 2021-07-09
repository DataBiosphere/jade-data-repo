package bio.terra.service.resourcemanagement.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class GoogleResourceException extends InternalServerErrorException {
  public GoogleResourceException(String message) {
    super(message);
  }

  public GoogleResourceException(String message, Throwable cause) {
    super(message, cause);
  }

  public GoogleResourceException(Throwable cause) {
    super(cause);
  }
}
