package bio.terra.service.configuration.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class InvalidConfigTypeException extends InternalServerErrorException {
  public InvalidConfigTypeException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidConfigTypeException(String message) {
    super(message);
  }

  public InvalidConfigTypeException(Throwable cause) {
    super(cause);
  }
}
