package bio.terra.service.configuration.exception;

import bio.terra.common.exception.InternalServerErrorException;

public class DuplicateConfigNameException extends InternalServerErrorException {
  public DuplicateConfigNameException(String message, Throwable cause) {
    super(message, cause);
  }

  public DuplicateConfigNameException(String message) {
    super(message);
  }

  public DuplicateConfigNameException(Throwable cause) {
    super(cause);
  }
}
