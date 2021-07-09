package bio.terra.datarepo.service.upgrade.exception;

import bio.terra.datarepo.common.exception.BadRequestException;

public class InvalidCustomNameException extends BadRequestException {
  public InvalidCustomNameException(String message) {
    super(message);
  }

  public InvalidCustomNameException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidCustomNameException(Throwable cause) {
    super(cause);
  }
}
