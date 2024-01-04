package bio.terra.service.filedata.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidDrsObjectException extends BadRequestException {
  public InvalidDrsObjectException(String message) {
    super(message);
  }

  public InvalidDrsObjectException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidDrsObjectException(Throwable cause) {
    super(cause);
  }
}
