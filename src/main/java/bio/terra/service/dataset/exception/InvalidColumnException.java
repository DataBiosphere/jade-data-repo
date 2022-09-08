package bio.terra.service.dataset.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidColumnException extends BadRequestException {
  public InvalidColumnException(String message) {
    super(message);
  }

  public InvalidColumnException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidColumnException(Throwable cause) {
    super(cause);
  }
}
