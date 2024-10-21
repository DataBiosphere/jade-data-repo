package bio.terra.service.dataset.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidTableException extends BadRequestException {
  public InvalidTableException(String message) {
    super(message);
  }

  public InvalidTableException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidTableException(Throwable cause) {
    super(cause);
  }
}
