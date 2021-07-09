package bio.terra.service.dataset.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidUriException extends BadRequestException {
  public InvalidUriException(String message) {
    super(message);
  }

  public InvalidUriException(String message, Throwable cause) {
    super(message, cause);
  }
}
