package bio.terra.datarepo.service.dataset.exception;

import bio.terra.datarepo.common.exception.BadRequestException;

public class InvalidUriException extends BadRequestException {
  public InvalidUriException(String message) {
    super(message);
  }

  public InvalidUriException(String message, Throwable cause) {
    super(message, cause);
  }
}
