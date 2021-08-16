package bio.terra.service.dataset.exception;

import bio.terra.common.exception.BadRequestException;

public class InvalidBlobURLException extends BadRequestException {
  public InvalidBlobURLException(String message) {
    super(message);
  }

  public InvalidBlobURLException(String message, Throwable cause) {
    super(message, cause);
  }
}
